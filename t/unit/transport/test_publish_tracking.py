from __future__ import annotations

import threading
from itertools import count
from unittest.mock import MagicMock, Mock

import pytest

from kombu import Connection
from kombu.transport import pyamqp
from kombu.transport.publish_tracking import PublishState, PublishTracker


class test_PublishTracker:

    def setup_method(self):
        self.tracker = PublishTracker()

    def states(self, *publish_ids):
        return [self.tracker.get_result(pid).state for pid in publish_ids]

    def test_publish_ids_are_stable_and_unique(self):
        pid1 = self.tracker.track()
        pid2 = self.tracker.track()
        assert pid1 != pid2
        assert self.tracker.get_result(pid1).publish_id == pid1
        assert self.tracker.get_result(pid1).state is PublishState.PENDING
        assert not self.tracker.get_result(pid1).terminal

    def test_unknown_publish_id(self):
        assert self.tracker.get_result("9:9") is None
        with pytest.raises(KeyError):
            self.tracker.add_callback("9:9", lambda result: None)
        with pytest.raises(KeyError):
            self.tracker.wait_result("9:9")

    def test_single_ack(self):
        pid = self.tracker.track()
        self.tracker.on_ack(1)
        result = self.tracker.get_result(pid)
        assert result.state is PublishState.ACKED
        assert result.terminal

    def test_single_nack(self):
        pid = self.tracker.track()
        self.tracker.on_nack(1)
        assert self.tracker.get_result(pid).state is PublishState.NACKED

    def test_ack_multiple(self):
        pids = [self.tracker.track() for _ in range(3)]
        self.tracker.on_ack(2, multiple=True)
        assert self.states(*pids) == [
            PublishState.ACKED, PublishState.ACKED, PublishState.PENDING]

    def test_nack_multiple(self):
        pids = [self.tracker.track() for _ in range(3)]
        self.tracker.on_nack(3, multiple=True)
        assert self.states(*pids) == [PublishState.NACKED] * 3

    def test_return_then_ack_stays_returned(self):
        # mandatory unroutable message: broker sends basic.return first,
        # then confirms with ack.  The ack must not flip the result.
        pid = self.tracker.track(mandatory=True)
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk")
        self.tracker.on_ack(1)
        result = self.tracker.get_result(pid)
        assert result.state is PublishState.RETURNED
        assert result.reply_code == 312
        assert result.reply_text == "NO_ROUTE"
        assert result.exchange == "ex"
        assert result.routing_key == "rk"

    def test_return_matches_oldest_pending_mandatory_publish(self):
        non_mandatory = self.tracker.track()
        first = self.tracker.track(mandatory=True)
        second = self.tracker.track(mandatory=True)
        self.tracker.on_ack(1)  # non-mandatory publish confirmed
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk")
        assert self.states(non_mandatory, first, second) == [
            PublishState.ACKED, PublishState.RETURNED, PublishState.PENDING]
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk")
        assert self.tracker.get_result(second).state is PublishState.RETURNED

    def test_return_does_not_match_by_body(self):
        # two publishes with identical bodies must still be told apart;
        # the tracker correlates returns by publish order only.
        first = self.tracker.track(mandatory=True)
        second = self.tracker.track(mandatory=True)
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk",
                               message=Mock(body="same-body"))
        assert self.states(first, second) == [
            PublishState.RETURNED, PublishState.PENDING]

    def test_return_for_unknown_publish_is_ignored(self):
        pid = self.tracker.track()  # not mandatory: never returnable
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk")
        assert self.tracker.get_result(pid).state is PublishState.PENDING

    def test_duplicate_events_yield_single_terminal_state(self):
        pid = self.tracker.track(mandatory=True)
        seen = []
        self.tracker.add_callback(pid, seen.append)
        self.tracker.on_ack(1)
        self.tracker.on_ack(1)
        self.tracker.on_nack(1)
        self.tracker.on_return(312, "NO_ROUTE", "ex", "rk")
        assert self.tracker.get_result(pid).state is PublishState.ACKED
        assert len(seen) == 1
        assert seen[0].state is PublishState.ACKED

    def test_callback_registered_after_resolution_fires_immediately(self):
        pid = self.tracker.track()
        self.tracker.on_ack(1)
        seen = []
        self.tracker.add_callback(pid, seen.append)
        assert len(seen) == 1
        assert seen[0].state is PublishState.ACKED

    def test_disconnect_marks_pending_unknown(self):
        done = self.tracker.track()
        pending = self.tracker.track(mandatory=True)
        self.tracker.on_ack(1)
        self.tracker.on_disconnect()
        assert self.tracker.get_result(done).state is PublishState.ACKED
        result = self.tracker.get_result(pending)
        assert result.state is PublishState.UNKNOWN
        assert result.terminal

    def test_stale_delivery_tag_cannot_confirm_new_generation(self):
        old = self.tracker.track()  # delivery tag 1, generation 0
        self.tracker.on_disconnect()
        new = self.tracker.track()  # delivery tag 1 again, generation 1
        assert old != new
        # a late ack carrying the old delivery tag must not confirm the
        # new publish that reused the tag:
        self.tracker.on_ack(1)
        assert self.tracker.get_result(old).state is PublishState.UNKNOWN
        assert self.tracker.get_result(new).state is PublishState.ACKED

    def test_discard_rolls_back_delivery_tag(self):
        failed = self.tracker.track()
        self.tracker.discard(failed)
        assert self.tracker.get_result(failed) is None
        nxt = self.tracker.track()
        self.tracker.on_ack(1)  # broker assigns tag 1 to second publish
        assert self.tracker.get_result(nxt).state is PublishState.ACKED

    def test_wait_result(self):
        pid = self.tracker.track()
        timer = threading.Timer(0.01, self.tracker.on_ack, args=(1,))
        timer.start()
        try:
            result = self.tracker.wait_result(pid, timeout=5)
            assert result.state is PublishState.ACKED
        finally:
            timer.cancel()

    def test_wait_result_timeout(self):
        pid = self.tracker.track()
        with pytest.raises(TimeoutError):
            self.tracker.wait_result(pid, timeout=0.01)


class ScriptedChannel(pyamqp.Channel):
    """pyamqp channel whose broker side is scripted by the test."""

    def wait(self, *args, **kwargs):
        return None

    def broker_ack(self, delivery_tag, multiple=False):
        self._on_basic_ack(delivery_tag, multiple)

    def broker_nack(self, delivery_tag, multiple=False):
        self._on_basic_nack(delivery_tag, multiple)

    def broker_return(self, exchange="ex", routing_key="rk", message=None):
        self._on_basic_return(312, "NO_ROUTE", exchange, routing_key,
                              message if message is not None else Mock())


class test_pyamqp_publish_tracking:

    def setup_method(self):
        conn = MagicMock()
        conn.channels = {}
        conn._get_free_channel_id.side_effect = count(1).__next__
        conn.confirm_publish = False
        self.connection = conn
        self.channel = ScriptedChannel(conn, 1)
        self.channel.is_open = True

    def publish(self, mandatory=False, body="body"):
        return self.channel.basic_publish_tracked(
            self.channel.prepare_message(body), "ex", "rk",
            mandatory=mandatory)

    def test_enables_confirm_select_and_returns_same_tracker(self):
        tracker = self.channel.enable_publish_tracking()
        assert tracker is self.channel.enable_publish_tracking()
        assert tracker is self.channel.publish_tracker

    def test_tracker_is_none_until_enabled(self):
        assert self.channel.publish_tracker is None

    def test_return_then_ack_reports_returned(self):
        pid = self.publish(mandatory=True)
        self.channel.broker_return()
        self.channel.broker_ack(1)
        result = self.channel.publish_tracker.get_result(pid)
        assert result.state is PublishState.RETURNED
        assert result.reply_code == 312

    def test_multiple_in_flight_and_nack_multiple(self):
        pids = [self.publish() for _ in range(3)]
        self.channel.broker_nack(3, multiple=True)
        tracker = self.channel.publish_tracker
        assert [tracker.get_result(p).state for p in pids] == \
            [PublishState.NACKED] * 3

    def test_out_of_order_confirms(self):
        pids = [self.publish() for _ in range(3)]
        self.channel.broker_ack(3)
        self.channel.broker_ack(1)
        tracker = self.channel.publish_tracker
        assert [tracker.get_result(p).state for p in pids] == \
            [PublishState.ACKED, PublishState.PENDING, PublishState.ACKED]

    def test_duplicate_events_single_terminal_state(self):
        pid = self.publish(mandatory=True)
        seen = []
        self.channel.publish_tracker.add_callback(pid, seen.append)
        self.channel.broker_return()
        self.channel.broker_ack(1)
        self.channel.broker_nack(1)
        assert [r.state for r in seen] == [PublishState.RETURNED]

    def test_close_marks_pending_unknown(self):
        pid = self.publish()
        self.channel.close()
        assert self.channel.publish_tracker.get_result(pid).state is \
            PublishState.UNKNOWN

    def test_broker_initiated_close_marks_pending_unknown(self):
        pid = self.publish()
        self.channel._on_close(0, "", 0, 0)
        assert self.channel.publish_tracker.get_result(pid).state is \
            PublishState.UNKNOWN

    def test_publish_failure_discards_tracking(self):
        self.connection.frame_writer.side_effect = OSError("conn dead")
        with pytest.raises(OSError):
            self.publish()
        # the failed publish did not consume a delivery tag:
        self.connection.frame_writer.side_effect = None
        pid = self.publish()
        self.channel.broker_ack(1)
        assert self.channel.publish_tracker.get_result(pid).state is \
            PublishState.ACKED


class test_transport_capability:

    def test_pyamqp_supports_publish_tracking(self):
        assert Connection("amqp://").transport.supports_publish_tracking

    def test_other_transports_declare_no_support(self):
        assert not \
            Connection("memory://").transport.supports_publish_tracking
        assert not \
            Connection("filesystem://").transport.supports_publish_tracking

    def test_base_transport_does_not_support_publish_tracking(self):
        from kombu.transport.base import Transport
        assert not Transport.implements.publish_tracking
