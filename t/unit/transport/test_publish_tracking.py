from __future__ import annotations

from unittest.mock import Mock

import pytest

from kombu.transport.publish_tracking import (
    PublishState,
    PublishTracker,
    PublishTrackingError,
)


class StubMessage:

    def __init__(self, body='', message_id=None):
        self.body = body
        self.properties = {}
        if message_id is not None:
            self.properties['message_id'] = message_id


class test_PublishTracker:

    def setup_method(self):
        self.results = []
        self.tracker = PublishTracker(on_result=self.results.append)

    def track(self, **kwargs):
        return self.tracker.track(**kwargs)

    def test_track__returns_stable_publish_id(self):
        publish_id = self.track(exchange='ex', routing_key='rk')
        assert publish_id
        assert self.tracker.pending == (publish_id,)
        assert self.tracker.result_for(publish_id) is None

    def test_track__custom_publish_id(self):
        assert self.track(publish_id='p1') == 'p1'

    def test_track__closed_tracker_raises(self):
        self.tracker.close()
        with pytest.raises(PublishTrackingError):
            self.track()

    def test_ack__single(self):
        publish_id = self.track(exchange='ex', routing_key='rk')
        self.tracker.handle_ack(1)
        result = self.tracker.result_for(publish_id)
        assert result.state is PublishState.DELIVERED
        assert result.delivery_tag == 1
        assert result.exchange == 'ex'
        assert result.routing_key == 'rk'
        assert self.results == [result]
        assert not self.tracker.pending

    def test_ack__multiple_covers_range(self):
        ids = [self.track() for _ in range(3)]
        self.tracker.handle_ack(3, multiple=True)
        assert [self.tracker.result_for(i).state for i in ids] == [
            PublishState.DELIVERED] * 3
        assert len(self.results) == 3

    def test_ack__multiple_ignores_higher_tags(self):
        ids = [self.track() for _ in range(3)]
        self.tracker.handle_ack(2, multiple=True)
        assert self.tracker.pending == (ids[2],)
        assert self.tracker.result_for(ids[0]).state is PublishState.DELIVERED

    def test_nack__single(self):
        publish_id = self.track()
        self.tracker.handle_nack(1)
        assert self.tracker.result_for(publish_id).state is PublishState.NACKED

    def test_nack__multiple(self):
        ids = [self.track() for _ in range(3)]
        self.tracker.handle_nack(2, multiple=True)
        assert self.tracker.result_for(ids[0]).state is PublishState.NACKED
        assert self.tracker.result_for(ids[1]).state is PublishState.NACKED
        assert self.tracker.pending == (ids[2],)
        assert len(self.results) == 2

    def test_return_then_ack__stays_returned(self):
        # mandatory unroutable publish: broker sends basic.return first,
        # then the confirm; the ack must not flip it to delivered.
        publish_id = self.track(exchange='ex', routing_key='nowhere')
        self.tracker.handle_return(
            reply_code=312, reply_text='NO_ROUTE',
            exchange='ex', routing_key='nowhere')
        self.tracker.handle_ack(1)
        result = self.tracker.result_for(publish_id)
        assert result.state is PublishState.RETURNED
        assert result.reply_code == 312
        assert result.reply_text == 'NO_ROUTE'
        assert self.results == [result]

    def test_return__correlates_by_message_id_not_body(self):
        first = self.track(exchange='ex', routing_key='rk',
                           message_id='m1')
        second = self.track(exchange='ex', routing_key='rk',
                            message_id='m2')
        # identical bodies, different message ids: the return for the
        # second message must not be attributed to the first.
        self.tracker.handle_return(
            312, 'NO_ROUTE', 'ex', 'rk',
            message=StubMessage(body='same-body', message_id='m2'))
        assert self.tracker.result_for(second).state is PublishState.RETURNED
        assert self.tracker.result_for(first) is None
        self.tracker.handle_ack(1)
        assert self.tracker.result_for(first).state is PublishState.DELIVERED

    def test_return__unknown_message_id_is_dropped(self):
        self.track(message_id='m1')
        self.tracker.handle_return(
            312, 'NO_ROUTE', 'ex', 'rk',
            message=StubMessage(message_id='someone-else'))
        assert len(self.tracker.pending) == 1
        assert not self.results

    def test_return__out_of_order_by_routing_key(self):
        first = self.track(exchange='ex', routing_key='a')
        second = self.track(exchange='ex', routing_key='b')
        # returns arrive out of publish order
        self.tracker.handle_return(312, 'NO_ROUTE', 'ex', 'b')
        self.tracker.handle_return(312, 'NO_ROUTE', 'ex', 'a')
        assert self.tracker.result_for(second).state is PublishState.RETURNED
        assert self.tracker.result_for(first).state is PublishState.RETURNED

    def test_return__unmatched_is_dropped(self):
        self.track(exchange='ex', routing_key='a')
        self.tracker.handle_return(312, 'NO_ROUTE', 'ex', 'other')
        assert len(self.tracker.pending) == 1
        assert not self.results

    def test_duplicate_ack__result_reported_once(self):
        publish_id = self.track()
        self.tracker.handle_ack(1)
        self.tracker.handle_ack(1)
        assert len(self.results) == 1
        assert self.tracker.result_for(publish_id).state is \
            PublishState.DELIVERED

    def test_duplicate_return__does_not_consume_next_pending(self):
        first = self.track(exchange='ex', routing_key='rk',
                           message_id='m1')
        second = self.track(exchange='ex', routing_key='rk',
                            message_id='m2')
        returned = StubMessage(message_id='m1')
        self.tracker.handle_return(312, 'NO_ROUTE', 'ex', 'rk',
                                   message=returned)
        self.tracker.handle_return(312, 'NO_ROUTE', 'ex', 'rk',
                                   message=returned)
        assert self.tracker.result_for(first).state is PublishState.RETURNED
        assert self.tracker.result_for(second) is None
        assert self.tracker.pending == (second,)
        assert len(self.results) == 1

    def test_ack__unknown_tag_is_ignored(self):
        self.track()
        self.tracker.handle_ack(99)
        assert len(self.tracker.pending) == 1
        assert not self.results

    def test_close__pending_become_unknown(self):
        ids = [self.track() for _ in range(2)]
        self.tracker.handle_ack(1)
        self.tracker.close()
        assert self.tracker.result_for(ids[0]).state is PublishState.DELIVERED
        assert self.tracker.result_for(ids[1]).state is PublishState.UNKNOWN
        assert len(self.results) == 2

    def test_close__late_events_are_ignored(self):
        # old channel generation must not confirm publishes after close
        publish_id = self.track()
        self.tracker.close()
        self.tracker.handle_ack(1)
        self.tracker.handle_nack(1)
        self.tracker.handle_return(312, 'NO_ROUTE', '', '')
        assert self.tracker.result_for(publish_id).state is \
            PublishState.UNKNOWN
        assert len(self.results) == 1

    def test_close__is_idempotent(self):
        self.track()
        self.tracker.close()
        self.tracker.close()
        assert len(self.results) == 1

    def test_unknown__single_publish(self):
        failed = self.track()
        other = self.track()
        self.tracker.unknown(failed)
        assert self.tracker.result_for(failed).state is PublishState.UNKNOWN
        assert self.tracker.pending == (other,)

    def test_per_publish_callback(self):
        on_result = Mock()
        publish_id = self.track(on_result=on_result)
        self.tracker.handle_ack(1)
        on_result.assert_called_once_with(self.tracker.result_for(publish_id))
        assert len(self.results) == 1

    def test_publish_state_terminal(self):
        assert not PublishState.PENDING.terminal
        for state in (PublishState.DELIVERED, PublishState.RETURNED,
                      PublishState.NACKED, PublishState.UNKNOWN):
            assert state.terminal
