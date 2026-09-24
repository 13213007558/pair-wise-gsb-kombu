"""Publisher confirm / return result tracking.

This module implements a small state machine that merges the two
broker-side outcomes of publishing a message with confirms enabled:

* basic.ack / basic.nack (publisher confirms), and
* basic.return (unroutable mandatory message),

into exactly one terminal PublishResult per published message.

Key properties:

* Every publish gets a *stable publish id* handed to the caller up
  front; broker delivery tags are an internal detail.
* A basic.return followed by a confirm ack for the same message
  (the normal RabbitMQ ordering for unroutable mandatory messages)
  yields RETURNED -- the later ack must not flip the result to
  "delivered".
* Batch (multiple=True) and out-of-order confirms are supported.
* Returns are correlated to publishes by *publish order* on the
  channel, never by comparing message bodies.
* When the connection/channel is replaced (disconnect), every
  unresolved publish is concluded as UNKNOWN and the tracker
  generation is bumped, so stale delivery tags from a previous
  channel incarnation can never confirm new messages.
* No automatic republish: UNKNOWN is surfaced to the caller, who
  decides what to do.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

__all__ = (
    'PublishState', 'PublishResult', 'PublishTracker',
)


class PublishState(Enum):
    """State of a tracked publish."""

    #: No broker conclusion received yet.
    PENDING = 'pending'
    #: Broker confirmed the message (and it was not returned).
    ACKED = 'acked'
    #: Broker negatively confirmed the message.
    NACKED = 'nacked'
    #: Message was unroutable and returned (mandatory publish).
    RETURNED = 'returned'
    #: Connection/channel was lost before any conclusion arrived.
    UNKNOWN = 'unknown'


@dataclass(frozen=True)
class PublishResult:
    """Final (or current) outcome of one tracked publish."""

    publish_id: str
    state: PublishState
    #: AMQP reply code/text from basic.return, when applicable.
    reply_code: int | None = None
    reply_text: str | None = None
    #: Exchange/routing key echoed by basic.return, when applicable.
    exchange: str | None = None
    routing_key: str | None = None

    @property
    def terminal(self):
        """True if this result is final and will never change."""
        return self.state is not PublishState.PENDING


@dataclass
class _Record:
    publish_id: str
    generation: int
    delivery_tag: int
    mandatory: bool
    state: PublishState = PublishState.PENDING
    reply_code: int | None = None
    reply_text: str | None = None
    exchange: str | None = None
    routing_key: str | None = None
    callbacks: list = field(default_factory=list)

    def to_result(self):
        return PublishResult(
            publish_id=self.publish_id,
            state=self.state,
            reply_code=self.reply_code,
            reply_text=self.reply_text,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )


class PublishTracker:
    """Merge publisher confirms and returns into per-message results.

    One tracker instance tracks one logical stream of publishes.  Call
    :meth:`on_disconnect` whenever the underlying channel/connection is
    lost or replaced; this concludes everything still pending as
    UNKNOWN and starts a new generation so that late events carrying
    old delivery tags are ignored.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._generation = 0
        self._seq = 0
        self._next_delivery_tag = 1
        # publish_id -> _Record (kept for the lifetime of the tracker
        # so callers can always query a result).
        self._records = {}
        # delivery_tag -> publish_id for the current generation.
        self._by_tag = {}
        # publish_ids of still-pending mandatory publishes, in publish
        # order; used to correlate basic.return without ever looking at
        # message bodies.
        self._returnable = deque()

    @property
    def generation(self):
        """Current channel/connection generation (0-based)."""
        return self._generation

    def track(self, mandatory=False):
        """Register one outgoing publish and return its stable id.

        Must be called in the exact order the messages are published on
        the channel, since broker delivery tags are assigned
        sequentially.
        """
        with self._condition:
            self._seq += 1
            publish_id = '%d:%d' % (self._generation, self._seq)
            record = _Record(
                publish_id=publish_id,
                generation=self._generation,
                delivery_tag=self._next_delivery_tag,
                mandatory=mandatory,
            )
            self._next_delivery_tag += 1
            self._records[publish_id] = record
            self._by_tag[record.delivery_tag] = publish_id
            if mandatory:
                self._returnable.append(publish_id)
            return publish_id

    def discard(self, publish_id):
        """Forget a publish whose basic_publish call itself failed.

        Rolls the delivery-tag counter back when possible so subsequent
        publishes stay aligned with the broker.
        """
        with self._condition:
            record = self._records.pop(publish_id, None)
            if record is None:
                return
            self._by_tag.pop(record.delivery_tag, None)
            if record.delivery_tag == self._next_delivery_tag - 1:
                self._next_delivery_tag -= 1

    def get_result(self, publish_id):
        """Return the current result, or None for an unknown id."""
        with self._condition:
            record = self._records.get(publish_id)
            return record.to_result() if record is not None else None

    def add_callback(self, publish_id, callback):
        """Call callback(result) exactly once, at the terminal state.

        If the publish already concluded, the callback fires
        immediately (synchronously).
        """
        with self._condition:
            record = self._records.get(publish_id)
            if record is None:
                raise KeyError('unknown publish id: %r' % (publish_id,))
            if record.state is PublishState.PENDING:
                record.callbacks.append(callback)
                return
            result = record.to_result()
        callback(result)

    def wait_result(self, publish_id, timeout=None):
        """Block until the publish concludes and return its result.

        Raises TimeoutError if no terminal state is reached within
        *timeout* seconds.
        """
        with self._condition:
            if publish_id not in self._records:
                raise KeyError('unknown publish id: %r' % (publish_id,))

            def concluded():
                return (self._records[publish_id].state
                        is not PublishState.PENDING)

            if not self._condition.wait_for(concluded, timeout=timeout):
                raise TimeoutError(
                    'no publish result for %r within timeout' % (publish_id,))
            return self._records[publish_id].to_result()

    # -- broker events -------------------------------------------------

    def on_ack(self, delivery_tag, multiple=False):
        """Handle basic.ack from the broker."""
        self._on_confirm(delivery_tag, multiple, PublishState.ACKED)

    def on_nack(self, delivery_tag, multiple=False):
        """Handle basic.nack from the broker."""
        self._on_confirm(delivery_tag, multiple, PublishState.NACKED)

    def on_return(self, reply_code, reply_text, exchange, routing_key,
                  message=None):
        """Handle basic.return from the broker.

        The return is correlated to the oldest still-pending mandatory
        publish (RabbitMQ emits returns in publish order on a channel).
        Message bodies are never inspected.
        """
        with self._condition:
            record = None
            while self._returnable:
                candidate = self._records.get(self._returnable[0])
                if (candidate is None or
                        candidate.state is not PublishState.PENDING):
                    self._returnable.popleft()
                    continue
                record = candidate
                self._returnable.popleft()
                break
            if record is None:
                # Return for a publish we no longer track (e.g. already
                # concluded by a stale event); nothing sensible to do.
                return
            self._resolve(
                record, PublishState.RETURNED,
                reply_code=reply_code, reply_text=reply_text,
                exchange=exchange, routing_key=routing_key)

    def on_disconnect(self):
        """Conclude every pending publish as UNKNOWN, bump generation."""
        with self._condition:
            self._generation += 1
            self._next_delivery_tag = 1
            self._by_tag.clear()
            self._returnable.clear()
            for record in self._records.values():
                if record.state is PublishState.PENDING:
                    self._resolve(record, PublishState.UNKNOWN)

    # -- internals -----------------------------------------------------

    def _on_confirm(self, delivery_tag, multiple, state):
        with self._condition:
            if multiple:
                tags = [tag for tag in self._by_tag if tag <= delivery_tag]
            else:
                tags = [delivery_tag] if delivery_tag in self._by_tag else []
            for tag in sorted(tags):
                record = self._records.get(self._by_tag[tag])
                if record is not None:
                    self._resolve(record, state)

    def _resolve(self, record, state, **info):
        # Must be called with the condition held.  A record leaves
        # PENDING at most once: duplicate or late events are ignored,
        # so a caller never observes more than one terminal state.
        if record.state is not PublishState.PENDING:
            return
        record.state = state
        for key, value in info.items():
            setattr(record, key, value)
        self._by_tag.pop(record.delivery_tag, None)
        result = record.to_result()
        callbacks, record.callbacks = record.callbacks, []
        self._condition.notify_all()
        for callback in callbacks:
            callback(result)
