"""Publisher confirm result tracking.

This module implements a per-channel tracker that merges the two
broker-side publish outcomes -- publisher confirms (basic.ack /
basic.nack) and returned messages (basic.return) -- into a single
terminal result for every published message.

The tracker never guesses: a publish is only reported as delivered when
the broker actually confirmed it, and a mandatory publish that comes back
through basic.return stays RETURNED even when the broker sends the
confirm for it afterwards.
"""

from __future__ import annotations

import enum
from itertools import count
from typing import Any, Callable, Dict, NamedTuple, Optional
from uuid import uuid4

__all__ = (
    'PublishState', 'PublishResult', 'PublishTracker',
    'PublishTrackingError',
)


class PublishTrackingError(Exception):
    """Raised when a publish cannot be registered with a tracker."""


class PublishState(enum.Enum):
    """State of a tracked publish."""

    #: Waiting for a broker conclusion.
    PENDING = 'pending'
    #: Confirmed by the broker (basic.ack) and never returned.
    DELIVERED = 'delivered'
    #: Came back through basic.return (unroutable mandatory publish).
    RETURNED = 'returned'
    #: Negatively confirmed by the broker (basic.nack).
    NACKED = 'nacked'
    #: No conclusion was received before the channel/connection was lost.
    UNKNOWN = 'unknown'

    @property
    def terminal(self):
        """Return True if this state concludes the publish."""
        return self is not PublishState.PENDING


class PublishResult(NamedTuple):
    """Outcome of a single tracked publish."""

    #: Stable identifier assigned when the publish was tracked.
    publish_id: str
    #: Terminal state of the publish.
    state: PublishState
    #: Delivery tag used by the broker confirm for this publish.
    delivery_tag: Optional[int] = None
    #: Exchange the message was published to.
    exchange: str = ''
    #: Routing key the message was published with.
    routing_key: str = ''
    #: AMQP reply code (set for returned publishes).
    reply_code: Optional[int] = None
    #: AMQP reply text (set for returned publishes).
    reply_text: Optional[str] = None


class _PendingPublish(NamedTuple):
    publish_id: str
    delivery_tag: int
    exchange: str
    routing_key: str
    message_id: Optional[str]
    on_result: Optional[Callable[[PublishResult], Any]]


def _returned_message_id(message):
    if message is None:
        return None
    properties = getattr(message, 'properties', None) or {}
    return properties.get('message_id')


class PublishTracker:
    """Track broker confirms and returns for one channel generation.

    A tracker instance is bound to a single channel generation: once
    :meth:`close` is called (the channel or connection was closed or
    lost) every pending publish is resolved as
    :attr:`~PublishState.UNKNOWN` and any late event belonging to the
    old generation is ignored, so stale delivery tags can never confirm
    messages published on a new channel.

    Every tracked publish reaches exactly one terminal state, reported
    exactly once through the ``on_result`` callbacks.
    """

    def __init__(self, on_result: Callable[[PublishResult], Any] = None):
        self.on_result = on_result
        self.active = True
        self._pending = {}  # delivery_tag -> _PendingPublish (ordered)
        self._results = {}  # publish_id -> PublishResult
        self._tags = count(1)

    def __repr__(self):
        return f'<PublishTracker: {len(self._pending)} pending>'

    @property
    def pending(self):
        """Publish ids still waiting for a broker conclusion."""
        return tuple(entry.publish_id for entry in self._pending.values())

    @property
    def results(self) -> Dict[str, PublishResult]:
        """Terminal results received so far, keyed by publish id."""
        return dict(self._results)

    def result_for(self, publish_id) -> Optional[PublishResult]:
        """Return the terminal result for ``publish_id``, if any."""
        return self._results.get(publish_id)

    def track(self, exchange='', routing_key='', publish_id=None,
              message_id=None, on_result=None):
        """Register a new in-flight publish.

        Returns the stable publish id identifying this publish; the id is
        generated when not provided by the caller.
        """
        if not self.active:
            raise PublishTrackingError(
                'cannot track publish: tracker belongs to a closed '
                'channel generation')
        delivery_tag = next(self._tags)
        publish_id = publish_id or uuid4().hex
        self._pending[delivery_tag] = _PendingPublish(
            publish_id=publish_id,
            delivery_tag=delivery_tag,
            exchange=exchange,
            routing_key=routing_key,
            message_id=message_id,
            on_result=on_result,
        )
        return publish_id

    def handle_ack(self, delivery_tag, multiple=False):
        """Handle broker basic.ack confirm."""
        self._handle_confirm(delivery_tag, multiple, PublishState.DELIVERED)

    def handle_nack(self, delivery_tag, multiple=False):
        """Handle broker basic.nack confirm."""
        self._handle_confirm(delivery_tag, multiple, PublishState.NACKED)

    def handle_return(self, reply_code=None, reply_text=None,
                      exchange='', routing_key='', message=None):
        """Handle basic.return of an unroutable mandatory message.

        The returned message is correlated to a pending publish by its
        ``message_id`` property when available, otherwise to the
        oldest pending publish with the same exchange and routing key.
        Message bodies are never used for correlation.
        """
        if not self.active:
            return
        tag = self._match_return(exchange, routing_key, message)
        if tag is None:
            return
        self._resolve(tag, PublishState.RETURNED,
                      reply_code=reply_code, reply_text=reply_text)

    def unknown(self, publish_id):
        """Resolve a single pending publish as UNKNOWN.

        Used when the publish could not be written to the broker, so no
        confirm will ever arrive for it.
        """
        if not self.active:
            return
        for tag, entry in self._pending.items():
            if entry.publish_id == publish_id:
                self._resolve(tag, PublishState.UNKNOWN)
                return

    def expire_all(self, state=PublishState.UNKNOWN):
        """Resolve every pending publish with ``state``."""
        for tag in list(self._pending):
            self._resolve(tag, state)

    def close(self):
        """Detach the tracker from its channel generation.

        All pending publishes become UNKNOWN and subsequent events are
        dropped.
        """
        if self.active:
            self.expire_all(PublishState.UNKNOWN)
            self.active = False

    def _handle_confirm(self, delivery_tag, multiple, state):
        if not self.active:
            return
        if multiple:
            tags = sorted(tag for tag in self._pending
                          if tag <= delivery_tag)
        else:
            tags = [delivery_tag]
        for tag in tags:
            self._resolve(tag, state)

    def _match_return(self, exchange, routing_key, message):
        message_id = _returned_message_id(message)
        if message_id is not None:
            for tag, entry in self._pending.items():
                if entry.message_id is not None and \
                        entry.message_id == message_id:
                    return tag
            return None
        for tag, entry in self._pending.items():
            if (entry.exchange, entry.routing_key) == \
                    (exchange, routing_key):
                return tag
        return None

    def _resolve(self, delivery_tag, state, **extra):
        entry = self._pending.pop(delivery_tag, None)
        if entry is None:
            # Duplicate or late event for an already concluded publish:
            # the first terminal state always wins.
            return None
        result = PublishResult(
            publish_id=entry.publish_id,
            state=state,
            delivery_tag=delivery_tag,
            exchange=entry.exchange,
            routing_key=entry.routing_key,
            **extra)
        self._results[entry.publish_id] = result
        for callback in (self.on_result, entry.on_result):
            if callback is not None:
                callback(result)
        return result
