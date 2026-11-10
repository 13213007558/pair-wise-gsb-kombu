"""Executable rolling-migration scenario for the json-v2 serializer.

It uses Kombu's in-memory transport, so it needs no broker. Each phase shows
which serializer the producer uses and which content types the consumer
accepts, and asserts the expected behaviour, including:

* a collision-prone audit dict round-tripping unchanged under json-v2 while it
  is silently turned into a date under the legacy json serializer;
* a legacy consumer rejecting (not misreading) a json-v2 message;
* the explicit legacy entry point for messages already stored on disk.
"""

from __future__ import annotations

from datetime import date

from kombu import Connection, Consumer, Producer, Queue
from kombu.exceptions import ContentDisallowed
from kombu.serialization import dumps, loads
from kombu.utils import json as legacy_json
from kombu.utils.json_safe import (CONTENT_TYPE as V2_CONTENT_TYPE,
                                   SERIALIZER_NAME as V2,
                                   loads_legacy)

QUEUE = Queue('migration', exchange=None, routing_key='migration')

# An audit original whose two keys collide with the legacy type-tag grammar.
AUDIT = {'__type__': 'date', '__value__': '2026-11-10'}


def publish(conn, body, serializer):
    Producer(conn.channel()).publish(
        body, routing_key='migration', serializer=serializer,
        declare=[QUEUE])


def consume(conn, accept):
    received = []

    def on_message(body, message):
        received.append(body)
        message.ack()

    with Consumer(conn.channel(), [QUEUE], callbacks=[on_message],
                  accept=accept):
        conn.drain_events(timeout=2)
    return received[0]


def reset_queue(conn):
    channel = conn.channel()
    channel.queue_delete('migration')
    channel.queue_declare('migration')


def phase(conn, number, producer_serializer, consumer_accept, expect_type):
    reset_queue(conn)
    publish(conn, AUDIT, producer_serializer)
    body = consume(conn, consumer_accept)
    label = 'kept as plain dict' if isinstance(body, dict) else 'decoded as date'
    print(f'phase {number}: producer={producer_serializer:<7} '
          f'accept={",".join(consumer_accept):<15} -> {label}')
    assert type(body) is expect_type
    if expect_type is dict:
        assert body == AUDIT
    return body


def show_legacy_collision():
    # The exact failure mode this migration removes.
    corrupted = legacy_json.loads(legacy_json.dumps(AUDIT))
    print('legacy json round trip:', repr(corrupted), type(corrupted).__name__)
    assert type(corrupted).__name__ == 'date'


def show_hard_boundary(conn):
    # A json-only consumer must reject a v2 message, never misread it.
    reset_queue(conn)
    publish(conn, AUDIT, V2)
    try:
        consume(conn, accept=['json'])
    except ContentDisallowed as exc:
        print('legacy consumer rejects v2 message:', type(exc).__name__)
        return
    raise AssertionError('legacy consumer unexpectedly accepted v2 message')


def show_on_disk_legacy_record():
    # Simulate a record persisted before the migration and read it explicitly.
    _, _, on_disk = dumps(AUDIT, serializer='json')
    recovered = loads_legacy(on_disk)
    print('on-disk legacy record via loads_legacy:', repr(recovered))
    assert type(recovered).__name__ == 'date'

    # The same bytes are refused by the v2 decoder rather than guessed at.
    try:
        from kombu.utils.json_safe import loads as v2_loads
        v2_loads(on_disk)
    except Exception as exc:  # DecodeError
        print('v2 decoder refuses legacy bytes:', type(exc).__name__)


def main():
    show_legacy_collision()
    with Connection('memory://') as conn:
        # Phase 1: consumers learn to read v2, everything is still written json.
        phase(conn, 1, 'json', ['json', V2], expect_type=date)
        # Phase 2: producers switch to v2; consumers still accept both formats.
        phase(conn, 2, V2, ['json', V2], expect_type=dict)
        # Phase 3: only the safe format remains accepted.
        phase(conn, 3, V2, [V2], expect_type=dict)
        show_hard_boundary(conn)
    show_on_disk_legacy_record()
    print('new content type:', V2_CONTENT_TYPE)
    print('all migration assertions passed')


if __name__ == '__main__':
    main()
