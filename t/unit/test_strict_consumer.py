"""Tests for the strict consumer admission mode."""

from __future__ import annotations

import pytest

from kombu import Connection, Consumer, Exchange, Queue
from kombu import compression as compression_module
from kombu import serialization as serialization_module
from kombu.exceptions import ContentDisallowed
from kombu.message import Message

EXCHANGE = Exchange('strict_ex', type='direct')
ROUTING_KEY = 'strict_rk'
FAKE_COMPRESSION = 'application/x-kombu-test-counting'
FAKE_CONTENT_TYPE = 'application/x-kombu-test-codec'


class CountingDecompressor:
    """Decompressor stand-in that records every invocation."""

    def __init__(self):
        self.calls = 0

    def __call__(self, body):
        self.calls += 1
        if body == b'z:__corrupt__':
            raise ValueError('corrupt compressed body')
        return body[2:] if body.startswith(b'z:') else body


class CountingDecoder:
    """Codec stand-in that records every invocation."""

    def __init__(self):
        self.calls = 0

    def __call__(self, body):
        self.calls += 1
        return {'decoded': body}


@pytest.fixture
def counting_pipeline():
    decompressor = CountingDecompressor()
    decoder = CountingDecoder()
    compression_module.register(
        encoder=lambda body: b'z:' + body,
        decoder=decompressor,
        content_type=FAKE_COMPRESSION,
        aliases=['counting'],
    )
    serialization_module.register(
        name='counting-codec',
        encoder=lambda obj: obj['encoded'],
        decoder=decoder,
        content_type=FAKE_CONTENT_TYPE,
        content_encoding='binary',
    )
    try:
        yield decompressor, decoder
    finally:
        compression_module._decoders.pop(FAKE_COMPRESSION, None)
        compression_module._encoders.pop(FAKE_COMPRESSION, None)
        compression_module._aliases.pop('counting', None)
        serialization_module.registry._decoders.pop(FAKE_CONTENT_TYPE, None)
        serialization_module.registry._encoders.pop('counting-codec', None)
        serialization_module.registry.type_to_name.pop(FAKE_CONTENT_TYPE, None)
        serialization_module.registry.name_to_type.pop('counting-codec', None)


@pytest.fixture
def connection_queue():
    connection = Connection('memory://')
    queue = Queue('strict_queue', EXCHANGE, routing_key=ROUTING_KEY)
    return connection, queue


def _publish(connection, body, *, content_type, content_encoding='binary',
             compression=None, serializer=None, headers=None):
    with connection.Producer() as producer:
        producer.publish(
            body,
            declare=[Queue('strict_queue', EXCHANGE,
                           routing_key=ROUTING_KEY)],
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            content_type=content_type,
            content_encoding=content_encoding,
            compression=compression,
            serializer=serializer,
            headers=headers,
        )


def _drain(connection, queue, **consumer_kwargs):
    errors = []
    consumer_kwargs.setdefault(
        'on_decode_error', lambda message, exc: errors.append((message, exc)))
    with connection.Consumer(queues=[queue], **consumer_kwargs) as consumer:
        connection.drain_events(timeout=1)
    return consumer, errors


class test_StrictConsumerRejection:

    def test_rejected_message_runs_neither_decompressor_nor_codec(
            self, counting_pipeline, connection_queue):
        decompressor, decoder = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'payload', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        _, errors = _drain(
            connection, queue, accept=['json'], strict=True,
            callbacks=[lambda body, message: None])

        message, exc = errors[0]
        assert isinstance(exc, ContentDisallowed)
        assert decompressor.calls == 0
        assert decoder.calls == 0

    def test_rejected_message_keeps_delivery_tag_for_reject(
            self, counting_pipeline, connection_queue):
        connection, queue = connection_queue
        _publish(connection, b'payload', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        _, errors = _drain(
            connection, queue, accept=['json'], strict=True,
            callbacks=[lambda body, message: None])
        message, exc = errors[0]
        assert isinstance(exc, ContentDisallowed)

        delivery_tag = message.delivery_tag
        assert delivery_tag is not None
        rejected = []
        original_reject = message.channel.basic_reject

        def spy_reject(tag, requeue=False):
            rejected.append((tag, requeue))
            return original_reject(tag, requeue=requeue)

        message.channel.basic_reject = spy_reject
        message.reject()

        assert message._state == 'REJECTED'
        assert rejected == [(delivery_tag, False)]

    def test_rejected_on_message_also_skips_all_conversion(
            self, counting_pipeline, connection_queue):
        decompressor, decoder = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'payload', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        delivered = []
        _, errors = _drain(
            connection, queue, accept=['json'], strict=True,
            on_message=lambda message: delivered.append(message))

        assert delivered == []
        assert isinstance(errors[0][1], ContentDisallowed)
        assert decompressor.calls == 0
        assert decoder.calls == 0

    def test_trusted_content_type_cannot_bypass_strict_set(
            self, counting_pipeline, connection_queue):
        connection, queue = connection_queue
        _publish(connection, b'plain', content_type='application/data')

        _, errors = _drain(
            connection, queue, accept=['json'], strict=True,
            callbacks=[lambda body, message: None])
        assert isinstance(errors[0][1], ContentDisallowed)

    def test_force_cannot_bypass_strict_admission(self):
        with pytest.raises(ContentDisallowed):
            serialization_module.loads(
                b'x', FAKE_CONTENT_TYPE, 'binary',
                accept={'application/json'}, force=True, strict=True)


class test_StrictConsumerAcceptance:

    def test_allowed_message_is_decompressed_and_decoded_once(
            self, counting_pipeline, connection_queue):
        decompressor, decoder = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'hello', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        seen = {}

        def callback(body, message):
            seen['body'] = body
            # Repeated access must not trigger repeated conversion.
            message.payload
            message.decode()
            message.payload

        _drain(connection, queue, accept=[FAKE_CONTENT_TYPE], strict=True,
               callbacks=[callback])

        assert seen['body'] == {'decoded': b'hello'}
        assert decompressor.calls == 1
        assert decoder.calls == 1

    def test_bad_compressed_body_never_reaches_codec(
            self, counting_pipeline, connection_queue):
        decompressor, decoder = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'__corrupt__', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        _, errors = _drain(
            connection, queue, accept=[FAKE_CONTENT_TYPE], strict=True,
            callbacks=[lambda body, message: None])

        assert isinstance(errors[0][1], ValueError)
        assert decompressor.calls == 1
        assert decoder.calls == 0

    def test_on_message_defers_conversion_until_payload_access(
            self, counting_pipeline, connection_queue):
        decompressor, decoder = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'hello', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        captured = []
        _drain(connection, queue, accept=[FAKE_CONTENT_TYPE], strict=True,
               on_message=lambda message: captured.append(message))

        message = captured[0]
        assert decompressor.calls == 0
        assert decoder.calls == 0
        assert message._decompressed is False
        assert message.payload == {'decoded': b'hello'}
        assert decompressor.calls == 1
        assert decoder.calls == 1

    def test_without_on_decode_error_exception_propagates(
            self, counting_pipeline, connection_queue):
        connection, queue = connection_queue
        _publish(connection, b'payload', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        with connection.Consumer(queues=[queue], accept=['json'],
                                 strict=True,
                                 callbacks=[lambda body, message: None]):
            with pytest.raises(ContentDisallowed):
                connection.drain_events(timeout=1)


class test_StrictModeDefaults:

    def test_default_consumer_is_not_strict(self, connection_queue):
        connection, queue = connection_queue
        consumer = Consumer(connection, queues=[queue], accept=['json'])
        assert consumer.strict is False

    def test_strict_can_be_enabled(self, connection_queue):
        connection, queue = connection_queue
        consumer = Consumer(connection, queues=[queue], accept=['json'],
                            strict=True)
        assert consumer.strict is True

    def test_non_strict_message_decompresses_eagerly(
            self, counting_pipeline, connection_queue):
        decompressor, _ = counting_pipeline
        connection, queue = connection_queue
        _publish(connection, b'hello', content_type=FAKE_CONTENT_TYPE,
                 compression='counting')

        _drain(connection, queue, accept=[FAKE_CONTENT_TYPE],
               callbacks=[lambda body, message: None])
        assert decompressor.calls == 1


class test_StrictMessageUnit:

    def test_construction_defers_decompression_in_strict_mode(
            self, counting_pipeline):
        decompressor, _ = counting_pipeline
        message = Message(
            b'z:hello',
            content_type=FAKE_CONTENT_TYPE,
            headers={'compression': 'counting'},
            accept={FAKE_CONTENT_TYPE},
            strict=True,
        )
        assert decompressor.calls == 0
        assert message.body == b'z:hello'
        assert message.payload == {'decoded': b'hello'}
        assert decompressor.calls == 1

    def test_gate_raises_before_any_conversion(self, counting_pipeline):
        decompressor, decoder = counting_pipeline
        message = Message(
            b'z:hello',
            content_type=FAKE_CONTENT_TYPE,
            headers={'compression': 'counting'},
            accept={'application/json'},
            strict=True,
        )
        with pytest.raises(ContentDisallowed):
            message._check_accept()
        assert decompressor.calls == 0
        assert decoder.calls == 0

    def test_repeated_payload_access_decodes_once(self, counting_pipeline):
        _, decoder = counting_pipeline
        message = Message(
            b'z:hello',
            content_type=FAKE_CONTENT_TYPE,
            headers={'compression': 'counting'},
            accept={FAKE_CONTENT_TYPE},
            strict=True,
        )
        for _ in range(3):
            message.payload
        assert decoder.calls == 1
