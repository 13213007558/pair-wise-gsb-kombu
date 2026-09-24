from __future__ import annotations

import socket
import warnings

import pytest

from kombu import Connection, Consumer, Exchange, Producer, Queue


class test_MemoryTransport:

    def setup_method(self):
        self.c = Connection(transport='memory')
        self.e = Exchange('test_transport_memory')
        self.q = Queue('test_transport_memory',
                       exchange=self.e,
                       routing_key='test_transport_memory')
        self.q2 = Queue('test_transport_memory2',
                        exchange=self.e,
                        routing_key='test_transport_memory2')
        self.fanout = Exchange('test_transport_memory_fanout', type='fanout')
        self.q3 = Queue('test_transport_memory_fanout1',
                        exchange=self.fanout)
        self.q4 = Queue('test_transport_memory_fanout2',
                        exchange=self.fanout)

    def test_driver_version(self):
        assert self.c.transport.driver_version()

    def test_produce_consume_noack(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer = Consumer(channel, self.q, no_ack=True)

        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')

        _received = []

        def callback(message_data, message):
            _received.append(message)

        consumer.register_callback(callback)
        consumer.consume()

        while 1:
            if len(_received) == 10:
                break
            self.c.drain_events()

        assert len(_received) == 10

    def test_produce_consume_fanout(self):
        producer = self.c.Producer()
        consumer = self.c.Consumer([self.q3, self.q4])

        producer.publish(
            {'hello': 'world'},
            declare=consumer.queues,
            exchange=self.fanout,
        )

        assert self.q3(self.c).get().payload == {'hello': 'world'}
        assert self.q4(self.c).get().payload == {'hello': 'world'}
        assert self.q3(self.c).get() is None
        assert self.q4(self.c).get() is None

    def test_produce_consume(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer1 = Consumer(channel, self.q)
        consumer2 = Consumer(channel, self.q2)
        self.q2(channel).declare()

        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory2')

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        consumer1.consume()
        consumer2.consume()

        while 1:
            if len(_received1) + len(_received2) == 20:
                break
            self.c.drain_events()

        assert len(_received1) + len(_received2) == 20

        # compression
        producer.publish({'compressed': True},
                         routing_key='test_transport_memory',
                         compression='zlib')
        m = self.q(channel).get()
        assert m.payload == {'compressed': True}

        # queue.delete
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')
        assert self.q(channel).get()
        self.q(channel).delete()
        self.q(channel).declare()
        assert self.q(channel).get() is None

        # queue.purge
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory2')
        assert self.q2(channel).get()
        self.q2(channel).purge()
        assert self.q2(channel).get() is None

    def test_drain_events(self):
        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        c1 = self.c.channel()
        c2 = self.c.channel()

        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        del c1  # so pyflakes doesn't complain.
        del c2

    def test_drain_events_unregistered_queue(self):
        c1 = self.c.channel()
        producer = self.c.Producer()
        consumer = self.c.Consumer([self.q2])

        producer.publish(
            {'hello': 'world'},
            declare=consumer.queues,
            routing_key=self.q2.routing_key,
            exchange=self.q2.exchange,
        )
        message = consumer.queues[0].get()._raw

        class Cycle:

            def get(self, callback, timeout=None):
                return (message, 'foo'), c1

        self.c.transport.cycle = Cycle()
        self.c.drain_events()

    def test_queue_for(self):
        chan = self.c.channel()
        chan.queues.clear()

        x = chan._queue_for('foo')
        assert x
        assert chan._queue_for('foo') is x

    # see the issue
    # https://github.com/celery/kombu/issues/1050
    def test_producer_on_return(self):
        def on_return(_exception, _exchange, _routing_key, _message):
            pass
        channel = self.c.channel()
        producer = Producer(channel, on_return=on_return)
        consumer = self.c.Consumer([self.q3])

        producer.publish(
            {'hello': 'on return'},
            declare=consumer.queues,
            exchange=self.fanout,
        )

        assert self.q3(self.c).get().payload == {'hello': 'on return'}
        assert self.q3(self.c).get() is None


class test_MemoryHeadersExchange:

    exchange = 'test_memory_headers'

    #: binding matrix: (queue, binding arguments)
    bindings = [
        ('q_h_all', {'x-match': 'all', 'format': 'pdf', 'pages': 10}),
        ('q_h_any', {'x-match': 'any', 'format': 'pdf', 'urgent': True}),
        ('q_h_allx', {'x-match': 'all-with-x',
                      'x-tenant': 'eu', 'format': 'pdf'}),
        ('q_h_anyx', {'x-match': 'any-with-x', 'x-tenant': 'us'}),
        ('q_h_empty_all', {'x-match': 'all'}),
        ('q_h_empty_any', {'x-match': 'any'}),
    ]

    #: message/binding matrix: message headers -> expected queues
    matrix = [
        ({'format': 'pdf', 'pages': 10},
         {'q_h_all', 'q_h_any', 'q_h_empty_all'}),
        ({'format': 'pdf', 'pages': 10, 'x-tenant': 'eu'},
         {'q_h_all', 'q_h_any', 'q_h_allx', 'q_h_empty_all'}),
        ({'format': 'pdf', 'pages': 5},
         {'q_h_any', 'q_h_empty_all'}),
        ({'urgent': True},
         {'q_h_any', 'q_h_empty_all'}),
        ({'x-tenant': 'us'},
         {'q_h_anyx', 'q_h_empty_all'}),
        ({'format': 'zip'},
         {'q_h_empty_all'}),
        ({},
         {'q_h_empty_all'}),
    ]

    def setup_method(self):
        self.c = Connection(transport='memory')
        self.channel = self.c.channel()
        self.channel.exchange_declare(self.exchange, type='headers')
        for queue, arguments in self.bindings:
            self.channel.queue_declare(queue)
            self.channel.queue_bind(
                queue, self.exchange, arguments=arguments)
        # the memory transport keeps queues in global state shared
        # between connections: drop leftovers from previous tests.
        self.purge()

    def teardown_method(self):
        self.c.close()

    @property
    def queues(self):
        return [queue for queue, _ in self.bindings]

    def publish(self, headers, exchange=None):
        message = self.channel.prepare_message('body', headers=headers)
        with warnings.catch_warnings():
            # unroutable messages are dropped with a warning
            warnings.simplefilter('ignore')
            self.channel.basic_publish(
                message, exchange or self.exchange, '')

    def sizes(self):
        return {
            queue: self.channel._size(queue) for queue in self.queues
        }

    def purge(self):
        for queue in self.queues:
            self.channel.queue_purge(queue)

    def test_routing_matrix(self):
        for headers, expected in self.matrix:
            self.publish(headers)
            assert self.sizes() == {
                queue: int(queue in expected) for queue in self.queues
            }, 'headers: {!r}'.format(headers)
            self.purge()

    def test_field_value_types(self):
        self.channel.queue_declare('q_h_types')
        self.channel.queue_bind(
            'q_h_types', self.exchange,
            arguments={'x-match': 'all', 'flag': True, 'count': 1},
        )
        try:
            # AMQP booleans are not integers: True must not match 1.
            self.publish({'flag': True, 'count': 1})
            assert self.channel._size('q_h_types') == 1
            self.publish({'flag': 1, 'count': 1})
            self.publish({'flag': True, 'count': True})
            assert self.channel._size('q_h_types') == 1
        finally:
            self.channel.queue_delete('q_h_types')

    def test_multiple_matching_bindings_deliver_once(self):
        self.channel.queue_declare('q_h_multi')
        self.channel.queue_bind(
            'q_h_multi', self.exchange, 'rk1',
            arguments={'x-match': 'all', 'format': 'pdf'})
        self.channel.queue_bind(
            'q_h_multi', self.exchange, 'rk2',
            arguments={'x-match': 'any', 'format': 'pdf', 'urgent': True})
        try:
            self.publish({'format': 'pdf', 'urgent': True})
            assert self.channel._size('q_h_multi') == 1
        finally:
            self.channel.queue_delete('q_h_multi')

    def test_unbind_stops_routing(self):
        arguments = {'x-match': 'all', 'format': 'pdf', 'pages': 10}
        self.publish({'format': 'pdf', 'pages': 10})
        assert self.channel._size('q_h_all') == 1
        self.purge()

        self.channel.queue_unbind('q_h_all', self.exchange,
                                  arguments=arguments)
        self.publish({'format': 'pdf', 'pages': 10})
        assert self.channel._size('q_h_all') == 0
        # other bindings are unaffected
        assert self.channel._size('q_h_any') == 1

    def test_queue_delete_stops_routing(self):
        self.channel.queue_delete('q_h_all')
        self.publish({'format': 'pdf', 'pages': 10})
        assert self.channel._size('q_h_any') == 1
        assert not self.channel.state.queue_index['q_h_all']

    def test_exchange_delete_stops_routing(self):
        self.channel.exchange_delete(self.exchange)
        assert self.exchange not in self.channel.state.exchanges
        self.publish({'format': 'pdf', 'pages': 10})
        assert all(size == 0 for size in self.sizes().values())

    def test_entity_integration(self):
        exchange = Exchange('test_memory_headers_entity', type='headers')
        queue = Queue(
            'test_memory_headers_entity_q', exchange=exchange,
            binding_arguments={'x-match': 'all', 'format': 'pdf'})
        channel = self.c.channel()
        producer = Producer(channel, exchange)
        queue(channel).declare()

        producer.publish({'n': 1}, headers={'format': 'pdf'})
        producer.publish({'n': 2}, headers={'format': 'zip'})

        assert queue(channel).get().payload == {'n': 1}
        assert queue(channel).get() is None
