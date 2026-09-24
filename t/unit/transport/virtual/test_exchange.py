from __future__ import annotations

from uuid import uuid4

from unittest.mock import Mock

import pytest

from kombu import Connection, Exchange, Producer, Queue, binding
from kombu.transport.virtual import exchange
from t.mocks import Transport


class ExchangeCase:
    type = None

    def setup_method(self):
        if self.type:
            self.e = self.type(Connection(transport=Transport).channel())


class test_Direct(ExchangeCase):
    type = exchange.DirectExchange
    table = [('rFoo', None, 'qFoo'),
             ('rFoo', None, 'qFox'),
             ('rBar', None, 'qBar'),
             ('rBaz', None, 'qBaz')]

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'rFoo', None, {'qFoo', 'qFox'}),
        ('eMoz', 'rMoz', 'DEFAULT', set()),
        ('eBar', 'rBar', None, {'qBar'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected


class test_Fanout(ExchangeCase):
    type = exchange.FanoutExchange
    table = [(None, None, 'qFoo'),
             (None, None, 'qFox'),
             (None, None, 'qBar')]

    def test_lookup(self):
        assert self.e.lookup(self.table, 'eFoo', 'rFoo', None) == {
            'qFoo', 'qFox', 'qBar',
        }

    def test_deliver_when_fanout_supported(self):
        self.e.channel = Mock()
        self.e.channel.supports_fanout = True
        message = Mock()

        self.e.deliver(message, 'exchange', 'rkey')
        self.e.channel._put_fanout.assert_called_with(
            'exchange', message, 'rkey',
        )

    def test_deliver_when_fanout_unsupported(self):
        self.e.channel = Mock()
        self.e.channel.supports_fanout = False

        self.e.deliver(Mock(), 'exchange', None)
        self.e.channel._put_fanout.assert_not_called()


class test_Topic(ExchangeCase):
    type = exchange.TopicExchange
    table = [
        ('stock.#', None, 'rFoo'),
        ('stock.us.*', None, 'rBar'),
    ]

    def setup_method(self):
        super().setup_method()
        self.table = [(rkey, self.e.key_to_pattern(rkey), queue)
                      for rkey, _, queue in self.table]

    def test_prepare_bind(self):
        x = self.e.prepare_bind('qFoo', 'eFoo', 'stock.#', {})
        assert x[0] == 'stock.#'
        assert x[1] is self.e.key_to_pattern('stock.#')
        assert x[2] == 'qFoo'

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'stock.us.nasdaq', None, {'rFoo', 'rBar'}),
        ('eFoo', 'stock.europe.OSE', None, {'rFoo'}),
        ('eFoo', 'stockxeuropexOSE', None, set()),
        ('eFoo', 'candy.schleckpulver.snap_crackle', None, set()),
        # '*' is a single word: only 'stock.#' may match a longer key.
        ('eFoo', 'stock.us.nasdaq.tech', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected
        assert self.e._compiled

    @pytest.mark.parametrize('binding_key,routing_key,should_match', [
        ('a.#', '', False),
        ('a.#', 'a', True),
        ('a.#', 'a.b', True),
        ('a.#', 'b', False),
        ('#.b', 'b', True),
        ('#.b', 'a.b', True),
        ('#.b', 'x.y.b', True),
        ('#.b', 'a.b.c', False),
        ('a.#.b', 'a.b', True),
        ('a.#.b', 'a.x.b', True),
        ('a.#.b', 'a.x.y.b', True),
        ('a.#.b', 'a', False),
        ('a.#.b', 'b', False),
        ('a.#.b', 'a.b.c', False),
        ('#', '', True),
        ('#', 'a.b.c', True),
        ('#.#', '', True),
        ('#.#', 'a', True),
        ('#.#', 'a.b', True),
        ('#.#.#', '', True),
        ('#.#.#', 'a.b', True),
        ('a.#.#.b', 'a.b', True),
        ('a.#.#.b', 'a.x.b', True),
        ('a.#.#.b', 'a.x.y.b', True),
        ('*', 'a', True),
        ('*', '', False),
        ('*', 'a.b', False),
        ('a.*.b', 'a.x.b', True),
        ('a.*.b', 'a.b', False),
        ('a.*.b', 'a..b', False),
        ('a.*.b', 'a.x.y.b', False),
        ('#.*.#', 'a', True),
        ('#.*.#', '', False),
        ('#.*.#', 'a.b.c', True),
        ('a+b.$^.[bar]', 'a+b.$^.[bar]', True),
        ('a+b.$^.[bar]', 'aaa.+bbbb.$^.[bbar]', False),
        (r'a\b.c+d', r'a\b.c+d', True),
        (r'a\b.c+d', 'aab.cd', False),
        ('a#b', 'a#b', True),
        ('a#b', 'axb', False),
        ('*b', '*b', True),
        ('*b', 'ab', False),
    ])
    def test_key_to_pattern_word_matching(
            self, binding_key, routing_key, should_match):
        matcher = self.e.key_to_pattern(binding_key)

        assert matcher.match(routing_key) is should_match

    def test_state_count_is_linear_in_binding_length(self):
        matcher = exchange.TopicMatcher('.'.join(['#'] * 100))
        states = matcher.initial_states()
        max_states = len(states)

        for word in ['word'] * 1000:
            states = matcher.next_states(states, word)
            max_states = max(max_states, len(states))

        assert states
        assert max_states <= len(matcher.words) + 1

    def test_long_key_with_many_wildcards_matches(self):
        binding_key = '.'.join(['#', '*', '#', '*', '#'])
        routing_key = '.'.join(['word'] * 1000)

        assert exchange.TopicMatcher(binding_key).match(routing_key)

    def test_memory_topic_bindings_deliver_once(self):
        connection = Connection(transport='memory')
        channel = connection.channel()
        suffix = uuid4().hex
        topic_exchange = Exchange(f'topic-{suffix}', type='topic')
        topic_queue = Queue(
            f'queue-{suffix}',
            bindings=[
                binding(topic_exchange, 'a.#'),
                binding(topic_exchange, '#.b'),
                binding(topic_exchange, 'a.#.b'),
            ],
        )
        producer = Producer(channel)

        producer.publish(
            {'hello': 'world'},
            exchange=topic_exchange,
            routing_key='a.b',
            declare=[topic_queue],
        )

        message = topic_queue(channel).get()
        assert message.payload == {'hello': 'world'}
        assert topic_queue(channel).get() is None

    def test_cached_matcher_is_used_after_unbind_and_rebind(self):
        connection = Connection(transport='memory')
        channel = connection.channel()
        suffix = uuid4().hex
        topic_exchange = f'topic-rebind-{suffix}'
        topic_queue = f'queue-rebind-{suffix}'
        cached = self.e.key_to_pattern('a.#')

        channel.exchange_declare(topic_exchange, type='topic')
        channel.queue_declare(topic_queue)
        channel.queue_bind(topic_queue, topic_exchange, 'a.#')

        assert channel._lookup(topic_exchange, 'a') == {topic_queue}
        channel.queue_unbind(topic_queue, topic_exchange, 'a.#')
        assert channel.get_table(topic_exchange) == []

        channel.queue_bind(topic_queue, topic_exchange, 'a.#')
        assert channel.get_table(topic_exchange) == [
            ('a.#', cached, topic_queue),
        ]
        assert channel._lookup(topic_exchange, 'a') == {topic_queue}

    def test_deliver(self):
        self.e.channel = Mock()
        self.e.channel._lookup.return_value = ('a', 'b')
        message = Mock()
        self.e.deliver(message, 'exchange', 'rkey')

        assert self.e.channel._put.call_args_list == [
            (('a', message), {}),
            (('b', message), {}),
        ]


class test_TopicMultibind(ExchangeCase):
    # Testing message delivery in case of multiple overlapping
    # bindings for the same queue. As AMQP states, in case of
    # overlapping bindings, a message must be delivered once to
    # each matching queue.
    type = exchange.TopicExchange
    table = [
        ('stock', None, 'rFoo'),
        ('stock.#', None, 'rFoo'),
        ('stock.us.*', None, 'rFoo'),
        ('#', None, 'rFoo'),
    ]

    def setup_method(self):
        super().setup_method()
        self.table = [(rkey, self.e.key_to_pattern(rkey), queue)
                      for rkey, _, queue in self.table]

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'stock.us.nasdaq', None, {'rFoo'}),
        ('eFoo', 'stock.europe.OSE', None, {'rFoo'}),
        ('eFoo', 'stockxeuropexOSE', None, {'rFoo'}),
        ('eFoo', 'candy.schleckpulver.snap_crackle', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e._compiled
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected


class test_ExchangeType(ExchangeCase):
    type = exchange.ExchangeType

    def test_lookup(self):
        with pytest.raises(NotImplementedError):
            self.e.lookup([], 'eFoo', 'rFoo', None)

    def test_prepare_bind(self):
        assert self.e.prepare_bind('qFoo', 'eFoo', 'rFoo', {}) == (
            'rFoo', None, 'qFoo',
        )

    e1 = {
        'type': 'direct',
        'durable': True,
        'auto_delete': True,
        'arguments': {},
    }
    e2 = dict(e1, arguments={'expires': 3000})

    @pytest.mark.parametrize('ex,eq,name,type,durable,auto_delete,arguments', [
        (e1, True, 'eFoo', 'direct', True, True, {}),
        (e1, False, 'eFoo', 'topic', True, True, {}),
        (e1, False, 'eFoo', 'direct', False, True, {}),
        (e1, False, 'eFoo', 'direct', True, False, {}),
        (e1, False, 'eFoo', 'direct', True, True, {'expires': 3000}),
        (e2, True, 'eFoo', 'direct', True, True, {'expires': 3000}),
        (e2, False, 'eFoo', 'direct', True, True, {'expires': 6000}),
    ])
    def test_equivalent(
            self, ex, eq, name, type, durable, auto_delete, arguments):
        is_eq = self.e.equivalent(
            ex, name, type, durable, auto_delete, arguments)
        assert is_eq if eq else not is_eq
