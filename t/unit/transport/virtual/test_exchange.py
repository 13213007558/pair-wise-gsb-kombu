from __future__ import annotations

import itertools
from unittest.mock import Mock

import pytest

from kombu import Connection
from kombu.transport.virtual import exchange
from t.mocks import Transport


def reference_topic_match(pattern, routing_key):
    """Independent brute-force topic matcher used to cross-check results.

    Tries every possible split of the routing key words among the
    pattern words, with ``*`` taking exactly one non-empty word and
    ``#`` taking zero or more words.
    """
    pattern_words = pattern.split('.') if pattern else []
    key_words = routing_key.split('.') if routing_key else []

    def match(pi, ki):
        if pi == len(pattern_words):
            return ki == len(key_words)
        word = pattern_words[pi]
        if word == '#':
            return any(match(pi + 1, end)
                       for end in range(ki, len(key_words) + 1))
        if ki >= len(key_words):
            return False
        if word == '*':
            return bool(key_words[ki]) and match(pi + 1, ki + 1)
        return word == key_words[ki] and match(pi + 1, ki + 1)

    return match(0, 0)


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
        assert x == ('stock.#', r'^stock\..*?$', 'qFoo')

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'stock.us.nasdaq', None, {'rFoo', 'rBar'}),
        ('eFoo', 'stock.europe.OSE', None, {'rFoo'}),
        ('eFoo', 'stockxeuropexOSE', None, set()),
        ('eFoo', 'candy.schleckpulver.snap_crackle', None, set()),
        # '*' is a single word: only 'stock.#' may match a longer key.
        ('eFoo', 'stock.us.nasdaq.tech', None, {'rFoo'}),
        # '#' matches zero words, so 'stock.#' also matches 'stock'.
        ('eFoo', 'stock', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected

    def test_lookup_with_legacy_persisted_pattern(self):
        # binding record as persisted by versions that stored a regex
        # in the pattern field (e.g. redis/mongodb member strings):
        # routing must use the stored routing key, not the regex.
        table = [('stock.#', r'^stock\..*?$', 'rFoo')]
        assert self.e.lookup(table, 'eFoo', 'stock.us', None) == {'rFoo'}
        assert self.e.lookup(table, 'eFoo', 'candy.us', None) == set()

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
        # multiple overlapping bindings still deliver only once.
        ('eFoo', 'stock', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected


class test_topic_match:

    @pytest.mark.parametrize('pattern,routing_key,expected', [
        # empty routing keys
        ('', '', True),
        ('#', '', True),
        ('#.#', '', True),
        ('*', '', False),
        ('', 'a', False),
        ('a', '', False),
        # literal words
        ('a', 'a', True),
        ('a', 'b', False),
        # '#' matches zero or more words
        ('stock.#', 'stock', True),
        ('stock.#', 'stock.us', True),
        ('stock.#', 'stock.us.nasdaq', True),
        ('stock.#', 'stockx', False),
        ('a.#.b', 'a.b', True),
        ('a.#.b', 'a.x.y.b', True),
        ('a.#.b', 'a.x.y.c', False),
        # '*' matches exactly one word
        ('stock.*', 'stock', False),
        ('stock.*', 'stock.us', True),
        ('stock.*', 'stock.us.nasdaq', False),
        ('a.*.b', 'a..b', False),
        # adjacent wildcards
        ('#.#', 'a', True),
        ('#.*.#', 'a', True),
        ('#.*.#', 'a.b', True),
        ('*.*', 'a', False),
        ('*.*', 'a.b', True),
        ('*.#', 'a.b.c', True),
        # regular expression metacharacters are literal
        ('a.b$c', 'a.b$c', True),
        ('a.b$c', 'a.bbc', False),
        ('a.+', 'a.+', True),
        ('a.+', 'a.b', False),
        ('a[0-9]', 'a[0-9]', True),
        ('a[0-9]', 'a5', False),
        ('a.*', 'a.+', True),
        # wildcards are only special as whole words
        ('a*', 'a*', True),
        ('a*', 'a', False),
        ('a#', 'a#', True),
        ('a#', 'a', False),
        ('**', '**', True),
        ('**', 'a', False),
    ])
    def test_match(self, pattern, routing_key, expected):
        assert exchange.topic_match(pattern, routing_key) == expected

    def test_exhaustive_delivery_sets_against_reference(self):
        pattern_words = ['a', '*', '#']
        patterns = [''] + [
            '.'.join(words)
            for n in (1, 2, 3)
            for words in itertools.product(pattern_words, repeat=n)
        ]
        routing_keys = [''] + [
            '.'.join(words)
            for n in (1, 2, 3, 4)
            for words in itertools.product('ab', repeat=n)
        ]
        e = exchange.TopicExchange(Mock())
        table = [(rkey, e.key_to_pattern(rkey), rkey) for rkey in patterns]
        for routing_key in routing_keys:
            expected = {p for p in patterns
                        if reference_topic_match(p, routing_key)}
            assert e.lookup(table, 'eFoo', routing_key, None) == expected

    def test_state_visits_grow_linearly_for_long_nonmatching_keys(self):
        pattern = '#.#.#.zzz'
        lengths = (8, 16, 32, 64, 128)
        counts = []
        for n in lengths:
            visits = []
            routing_key = '.'.join(['a'] * n)
            assert not exchange.topic_match(
                pattern, routing_key, _visits=visits)
            counts.append(visits[0])
        # each of the three '#' words visits n + 1 states and the
        # literal 'zzz' visits n states, so the work is exactly 4n + 3
        # state evaluations regardless of the input content.
        assert counts == [4 * n + 3 for n in lengths]
        assert counts[-1] <= 16 * counts[0]


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
