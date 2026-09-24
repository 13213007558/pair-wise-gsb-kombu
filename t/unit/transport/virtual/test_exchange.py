from __future__ import annotations

from unittest.mock import Mock

import pytest

from kombu import Connection
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
        assert x == ('stock.#', r'^stock\..*?$', 'qFoo')

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


class test_Headers(ExchangeCase):
    type = exchange.HeadersExchange

    #: binding matrix: (queue, binding arguments)
    bindings = [
        ('qAll', {'x-match': 'all', 'format': 'pdf', 'pages': 10}),
        ('qAny', {'x-match': 'any', 'format': 'pdf', 'urgent': True}),
        ('qAllX', {'x-match': 'all-with-x',
                   'x-tenant': 'eu', 'format': 'pdf'}),
        ('qAnyX', {'x-match': 'any-with-x', 'x-tenant': 'us'}),
        ('qEmptyAll', {'x-match': 'all'}),
        ('qEmptyAny', {'x-match': 'any'}),
        # plain 'all' ignores x- fields: no match fields left.
        ('qXIgnored', {'x-match': 'all', 'x-tenant': 'eu'}),
    ]

    def setup_method(self):
        super().setup_method()
        self.table = [
            self.e.prepare_bind(queue, 'eFoo', '', arguments)
            for queue, arguments in self.bindings
        ]

    def test_prepare_bind(self):
        arguments = {'x-match': 'any', 'format': 'pdf'}
        assert self.e.prepare_bind('qFoo', 'eFoo', '', arguments) == (
            '', arguments, 'qFoo',
        )

    def test_prepare_bind__defaults(self):
        assert self.e.prepare_bind('qFoo', 'eFoo', 'rFoo', None) == (
            'rFoo', {}, 'qFoo',
        )

    @pytest.mark.parametrize('match', ['sometimes', 'all-with-y', '', 42])
    def test_prepare_bind__invalid_x_match(self, match):
        with pytest.raises(ValueError):
            self.e.prepare_bind('qFoo', 'eFoo', '', {'x-match': match})

    #: message/binding matrix: message headers -> expected queues
    @pytest.mark.parametrize('headers,expected', [
        ({'format': 'pdf', 'pages': 10},
         {'qAll', 'qAny', 'qEmptyAll', 'qXIgnored'}),
        ({'format': 'pdf', 'pages': 10, 'x-tenant': 'eu'},
         {'qAll', 'qAny', 'qAllX', 'qEmptyAll', 'qXIgnored'}),
        ({'format': 'pdf', 'x-tenant': 'eu'},
         {'qAny', 'qAllX', 'qEmptyAll', 'qXIgnored'}),
        ({'format': 'zip', 'pages': 10},
         {'qEmptyAll', 'qXIgnored'}),
        ({'urgent': True},
         {'qAny', 'qEmptyAll', 'qXIgnored'}),
        ({'urgent': False},
         {'qEmptyAll', 'qXIgnored'}),
        ({'x-tenant': 'us'},
         {'qAnyX', 'qEmptyAll', 'qXIgnored'}),
        ({'x-tenant': 'eu', 'x-tenant-other': 'us'},
         {'qEmptyAll', 'qXIgnored'}),
        # 'any' with no match fields never matches, 'all' always does.
        ({},
         {'qEmptyAll', 'qXIgnored'}),
    ])
    def test_lookup_matrix(self, headers, expected):
        assert self.e.lookup(
            self.table, 'eFoo', '', None, headers=headers) == expected

    def test_lookup__no_headers(self):
        assert self.e.lookup(self.table, 'eFoo', '', None) == {
            'qEmptyAll', 'qXIgnored',
        }


class test_HeadersFieldValues(ExchangeCase):
    type = exchange.HeadersExchange

    def setup_method(self):
        super().setup_method()
        arguments = {
            'x-match': 'all',
            'missing-is-no-match': None,
            'null': None,
            'flag': True,
            'count': 1,
            'name': 'doc',
            'tags': ['a', 'b'],
        }
        self.table = [
            self.e.prepare_bind('qFoo', 'eFoo', '', arguments),
        ]

    def matching_headers(self, **overrides):
        headers = {
            'missing-is-no-match': None,
            'null': None,
            'flag': True,
            'count': 1,
            'name': 'doc',
            'tags': ['a', 'b'],
        }
        headers.update(overrides)
        return headers

    def lookup(self, headers):
        return self.e.lookup(self.table, 'eFoo', '', None, headers=headers)

    def test_exact_match(self):
        assert self.lookup(self.matching_headers()) == {'qFoo'}

    def test_extra_headers_ignored(self):
        assert self.lookup(self.matching_headers(extra='value')) == {'qFoo'}

    def test_missing_header_never_matches(self):
        headers = self.matching_headers()
        del headers['null']
        assert self.lookup(headers) == set()

    def test_null_only_matches_null(self):
        assert self.lookup(self.matching_headers(**{'null': 0})) == set()
        assert self.lookup(self.matching_headers(**{'null': ''})) == set()
        assert self.lookup(self.matching_headers(**{'null': False})) == set()

    def test_bool_does_not_match_int(self):
        # Python says True == 1, AMQP field values say otherwise.
        assert self.lookup(self.matching_headers(flag=1)) == set()
        assert self.lookup(self.matching_headers(count=True)) == set()

    def test_int_does_not_match_float_or_str(self):
        assert self.lookup(self.matching_headers(count=1.0)) == set()
        assert self.lookup(self.matching_headers(count='1')) == set()

    def test_str_is_case_sensitive(self):
        assert self.lookup(self.matching_headers(name='Doc')) == set()

    def test_array_must_match_exactly(self):
        assert self.lookup(self.matching_headers(tags=['a', 'b'])) == {
            'qFoo'}
        assert self.lookup(self.matching_headers(tags=['b', 'a'])) == set()
        assert self.lookup(self.matching_headers(tags=['a'])) == set()
        assert self.lookup(self.matching_headers(tags=['a', 'b', 'c'])) == (
            set())
        assert self.lookup(self.matching_headers(tags='ab')) == set()

    def test_array_elements_keep_their_types(self):
        table = [
            self.e.prepare_bind(
                'qFoo', 'eFoo', '', {'x-match': 'all', 'tags': [True, 1]}),
        ]
        lookup = self.e.lookup(
            table, 'eFoo', '', None, headers={'tags': [True, 1]})
        assert lookup == {'qFoo'}
        lookup = self.e.lookup(
            table, 'eFoo', '', None, headers={'tags': [1, 1]})
        assert lookup == set()


class test_HeadersMultibind(ExchangeCase):
    # Multiple matching bindings for the same queue must result in a
    # single delivery to that queue.
    type = exchange.HeadersExchange
    table = [
        ('', {'x-match': 'all', 'format': 'pdf'}, 'rFoo'),
        ('', {'x-match': 'any', 'format': 'pdf', 'urgent': True}, 'rFoo'),
        ('', {'x-match': 'all'}, 'rFoo'),
    ]

    def test_lookup(self):
        assert self.e.lookup(
            self.table, 'eFoo', '', None,
            headers={'format': 'pdf', 'urgent': True}) == {'rFoo'}

    def test_deliver(self):
        self.e.channel = Mock()
        self.e.channel._lookup.return_value = ('a', 'b')
        message = {'headers': {'format': 'pdf'}}
        self.e.deliver(message, 'exchange', 'rkey')

        self.e.channel._lookup.assert_called_with(
            'exchange', 'rkey', headers={'format': 'pdf'})
        assert self.e.channel._put.call_args_list == [
            (('a', message), {}),
            (('b', message), {}),
        ]

    def test_deliver__message_without_headers(self):
        self.e.channel = Mock()
        self.e.channel._lookup.return_value = ()
        message = {'body': 'the-body'}
        self.e.deliver(message, 'exchange', 'rkey')

        # routing must not depend on (or even read) the message body.
        self.e.channel._lookup.assert_called_with(
            'exchange', 'rkey', headers=None)


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
