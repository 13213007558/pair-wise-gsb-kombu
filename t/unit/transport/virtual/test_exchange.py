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

    def test_prepare_bind(self):
        arguments = {'x-match': 'all', 'format': 'pdf'}
        assert self.e.prepare_bind('qFoo', 'eFoo', '', arguments) == (
            '', arguments, 'qFoo',
        )

    def test_lookup_has_no_headers_to_match(self):
        assert self.e.lookup(
            [('', {'format': 'pdf'}, 'qFoo')], 'eFoo', 'rFoo', None,
        ) == set()

    @pytest.mark.parametrize(
        'arguments,headers,expected',
        [
            # x-match=all requires every match field.
            ({'x-match': 'all', 'a': 1, 'b': 'x'},
             {'a': 1, 'b': 'x'}, True),
            ({'x-match': 'all', 'a': 1, 'b': 'x'},
             {'a': 1}, False),  # 'b' missing
            ({'x-match': 'all', 'a': 1},
             {'a': 1, 'extra': 2}, True),  # extra headers are fine
            ({'a': 1}, {'a': 1}, True),  # default mode is 'all'
            ({'a': 1}, {'a': 2}, False),
            # x-match=any requires at least one match field.
            ({'x-match': 'any', 'a': 1, 'b': 2}, {'b': 2}, True),
            ({'x-match': 'any', 'a': 1, 'b': 2}, {'c': 3}, False),
            # AMQP field types: bool is not int (True != 1).
            ({'a': True}, {'a': True}, True),
            ({'a': True}, {'a': 1}, False),
            ({'a': 1}, {'a': True}, False),
            ({'a': False}, {'a': 0}, False),
            ({'a': 1}, {'a': 1.0}, False),  # int is not float
            # null only equals null, and never a missing header.
            ({'a': None}, {'a': None}, True),
            ({'a': None}, {}, False),
            ({'a': None}, {'a': False}, False),
            ({'a': None}, {'a': 0}, False),
            # strings.
            ({'a': 'x'}, {'a': 'x'}, True),
            ({'a': 'x'}, {'a': 'y'}, False),
            # arrays match element-wise, respecting types and order.
            ({'a': [1, 'x']}, {'a': [1, 'x']}, True),
            ({'a': [1, 'x']}, {'a': ['x', 1]}, False),
            ({'a': [1, 2]}, {'a': [1]}, False),
            ({'a': [1]}, {'a': 1}, False),
            ({'a': [True]}, {'a': [1]}, False),
            # plain all/any ignore x- prefixed match fields.
            ({'x-match': 'all', 'x-foo': 1, 'a': 1}, {'a': 1}, True),
            ({'x-match': 'all', 'x-foo': 1}, {}, True),
            ({'x-match': 'any', 'x-foo': 1}, {'x-foo': 1}, False),
            # with-x modes match on x- prefixed fields too.
            ({'x-match': 'all-with-x', 'x-foo': 1, 'a': 1},
             {'a': 1}, False),
            ({'x-match': 'all-with-x', 'x-foo': 1, 'a': 1},
             {'a': 1, 'x-foo': 1}, True),
            ({'x-match': 'any-with-x', 'x-foo': 1, 'a': 1},
             {'x-foo': 1}, True),
            ({'x-match': 'any-with-x', 'x-foo': 1, 'a': 1},
             {'b': 1}, False),
            # empty match sets: all matches everything, any nothing.
            ({'x-match': 'all'}, {'a': 1}, True),
            ({'x-match': 'any'}, {'a': 1}, False),
            ({'x-match': 'all-with-x'}, {'a': 1}, True),
            ({'x-match': 'any-with-x'}, {'a': 1}, False),
            ({}, {'a': 1}, True),
            (None, {'a': 1}, True),
            # unknown x-match mode never matches.
            ({'x-match': 'sometimes', 'a': 1}, {'a': 1}, False),
        ],
    )
    def test_matching_matrix(self, arguments, headers, expected):
        assert self.e.matches(arguments, headers) is expected

    def test_deliver(self):
        self.e.channel = Mock()
        self.e.channel.get_table.return_value = [
            ('', {'x-match': 'all', 'a': 1}, 'qFoo'),
            ('', {'x-match': 'any', 'b': 2}, 'qBar'),
            ('', {'x-match': 'all', 'c': 3}, 'qBaz'),
        ]
        message = {'body': 'body', 'headers': {'a': 1, 'b': 2}}
        self.e.deliver(message, 'eFoo', '')

        assert {call[0][0] for call in
                self.e.channel._put.call_args_list} == {'qFoo', 'qBar'}
        for call in self.e.channel._put.call_args_list:
            assert call[0][1] is message

    def test_deliver_multiple_matching_bindings_same_queue(self):
        self.e.channel = Mock()
        self.e.channel.get_table.return_value = [
            ('', {'x-match': 'all', 'a': 1}, 'qFoo'),
            ('rk2', {'x-match': 'any', 'b': 2}, 'qFoo'),
            ('rk3', {'x-match': 'all'}, 'qFoo'),
        ]
        message = {'headers': {'a': 1, 'b': 2}}
        self.e.deliver(message, 'eFoo', '')

        # all three bindings match, but the queue receives one copy.
        self.e.channel._put.assert_called_once_with('qFoo', message)

    def test_deliver_does_not_read_body(self):
        class BodyGuard(dict):
            def __getitem__(self, key):
                assert key != 'body', 'routing must not read the body'
                return super().__getitem__(key)

            def get(self, key, default=None):
                assert key != 'body', 'routing must not read the body'
                return super().get(key, default)

        self.e.channel = Mock()
        self.e.channel.get_table.return_value = [
            ('', {'a': 1}, 'qFoo'),
        ]
        message = BodyGuard(body=object(), headers={'a': 1})
        self.e.deliver(message, 'eFoo', '')

        self.e.channel._put.assert_called_once_with('qFoo', message)

    def test_deliver_unknown_exchange(self):
        self.e.channel = Mock()
        self.e.channel.get_table.side_effect = KeyError('eFoo')
        self.e.deliver({'headers': {}}, 'eFoo', '')
        self.e.channel._put.assert_not_called()


class test_HeadersIntegration:
    """End-to-end headers routing on the memory transport."""

    def setup_method(self):
        self.c = Connection(transport='memory')
        self.channel = self.c.channel()

    def _publish(self, exchange, headers):
        message = self.channel.prepare_message('body', headers=headers)
        self.channel.basic_publish(message, exchange, '')
        return message

    def test_declare(self):
        self.channel.exchange_declare('hx.declare', type='headers')
        assert isinstance(
            self.channel.typeof('hx.declare'), exchange.HeadersExchange)
        # redeclaring with the same parameters is fine.
        self.channel.exchange_declare('hx.declare', type='headers')

    def test_binding_message_matrix(self):
        self.channel.exchange_declare('hx.matrix', type='headers')
        bindings = [
            ('hx.matrix.q_all',
             {'x-match': 'all', 'format': 'pdf', 'pages': 10}),
            ('hx.matrix.q_any',
             {'x-match': 'any', 'format': 'pdf', 'priority': True}),
            ('hx.matrix.q_x',
             {'x-match': 'all-with-x', 'x-tenant': 'acme',
              'format': 'pdf'}),
        ]
        for queue, arguments in bindings:
            self.channel.queue_declare(queue)
            self.channel.queue_bind(queue,
                                    'hx.matrix', arguments=arguments)

        matrix = [
            # headers, queues expected to receive the message
            ({'format': 'pdf', 'pages': 10},
             {'hx.matrix.q_all', 'hx.matrix.q_any'}),
            ({'format': 'pdf', 'pages': 10, 'x-tenant': 'acme'},
             {'hx.matrix.q_all', 'hx.matrix.q_any', 'hx.matrix.q_x'}),
            ({'priority': True}, {'hx.matrix.q_any'}),
            ({'priority': 1}, set()),  # True != 1
            ({'format': 'pdf', 'pages': 10.0},
             {'hx.matrix.q_any'}),  # 10 != 10.0
            ({}, set()),
        ]
        counts = {queue: 0 for queue, _ in bindings}
        for headers, expected in matrix:
            self._publish('hx.matrix', headers)
            for queue in counts:
                counts[queue] += queue in expected
                assert self.channel._size(queue) == counts[queue]

    def test_multiple_bindings_same_queue_delivers_once(self):
        self.channel.exchange_declare('hx.multibind', type='headers')
        self.channel.queue_declare('hx.multibind.q')
        self.channel.queue_bind(
            'hx.multibind.q', 'hx.multibind',
            arguments={'x-match': 'all', 'a': 1})
        self.channel.queue_bind(
            'hx.multibind.q', 'hx.multibind',
            arguments={'x-match': 'all', 'b': 2})
        # an exact duplicate binding is ignored.
        self.channel.queue_bind(
            'hx.multibind.q', 'hx.multibind',
            arguments={'x-match': 'all', 'b': 2})
        assert len(self.channel.get_table('hx.multibind')) == 2

        self._publish('hx.multibind', {'a': 1, 'b': 2})  # both match
        assert self.channel._size('hx.multibind.q') == 1
        self._publish('hx.multibind', {'b': 2})
        assert self.channel._size('hx.multibind.q') == 2
        self._publish('hx.multibind', {'c': 3})
        assert self.channel._size('hx.multibind.q') == 2

    def test_unbind(self):
        self.channel.exchange_declare('hx.unbind', type='headers')
        arguments = {'x-match': 'all', 'a': 1}
        for queue in ('hx.unbind.q1', 'hx.unbind.q2'):
            self.channel.queue_declare(queue)
            self.channel.queue_bind(queue, 'hx.unbind',
                                    arguments=arguments)

        self._publish('hx.unbind', {'a': 1})
        assert self.channel._size('hx.unbind.q1') == 1
        assert self.channel._size('hx.unbind.q2') == 1

        # unbinding with different arguments keeps the binding.
        self.channel.queue_unbind(
            'hx.unbind.q1', 'hx.unbind',
            arguments={'x-match': 'all', 'a': 2})
        assert len(self.channel.get_table('hx.unbind')) == 2

        self.channel.queue_unbind(
            'hx.unbind.q1', 'hx.unbind', arguments=arguments)
        assert len(self.channel.get_table('hx.unbind')) == 1

        self._publish('hx.unbind', {'a': 1})
        assert self.channel._size('hx.unbind.q1') == 1
        assert self.channel._size('hx.unbind.q2') == 2

    def test_exchange_delete(self):
        self.channel.exchange_declare('hx.delete', type='headers')
        self.channel.queue_declare('hx.delete.q')
        self.channel.queue_bind(
            'hx.delete.q', 'hx.delete', arguments={'a': 1})
        self._publish('hx.delete', {'a': 1})
        assert self.channel._size('hx.delete.q') == 1

        self.channel.exchange_delete('hx.delete')
        assert 'hx.delete' not in self.channel.state.exchanges

        # messages published after the delete are not routed.
        self._publish('hx.delete', {'a': 1})
        assert self.channel._size('hx.delete.q') == 1

        # redeclaring starts over with an empty binding table.
        self.channel.exchange_declare('hx.delete', type='headers')
        self._publish('hx.delete', {'a': 1})
        assert self.channel._size('hx.delete.q') == 1

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
