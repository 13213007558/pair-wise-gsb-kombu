from __future__ import annotations

import multiprocessing
import socket
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest

from kombu import Connection, pidbox
from kombu.exceptions import ContentDisallowed, InconsistencyError, PolicyDenied
from kombu.pidbox import AuthorizationContext, Policy
from kombu.utils.uuid import uuid


def is_cast(message):
    return message['method']


def is_call(message):
    return message['method'] and message['reply_to']


class test_Mailbox:

    class Mailbox(pidbox.Mailbox):

        def _collect(self, *args, **kwargs):
            return 'COLLECTED'

    def setup_method(self):
        self.mailbox = self.Mailbox('test_pidbox')
        self.connection = Connection(transport='memory')
        self.state = {'var': 1}
        self.handlers = {'mymethod': self._handler}
        self.bound = self.mailbox(self.connection)
        self.default_chan = self.connection.channel()
        self.node = self.bound.Node(
            'test_pidbox',
            state=self.state, handlers=self.handlers,
            channel=self.default_chan,
        )

    def _handler(self, state):
        return self.stats['var']

    def test_broadcast_matcher_pattern_string_type(self):
        mailbox = pidbox.Mailbox("test_matcher_str")(self.connection)
        with pytest.raises(ValueError):
            mailbox._broadcast("ping", pattern=1, matcher=2)

    def test_publish_reply_ignores_InconsistencyError(self):
        mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
        with patch('kombu.pidbox.Producer') as Producer:
            producer = Producer.return_value = Mock(name='producer')
            producer.publish.side_effect = InconsistencyError()
            mailbox._publish_reply(
                {'foo': 'bar'}, mailbox.reply_exchange, mailbox.oid, 'foo',
            )
            producer.publish.assert_called()

    def test_reply__collect(self):
        mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
        exchange = mailbox.reply_exchange.name
        channel = self.connection.channel()
        mailbox.reply_queue(channel).declare()

        ticket = uuid()
        mailbox._publish_reply({'foo': 'bar'}, exchange, mailbox.oid, ticket)
        _callback_called = [False]

        def callback(body):
            _callback_called[0] = True

        reply = mailbox._collect(ticket, limit=1,
                                 callback=callback, channel=channel)
        assert reply == [{'foo': 'bar'}]
        assert _callback_called[0]

        ticket = uuid()
        mailbox._publish_reply({'biz': 'boz'}, exchange, mailbox.oid, ticket)
        reply = mailbox._collect(ticket, limit=1, channel=channel)
        assert reply == [{'biz': 'boz'}]

        mailbox._publish_reply({'foo': 'BAM'}, exchange, mailbox.oid, 'doom',
                               serializer='pickle')
        with pytest.raises(ContentDisallowed):
            reply = mailbox._collect('doom', limit=1, channel=channel)
        mailbox._publish_reply(
            {'foo': 'BAMBAM'}, exchange, mailbox.oid, 'doom',
            serializer='pickle',
        )
        reply = mailbox._collect('doom', limit=1, channel=channel,
                                 accept=['pickle'])
        assert reply[0]['foo'] == 'BAMBAM'

        de = mailbox.connection.drain_events = Mock()
        de.side_effect = socket.timeout
        mailbox._collect(ticket, limit=1, channel=channel)

    def test_reply__collect_uses_default_channel(self):
        class ConsumerCalled(Exception):
            pass

        def fake_Consumer(channel, *args, **kwargs):
            raise ConsumerCalled(channel)

        ticket = uuid()
        with patch('kombu.pidbox.Consumer') as Consumer:
            mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
            assert mailbox.connection.default_channel is not None
            Consumer.side_effect = fake_Consumer
            try:
                mailbox._collect(ticket, limit=1)
            except ConsumerCalled as c:
                assert c.args[0] is not None
            except Exception:
                raise
            else:
                assert False, "Consumer not called"

    def test__publish_uses_default_channel(self):
        class QueueCalled(Exception):
            pass

        def queue__call__side(channel, *args, **kwargs):
            raise QueueCalled(channel)

        ticket = uuid()
        with patch.object(pidbox.Queue, '__call__') as queue__call__:
            mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
            queue__call__.side_effect = queue__call__side
            try:
                mailbox._publish(ticket, {}, reply_ticket=ticket)
            except QueueCalled as c:
                assert c.args[0] is not None
            except Exception:
                raise
            else:
                assert False, "Queue not called"

    def test_constructor(self):
        assert self.mailbox.connection is None
        assert self.mailbox.exchange.name
        assert self.mailbox.reply_exchange.name

    def test_bound(self):
        bound = self.mailbox(self.connection)
        assert bound.connection is self.connection

    def test_Node(self):
        assert self.node.hostname
        assert self.node.state
        assert self.node.mailbox is self.bound
        assert self.handlers

        # No initial handlers
        node2 = self.bound.Node('test_pidbox2', state=self.state)
        assert node2.handlers == {}

    def test_Node_consumer(self):
        consumer1 = self.node.Consumer()
        assert consumer1.channel is self.default_chan
        assert consumer1.no_ack

        chan2 = self.connection.channel()
        consumer2 = self.node.Consumer(channel=chan2, no_ack=False)
        assert consumer2.channel is chan2
        assert not consumer2.no_ack

    def test_Node_consumer_resource_locked_raises_inconsistency(self):
        base_channel_error = self.connection.channel_errors[0]

        class ResourceLockedChannelError(base_channel_error):
            code = 405

            def __str__(self):
                return (
                    "RESOURCE_LOCKED - cannot obtain exclusive access to "
                    "locked queue 'pidbox': already using this queue"
                )

        with patch('kombu.pidbox.Consumer') as MockConsumer:
            MockConsumer.side_effect = ResourceLockedChannelError()
            with pytest.raises(InconsistencyError, match='already using'):
                self.node.Consumer()

    def test_Node_consumer_other_channel_error_propagates(self):
        base_channel_error = self.connection.channel_errors[0]

        class AccessRefusedError(base_channel_error):
            code = 403

        with patch('kombu.pidbox.Consumer') as MockConsumer:
            MockConsumer.side_effect = AccessRefusedError()
            with pytest.raises(AccessRefusedError):
                self.node.Consumer()

    def test_Node_consumer_multiple_listeners(self):
        warnings.resetwarnings()
        consumer = self.node.Consumer()
        q = consumer.queues[0]
        with warnings.catch_warnings(record=True) as log:
            q.on_declared('foo', 1, 1)
            assert log
            assert 'already using this' in log[0].message.args[0]

        with warnings.catch_warnings(record=True) as log:
            q.on_declared('foo', 1, 0)
            assert not log

    def test_handler(self):
        node = self.bound.Node('test_handler', state=self.state)

        @node.handler
        def my_handler_name(state):
            return 42

        assert 'my_handler_name' in node.handlers

    def test_dispatch(self):
        node = self.bound.Node('test_dispatch', state=self.state)

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x + y

        assert node.dispatch('my_handler_name',
                             arguments={'x': 10, 'y': 10}) == 20

    def test_dispatch_raising_SystemExit(self):
        node = self.bound.Node('test_dispatch_raising_SystemExit',
                               state=self.state)

        @node.handler
        def my_handler_name(state):
            raise SystemExit

        with pytest.raises(SystemExit):
            node.dispatch('my_handler_name')

    def test_dispatch_raising(self):
        node = self.bound.Node('test_dispatch_raising', state=self.state)

        @node.handler
        def my_handler_name(state):
            raise KeyError('foo')

        res = node.dispatch('my_handler_name')
        assert 'error' in res
        assert 'KeyError' in res['error']

    def test_dispatch_replies(self):
        _replied = [False]

        def reply(data, **options):
            _replied[0] = True

        node = self.bound.Node('test_dispatch', state=self.state)
        node.reply = reply

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x + y

        node.dispatch('my_handler_name',
                      arguments={'x': 10, 'y': 10},
                      reply_to={'exchange': 'foo', 'routing_key': 'bar'})
        assert _replied[0]

    def test_reply(self):
        _replied = [(None, None, None)]

        def publish_reply(data, exchange, routing_key, ticket, **kwargs):
            _replied[0] = (data, exchange, routing_key, ticket)

        mailbox = self.mailbox(self.connection)
        mailbox._publish_reply = publish_reply
        node = mailbox.Node('test_reply')

        @node.handler
        def my_handler_name(state):
            return 42

        node.dispatch('my_handler_name',
                      reply_to={'exchange': 'exchange',
                                'routing_key': 'rkey'},
                      ticket='TICKET')
        data, exchange, routing_key, ticket = _replied[0]
        assert data == {'test_reply': 42}
        assert exchange == 'exchange'
        assert routing_key == 'rkey'
        assert ticket == 'TICKET'

    def test_handle_message(self):
        node = self.bound.Node('test_dispatch_from_message')

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x * y

        body = {'method': 'my_handler_name',
                'arguments': {'x': 64, 'y': 64}}

        assert node.handle_message(body, None) == 64 * 64

        # message not for me should not be processed.
        body['destination'] = ['some_other_node']
        assert node.handle_message(body, None) is None

        # message for me should be processed.
        body['destination'] = ['test_dispatch_from_message']
        assert node.handle_message(body, None) is not None

        # message not for me should not be processed.
        body.pop("destination")
        body['matcher'] = 'glob'
        body["pattern"] = "something*"
        assert node.handle_message(body, None) is None

        body["pattern"] = "test*"
        assert node.handle_message(body, None) is not None

    def test_handle_message_adjusts_clock(self):
        node = self.bound.Node('test_adjusts_clock')

        @node.handler
        def my_handler_name(state):
            return 10

        body = {'method': 'my_handler_name',
                'arguments': {}}
        message = Mock(name='message')
        message.headers = {'clock': 313}
        node.adjust_clock = Mock(name='adjust_clock')
        res = node.handle_message(body, message)
        node.adjust_clock.assert_called_with(313)
        assert res == 10

    def test_listen(self):
        consumer = self.node.listen()
        assert consumer.callbacks[0] == self.node.handle_message
        assert consumer.channel == self.default_chan

    def test_cast(self):
        self.bound.cast(['somenode'], 'mymethod')
        consumer = self.node.Consumer()
        assert is_cast(self.get_next(consumer))

    def test_abcast(self):
        self.bound.abcast('mymethod')
        consumer = self.node.Consumer()
        assert is_cast(self.get_next(consumer))

    def test_call_destination_must_be_sequence(self):
        with pytest.raises(ValueError):
            self.bound.call('some_node', 'mymethod')

    def test_call(self):
        assert self.bound.call(['some_node'], 'mymethod') == 'COLLECTED'
        consumer = self.node.Consumer()
        assert is_call(self.get_next(consumer))

    def test_multi_call(self):
        assert self.bound.multi_call('mymethod') == 'COLLECTED'
        consumer = self.node.Consumer()
        assert is_call(self.get_next(consumer))

    def get_next(self, consumer):
        m = consumer.queues[0].get()
        if m:
            return m.payload

    def test_mailbox_defaults_to_exclusive(self):
        mbox = pidbox.Mailbox('flagbox_default')(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.exclusive is True
            assert q.durable is False
            assert q.auto_delete is True

    def test_mailbox_queue_exclusive(self):
        mbox = pidbox.Mailbox(
            'flagbox_ex',
            queue_exclusive=True,
            queue_durable=False,
        )(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.exclusive is True
            assert q.durable is False
            assert q.auto_delete is True

    def test_mailbox_queue_durable(self):
        mbox = pidbox.Mailbox(
            'flagbox_dur',
            queue_exclusive=False,
            queue_durable=True,
        )(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.durable is True
            assert q.exclusive is False
            assert q.auto_delete is False

    def test_mailbox_invalid_flag_combo(self):
        with pytest.raises(ValueError):
            pidbox.Mailbox(
                'flagbox_bad',
                queue_exclusive=True,
                queue_durable=True,
            )(self.connection)


GLOBAL_PIDBOX = pidbox.Mailbox('global_unittest_mailbox')


def getoid():
    return GLOBAL_PIDBOX.oid


class test_PidboxOid:
    """Unittests checking oid consistency of Pidbox"""

    def test_oid_consistency(self):
        """Tests that oid is consistent in single process"""
        m1 = pidbox.Mailbox('mailbox1')
        m2 = pidbox.Mailbox('mailbox2')
        assert m1.oid == m1.oid
        assert m2.oid == m2.oid
        assert m1.oid != m2.oid

    def test_subprocess_oid(self):
        """Tests that subprocess will not share oid with parent process."""
        oid = GLOBAL_PIDBOX.oid
        mp_context = multiprocessing.get_context('forkserver')
        with ProcessPoolExecutor(mp_context=mp_context) as e:
            res = e.submit(getoid)
            subprocess_oid = res.result()
        assert subprocess_oid != oid

    def test_thread_oid(self):
        """Tests that threads will not share oid."""
        oid = GLOBAL_PIDBOX.oid
        with ThreadPoolExecutor() as e:
            res = e.submit(getoid)
            subprocess_oid = res.result()
        assert subprocess_oid != oid


READ_ONLY_COMMANDS = frozenset({'ping', 'stats'})


class ReadOnlyOpsPolicy(Policy):
    """Example read-only operations role.

    The trusted role is read from the locally bound ``state``; anything the
    message itself claims (including a ``role`` argument or header) is
    ignored.  Commands must be on the read-only allowlist and any
    ``reply_to`` must point at this mailbox's own reply exchange.
    """

    def authorize(self, context):
        role = (context.state or {}).get('role')
        if role != 'ops-readonly':
            return False
        if context.method not in READ_ONLY_COMMANDS:
            return False
        if context.reply_to is not None:
            expected = 'reply.{}.pidbox'.format(context.state['namespace'])
            if context.reply_to.get('exchange') != expected:
                return False
        return True


class AdminPolicy(Policy):

    def authorize(self, context):
        return (context.state or {}).get('role') == 'admin'


class ExplodingPolicy(Policy):

    def authorize(self, context):
        raise RuntimeError('policy boom')


class DenyingPolicy(Policy):

    def __init__(self, reason='not allowed'):
        self.reason = reason

    def authorize(self, context):
        raise PolicyDenied(self.reason)


class test_ReceiverAuthorization:
    """Two nodes with different policies on the in-memory bus."""

    def setup_method(self):
        self.connection = Connection(transport='memory')
        self.ops_mailbox, self.ops_node, self.ops_chan = self._make_node(
            'auth_ops', ReadOnlyOpsPolicy(), 'ops-readonly')
        self.admin_mailbox, self.admin_node, self.admin_chan = self._make_node(
            'auth_admin', AdminPolicy(), 'admin')
        self.replies = []

    def _make_node(self, namespace, policy, role):
        mailbox = pidbox.Mailbox(
            namespace, type='direct', policy=policy)(self.connection)
        channel = self.connection.channel()
        state = {'role': role, 'namespace': namespace, 'handler_calls': []}
        node = mailbox.Node(
            '{}-node'.format(namespace), state=state, channel=channel)
        for name in ('ping', 'stats', 'shutdown'):
            node.handlers[name] = self._make_handler(name)
        node.listen()
        mailbox.reply_queue(channel).declare()
        return mailbox, node, channel

    def _make_handler(self, name):
        def handler(state, **kwargs):
            state['handler_calls'].append((name, kwargs))
            if name == 'ping':
                return 'pong'
            if name == 'stats':
                return {'queue': 1}
            return 'bye'
        handler.__name__ = name
        return handler

    def _drain(self, times=6):
        for _ in range(times):
            try:
                self.connection.drain_events(timeout=0.05)
            except socket.timeout:
                break

    def _spy_replies(self):
        real_publish_reply = pidbox.Mailbox._publish_reply
        replies = self.replies

        def spy(self, reply, exchange, routing_key, ticket, **kwargs):
            replies.append((self.namespace, reply, exchange, routing_key))
            return real_publish_reply(
                self, reply, exchange, routing_key, ticket, **kwargs)

        return patch.object(pidbox.Mailbox, '_publish_reply', spy)

    def test_mailbox_policy_is_inherited_by_nodes(self):
        assert self.ops_node.policy is self.ops_mailbox.policy
        assert isinstance(self.ops_node.policy, ReadOnlyOpsPolicy)

    def test_node_policy_overrides_mailbox_policy(self):
        mailbox = pidbox.Mailbox('auth_override', type='direct',
                                 policy=AdminPolicy())(self.connection)
        node = mailbox.Node('override-node', policy=ReadOnlyOpsPolicy())
        assert isinstance(node.policy, ReadOnlyOpsPolicy)
        assert mailbox.policy is not node.policy

    def test_no_policy_keeps_legacy_behavior(self):
        mailbox = pidbox.Mailbox('auth_legacy', type='direct')(
            self.connection)
        node = mailbox.Node('legacy-node', state={'handler_calls': []})
        node.handlers['ping'] = self._make_handler('ping')
        assert node.policy is None and mailbox.policy is None
        assert node.handle_message(
            {'method': 'ping', 'arguments': {}}, None) == 'pong'
        assert node.state['handler_calls'] == [('ping', {})]

    def test_allowed_command_runs_handler(self):
        self.ops_mailbox.cast(['auth_ops-node'], 'ping')
        self._drain()
        assert self.ops_node.state['handler_calls'] == [('ping', {})]

    def test_unknown_command_is_denied_without_handler_side_effect(self):
        self.ops_mailbox.cast(['auth_ops-node'], 'no_such_command')
        self._drain()
        assert self.ops_node.state['handler_calls'] == []

    def test_denied_command_has_no_side_effect_and_no_reply(self):
        with self._spy_replies():
            self.ops_mailbox.cast(['auth_ops-node'], 'shutdown')
            self._drain()
        assert self.ops_node.state['handler_calls'] == []
        assert self.replies == []

    def test_denied_call_does_not_publish_reflected_reply(self):
        with self._spy_replies():
            self.ops_mailbox.call(
                ['auth_ops-node'], 'shutdown',
                timeout=0.1, channel=self.ops_chan)
            self._drain()
        assert self.ops_node.state['handler_calls'] == []
        assert self.replies == []

    def test_allowed_call_publishes_reply(self):
        with self._spy_replies():
            self.ops_mailbox.call(
                ['auth_ops-node'], 'ping',
                timeout=0.3, channel=self.ops_chan)
            self._drain(10)
        assert self.ops_node.state['handler_calls'] == [('ping', {})]
        assert len(self.replies) == 1
        namespace, reply, exchange, routing_key = self.replies[0]
        assert namespace == 'auth_ops'
        assert reply == {'auth_ops-node': 'pong'}
        assert exchange == self.ops_mailbox.reply_exchange.name

    def test_role_claimed_in_arguments_is_not_trusted(self):
        with self._spy_replies():
            self.ops_mailbox.cast(
                ['auth_ops-node'], 'shutdown', kwargs={'role': 'admin'})
            self._drain()
        assert self.ops_node.state['handler_calls'] == []
        assert self.replies == []

    def test_role_claimed_in_headers_is_not_trusted(self):
        node = self.ops_node

        class ForgedMessage:
            headers = {'role': 'admin', 'clock': 0}

        with self._spy_replies():
            result = node.handle_message(
                {'method': 'shutdown', 'arguments': {}}, ForgedMessage())
        assert result is None
        assert node.state['handler_calls'] == []
        assert self.replies == []

    def test_out_of_bounds_reply_to_is_rejected(self):
        node = self.ops_node
        body = {
            'method': 'ping',
            'arguments': {},
            'destination': ['auth_ops-node'],
            'ticket': 'T',
            'reply_to': {'exchange': 'reply.auth_admin.pidbox',
                         'routing_key': 'attacker'},
        }
        with self._spy_replies():
            assert node.handle_message(body, None) is None
        assert node.state['handler_calls'] == []
        assert self.replies == []

    def test_policy_returning_false_fails_closed(self):
        node = self.ops_mailbox.Node(
            'false-node',
            state={'role': 'ops-readonly', 'namespace': 'auth_ops',
                   'handler_calls': []},
            channel=self.ops_chan)
        node.handlers['ping'] = self._make_handler('ping')

        class FalsePolicy(Policy):
            def authorize(self, context):
                return False

        node.policy = FalsePolicy()
        assert node.handle_message(
            {'method': 'ping', 'arguments': {}}, None) is None
        assert node.state['handler_calls'] == []

    def test_policy_raising_policy_denied_fails_closed(self, caplog):
        node = self.ops_mailbox.Node(
            'denied-node',
            state={'role': 'ops-readonly', 'namespace': 'auth_ops',
                   'handler_calls': []},
            channel=self.ops_chan, policy=DenyingPolicy('nope'))
        node.handlers['ping'] = self._make_handler('ping')
        with caplog.at_level('WARNING', logger='kombu.pidbox'):
            assert node.handle_message(
                {'method': 'ping', 'arguments': {}}, None) is None
        assert node.state['handler_calls'] == []
        assert any('denied by policy' in record.message
                   and 'nope' in record.message for record in caplog.records)

    def test_policy_raising_unexpected_error_fails_closed(self, caplog):
        node = self.ops_mailbox.Node(
            'boom-node',
            state={'role': 'ops-readonly', 'namespace': 'auth_ops',
                   'handler_calls': []},
            channel=self.ops_chan, policy=ExplodingPolicy())
        node.handlers['ping'] = self._make_handler('ping')
        with caplog.at_level('ERROR', logger='kombu.pidbox'):
            assert node.handle_message(
                {'method': 'ping', 'arguments': {}}, None) is None
        assert node.state['handler_calls'] == []
        assert any('policy raised' in record.message for record in caplog.records)

    def test_destination_not_targeting_node_skips_policy_and_handler(self):
        policy = Mock(wraps=ReadOnlyOpsPolicy())
        node = self.ops_mailbox.Node(
            'elsewhere-check', policy=policy,
            state={'role': 'ops-readonly', 'namespace': 'auth_ops',
                   'handler_calls': []},
            channel=self.ops_chan)
        node.handlers['ping'] = self._make_handler('ping')
        body = {'method': 'ping', 'arguments': {},
                'destination': ['some-other-node']}
        assert node.handle_message(body, None) is None
        policy.authorize.assert_not_called()
        assert node.state['handler_calls'] == []

    def test_broadcast_goes_through_same_policy(self):
        with self._spy_replies():
            self.ops_mailbox.abcast('stats')
            self._drain()
            self.ops_mailbox.abcast('shutdown')
            self._drain()
        calls = self.ops_node.state['handler_calls']
        assert ('stats', {}) in calls
        assert not any(name == 'shutdown' for name, _ in calls)
        assert self.replies == []

    def test_pattern_selection_goes_through_same_policy(self):
        with self._spy_replies():
            self.ops_mailbox._broadcast(
                'shutdown', {}, pattern='auth_ops-*', matcher='glob',
                channel=self.ops_chan)
            self._drain()
        assert self.ops_node.state['handler_calls'] == []
        assert self.replies == []

    def test_admin_node_may_run_command_denied_to_ops_node(self):
        self.admin_mailbox.cast(['auth_admin-node'], 'shutdown')
        self._drain()
        assert self.admin_node.state['handler_calls'] == [
            ('shutdown', {})]
        assert self.ops_node.state['handler_calls'] == []

    def test_authorization_context_carries_all_message_fields(self):
        seen = {}

        class RecordingPolicy(Policy):
            def authorize(self, context):
                seen['context'] = context
                return True

        node = self.ops_mailbox.Node(
            'context-node', policy=RecordingPolicy(),
            state={'role': 'ops-readonly', 'namespace': 'auth_ops'},
            channel=self.ops_chan)

        class Message:
            headers = {'clock': 7, 'ticket': 'hdr-ticket'}

        body = {
            'method': 'ping', 'arguments': {'x': 1},
            'destination': ['context-node'], 'pattern': None,
            'matcher': None,
            'reply_to': {'exchange': 'reply.auth_ops.pidbox',
                         'routing_key': 'rk'},
            'ticket': 'body-ticket',
        }
        assert node.authorize_message(body, Message())
        context = seen['context']
        assert isinstance(context, AuthorizationContext)
        assert context.method == 'ping'
        assert context.arguments == {'x': 1}
        assert context.destination == ['context-node']
        assert context.reply_to['routing_key'] == 'rk'
        assert context.ticket == 'body-ticket'
        assert context.headers['clock'] == 7
        assert context.body is body
        assert context.hostname == 'context-node'
        assert context.state['role'] == 'ops-readonly'

    def test_policy_cannot_be_subclassed_without_authorize(self):
        with pytest.raises(TypeError):
            Policy()
