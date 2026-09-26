from __future__ import annotations

from unittest.mock import patch

import pytest

from kombu import Connection, Exchange, Queue
from kombu.exceptions import (
    TopologyConflict,
    TopologyCycleError,
    TopologyExecutionError,
    UnsupportedExchangeType,
    UnsupportedTopologyFeature,
)
from kombu.transport import memory
from kombu.transport.virtual import BrokerState
from kombu.topology import ExchangeBinding, Operation, TopologyPlan


@pytest.fixture(autouse=True)
def _isolate_memory_transport_state():
    memory.Transport.global_state.clear()
    memory.Channel.queues.clear()
    yield
    memory.Transport.global_state.clear()
    memory.Channel.queues.clear()


class LimitedTransport(memory.Transport):
    """Memory transport that only declares direct exchanges."""

    implements = memory.Transport.implements.extend(
        exchange_type=frozenset(['direct']))

    class Channel(memory.Channel):
        pass

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.state = BrokerState()  # avoid the shared global state


class ExchangeBindTransport(LimitedTransport):
    """Transport whose channels implement exchange-to-exchange binding."""

    class Channel(LimitedTransport.Channel):

        def exchange_bind(self, destination, source='', routing_key='',
                          nowait=False, arguments=None):
            return ('bound', destination, source, routing_key)


def kinds(operations):
    return [operation.kind for operation in operations]


def describe(operations):
    return [operation.description for operation in operations]


class test_TopologyPlanCollection:

    def test_add_rejects_unknown_entity(self):
        plan = TopologyPlan()
        with pytest.raises(TypeError):
            plan.add(object())

    def test_identical_exchange_is_deduplicated(self):
        plan = TopologyPlan()
        e1 = Exchange('events', type='topic')
        e2 = Exchange('events', type='topic')
        assert plan.add_exchange(e1) is e1
        assert plan.add_exchange(e2) is e1
        assert plan.exchanges == [e1]

    def test_exchange_conflicting_type_is_reported(self):
        plan = TopologyPlan([
            Exchange('events', type='topic'),
        ])
        with pytest.raises(TopologyConflict) as exc:
            plan.add_exchange(Exchange('events', type='fanout'))
        assert 'events' in str(exc.value)
        assert 'type' in str(exc.value)

    def test_exchange_conflicting_durable_is_reported(self):
        plan = TopologyPlan([
            Exchange('events', type='topic', durable=True),
        ])
        with pytest.raises(TopologyConflict) as exc:
            plan.add_exchange(
                Exchange('events', type='topic', durable=False))
        assert 'durable' in str(exc.value)

    def test_queue_conflicting_arguments_is_reported(self):
        plan = TopologyPlan([
            Queue('tasks', queue_arguments={'x-message-ttl': 1000}),
        ])
        with pytest.raises(TopologyConflict) as exc:
            plan.add_queue(
                Queue('tasks', queue_arguments={'x-message-ttl': 2000}))
        assert 'tasks' in str(exc.value)
        assert 'arguments' in str(exc.value)

    def test_compatible_queue_redeclaration_merges_bindings(self):
        exchange = Exchange('events', type='topic')
        plan = TopologyPlan([
            Queue('tasks', exchange, routing_key='a'),
            Queue('tasks', exchange, routing_key='b'),
        ])
        operations = plan.plan()
        assert kinds(operations) == [
            'exchange.declare', 'queue.declare', 'queue.bind', 'queue.bind']
        assert describe(operations)[2].endswith("routing_key='a')")
        assert describe(operations)[3].endswith("routing_key='b')")

    def test_server_named_queues_are_never_deduplicated(self):
        exchange = Exchange('events', type='topic')
        q1 = Queue(exchange=exchange, routing_key='a')
        q2 = Queue(exchange=exchange, routing_key='b')
        plan = TopologyPlan([q1, q2])
        assert plan.queues == [q1, q2]
        operations = plan.plan()
        assert kinds(operations).count('queue.declare') == 2

    def test_server_named_queue_added_twice_stays_single(self):
        q = Queue(exchange=Exchange('events', type='topic'))
        plan = TopologyPlan([q, q])
        assert plan.queues == [q]

    def test_no_declare_entities_are_skipped(self):
        plan = TopologyPlan([
            Exchange('hidden', no_declare=True),
            Queue('hidden-q', no_declare=True),
        ])
        assert plan.exchanges == []
        assert plan.queues == []
        assert plan.plan() == []

    def test_queue_collects_its_exchanges_and_bindings(self):
        main = Exchange('main', type='direct')
        extra = Exchange('extra', type='topic')
        # a Queue with explicit bindings lists every binding, incl.
        # the one to its primary exchange.
        q = Queue('tasks', bindings=[
            main.binding('rk'), extra.binding('other')])
        plan = TopologyPlan([q])
        assert {e.name for e in plan.exchanges} == {'main', 'extra'}
        assert plan.queues == [q]

    def test_add_exchange_binding_collects_endpoints(self):
        source = Exchange('source', type='topic')
        destination = Exchange('destination', type='topic')
        plan = TopologyPlan()
        binding = plan.add_exchange_binding(destination, source, 'rk')
        assert isinstance(binding, ExchangeBinding)
        assert plan.exchange_bindings == [binding]
        assert {e.name for e in plan.exchanges} == {'source', 'destination'}


class test_TopologyPlanOrderingAndValidation:

    def test_exchange_binding_dependency_order(self):
        source = Exchange('source', type='topic')
        destination = Exchange('destination', type='topic')
        plan = TopologyPlan([destination, source])
        plan.add_exchange_binding(destination, source, 'rk')
        operations = plan.plan()
        assert kinds(operations) == [
            'exchange.declare', 'exchange.declare', 'exchange.bind']
        declared = [op.entity.name for op in operations[:2]]
        assert declared.index('source') < declared.index('destination')
        assert operations[2].kind == 'exchange.bind'

    def test_exchange_binding_cycle_is_rejected(self):
        alpha = Exchange('alpha', type='topic')
        beta = Exchange('beta', type='topic')
        plan = TopologyPlan([alpha, beta])
        plan.add_exchange_binding(alpha, beta)
        plan.add_exchange_binding(beta, alpha)
        with pytest.raises(TopologyCycleError) as exc:
            plan.validate()
        message = str(exc.value)
        assert 'alpha' in message and 'beta' in message

    def test_unsupported_exchange_type(self):
        conn = Connection(transport=LimitedTransport)
        plan = TopologyPlan([Exchange('events', type='topic')])
        with pytest.raises(UnsupportedExchangeType):
            plan.validate(conn)

    def test_exchange_bind_unsupported_by_memory_transport(self):
        conn = Connection(transport='memory')
        plan = TopologyPlan()
        plan.add_exchange_binding(
            Exchange('destination'), Exchange('source'))
        with pytest.raises(UnsupportedTopologyFeature):
            plan.validate(conn)

    def test_exchange_bind_supported_by_capable_transport(self):
        conn = Connection(transport=ExchangeBindTransport)
        plan = TopologyPlan()
        plan.add_exchange_binding(
            Exchange('destination'), Exchange('source'), 'rk')
        assert plan.validate(conn) is True
        completed = plan.execute(connection=conn)
        assert kinds(completed) == [
            'exchange.declare', 'exchange.declare', 'exchange.bind']
        assert conn.connected is True

    def test_plan_without_transport_skips_capability_checks(self):
        plan = TopologyPlan([
            Exchange('exotic', type='x-consistent-hash'),
        ])
        operations = plan.plan()
        assert operations[0].kind == 'exchange.declare'

    def test_validation_accepts_connection_without_connecting(self):
        conn = Connection('memory://')
        exchanges_before = dict(memory.Transport.global_state.exchanges)
        queues_before = dict(memory.Channel.queues)
        plan = TopologyPlan([
            Queue('tasks', Exchange('tasks', type='topic'),
                  routing_key='rk')])
        assert plan.validate(conn) is True
        assert plan.validate(conn.transport) is True
        assert conn.connected is False
        assert conn.transport.channels == []
        assert memory.Transport.global_state.exchanges == exchanges_before
        assert memory.Channel.queues == queues_before

    def test_preflight_with_limited_transport_has_no_side_effects(self):
        conn = Connection(transport=LimitedTransport)
        transport = conn.transport
        assert transport.state.exchanges == {}
        assert transport.channels == []
        plan = TopologyPlan([
            Queue('tasks', Exchange('tasks', type='topic'),
                  routing_key='rk')])
        with pytest.raises(UnsupportedExchangeType):
            plan.plan(conn)
        assert conn.connected is False
        assert transport.channels == []
        assert transport.state.exchanges == {}
        plan2 = TopologyPlan()
        plan2.add_exchange_binding(
            Exchange('destination'), Exchange('source'))
        with pytest.raises(UnsupportedTopologyFeature):
            plan2.plan(conn)
        assert transport.state.exchanges == {}


class test_TopologyPlanExecution:

    def test_execute_declares_in_order_on_memory_transport(self):
        conn = Connection('memory://')
        exchange = Exchange('events', type='topic')
        queue = Queue('tasks', exchange, routing_key='rk')
        plan = TopologyPlan([queue])
        completed = plan.execute(connection=conn)
        assert kinds(completed) == [
            'exchange.declare', 'queue.declare', 'queue.bind']
        state = conn.transport.state
        assert 'events' in state.exchanges
        assert ('tasks', 'events', 'rk') in state.bindings

    def test_execute_with_explicit_channel(self):
        conn = Connection('memory://')
        channel = conn.channel()
        queue = Queue('tasks', Exchange('events'), routing_key='rk')
        plan = TopologyPlan([queue])
        completed = plan.execute(channel=channel)
        assert kinds(completed) == [
            'exchange.declare', 'queue.declare', 'queue.bind']

    def test_execute_requires_channel_or_connection(self):
        plan = TopologyPlan([Queue('q')])
        with pytest.raises(TypeError):
            plan.execute()

    def test_execute_revalidates_transport_capabilities(self):
        conn = Connection(transport=LimitedTransport)
        plan = TopologyPlan([Exchange('events', type='topic')])
        with pytest.raises(UnsupportedExchangeType):
            plan.execute(connection=conn)
        assert conn.connected is False

    def test_server_named_queue_name_backfilled_into_binding(self):
        conn = Connection('memory://')
        exchange = Exchange('events', type='topic')
        queue = Queue(exchange=exchange, routing_key='rk')
        assert not queue.name
        empty_name_key = hash('Q|')
        plan = TopologyPlan([queue])
        completed = plan.execute(connection=conn)
        assert queue.name  # broker-generated name was backfilled
        assert queue.name.startswith('amq.gen-')
        assert kinds(completed) == [
            'exchange.declare', 'queue.declare', 'queue.bind']
        state = conn.transport.state
        assert (queue.name, 'events', 'rk') in state.bindings
        # the empty name must never end up in the declaration cache
        assert empty_name_key not in conn.declared_entities
        assert hash(queue) in conn.declared_entities

    def test_execute_uses_declaration_cache(self):
        conn = Connection('memory://')
        exchange = Exchange('events', type='topic')
        queue = Queue('tasks', exchange, routing_key='rk')
        plan = TopologyPlan([queue])
        first = plan.execute(connection=conn)
        assert len(first) == 3
        # second run is fully served from declared_entities
        second = plan.execute(connection=conn)
        assert second == []

    def test_execute_failure_reports_completed_and_failed_step(self):
        conn = Connection('memory://')
        channel = conn.default_channel
        queue = Queue('tasks', Exchange('events'), routing_key='rk')
        plan = TopologyPlan([queue])
        with patch.object(
                channel, 'queue_bind',
                side_effect=RuntimeError('boom')):
            with pytest.raises(TopologyExecutionError) as exc_info:
                plan.execute(channel=channel)
        error = exc_info.value
        assert error.original.__class__ is RuntimeError
        assert error.operation.kind == 'queue.bind'
        assert [op.kind for op in error.completed] == [
            'exchange.declare', 'queue.declare']
        message = str(error)
        assert 'step 3' in message
        assert '2 step(s) already executed' in message
        # declarations already on the server are not rolled back
        assert 'events' in conn.transport.state.exchanges

    def test_execute_does_not_connect_when_capability_check_fails(self):
        conn = Connection(transport=LimitedTransport)
        plan = TopologyPlan([Queue('q', Exchange('e', type='topic'))])
        with pytest.raises(UnsupportedExchangeType):
            plan.execute(connection=conn)
        assert conn.connected is False
        assert conn.transport.state.exchanges == {}


class test_Operation:

    def test_operation_repr_and_str(self):
        op = Operation('queue.declare', "declare queue 'tasks'")
        assert str(op) == "declare queue 'tasks'"
        assert 'queue.declare' in repr(op)
