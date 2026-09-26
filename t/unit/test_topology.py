from __future__ import annotations

import pytest

from kombu import Connection, Exchange, Queue, binding
from kombu.entity import Exchange as ExchangeEntity
from kombu.exceptions import (TopologyCapabilityError, TopologyConflictError,
                              TopologyCycleError, TopologyExecutionError)
from kombu.topology import ExchangeBinding, TopologyPlan
from kombu.transport import memory
from kombu.transport.base import default_transport_capabilities
from t.mocks import Transport


@pytest.fixture(autouse=True)
def _default_memory_capabilities():
    # t/unit/test_connection.py mutates the shared
    # base.Transport.implements mapping in place; make sure the memory
    # transport exposes its documented capabilities here.
    implements = memory.Transport.implements
    previous = implements.exchange_type
    implements.exchange_type = (
        default_transport_capabilities['exchange_type'])
    yield
    implements.exchange_type = previous


class DirectOnlyTransport(Transport):
    implements = Transport.implements.extend(
        exchange_type=frozenset(['direct']),
    )


def get_memory_channel():
    memory.Transport.global_state.clear()
    memory.Channel.queues.clear()
    return Connection('memory://').channel()


class test_ExchangeBinding:

    def test_repr(self):
        x = ExchangeBinding(Exchange('dest'), Exchange('src'), 'rk')
        assert 'src->dest' in str(x)
        assert 'src->dest' in repr(x)

    def test_accepts_plain_names(self):
        x = ExchangeBinding('dest', 'src')
        assert x.destination == 'dest'
        assert x.source == 'src'


class test_declaration_dedup:

    def test_identical_exchanges_deduped(self):
        plan = TopologyPlan([
            Exchange('e', 'topic', durable=True),
            Exchange('e', 'topic', durable=True),
        ])
        steps = plan.compile()
        assert [s.kind for s in steps] == ['exchange']

    def test_exchange_same_name_different_type_conflicts(self):
        with pytest.raises(TopologyConflictError, match="exchange 'e'"):
            TopologyPlan([
                Exchange('e', 'topic'),
                Exchange('e', 'direct'),
            ])

    def test_exchange_same_name_different_durable_conflicts(self):
        with pytest.raises(TopologyConflictError, match="exchange 'e'"):
            TopologyPlan([
                Exchange('e', 'topic', durable=True),
                Exchange('e', 'topic', durable=False),
            ])

    def test_exchange_same_name_different_arguments_conflicts(self):
        with pytest.raises(TopologyConflictError, match="exchange 'e'"):
            TopologyPlan([
                Exchange('e', 'topic', arguments={'a': 1}),
                Exchange('e', 'topic', arguments={'a': 2}),
            ])

    def test_queue_same_name_different_arguments_conflicts(self):
        with pytest.raises(TopologyConflictError, match="queue 'q'"):
            TopologyPlan([
                Queue('q', queue_arguments={'x-max-length': 1}),
                Queue('q', queue_arguments={'x-max-length': 2}),
            ])

    def test_queue_same_name_different_durable_conflicts(self):
        with pytest.raises(TopologyConflictError, match="queue 'q'"):
            TopologyPlan([
                Queue('q', durable=True),
                Queue('q', durable=False),
            ])

    def test_identical_queues_deduped_and_bindings_merged(self):
        exchange = Exchange('e', 'direct')
        plan = TopologyPlan([
            Queue('q', exchange, 'rk1'),
            Queue('q', exchange, 'rk2'),
        ])
        steps = plan.compile()
        assert [s.kind for s in steps].count('queue') == 1
        bound_keys = {
            s.routing_key for s in steps if s.kind == 'queue_bind'
        }
        assert bound_keys == {'rk1', 'rk2'}

    def test_queue_conflict_via_embedded_exchange(self):
        with pytest.raises(TopologyConflictError, match="exchange 'e'"):
            TopologyPlan([
                Queue('q1', Exchange('e', 'topic')),
                Queue('q2', Exchange('e', 'direct')),
            ])

    def test_anonymous_queues_never_deduped(self):
        plan = TopologyPlan([Queue(), Queue()])
        steps = plan.compile()
        assert [s.kind for s in steps].count('queue') == 2

    def test_no_declare_entities_skipped(self):
        plan = TopologyPlan([
            Exchange('e', 'topic', no_declare=True),
            Queue('q', no_declare=True),
        ])
        assert plan.compile() == []

    def test_default_exchange_not_declared(self):
        plan = TopologyPlan([Queue('q', Exchange(''), 'rk')])
        assert [s.kind for s in plan.compile()] == ['queue']

    def test_add_rejects_unknown_entity(self):
        with pytest.raises(TypeError):
            TopologyPlan([object()])


class test_exchange_binding_dependencies:

    def test_source_declared_before_destination(self):
        plan = TopologyPlan([
            ExchangeBinding(Exchange('dest', 'direct'),
                            Exchange('src', 'direct')),
        ])
        declared = [
            s.exchange.name for s in plan.compile() if s.kind == 'exchange'
        ]
        assert declared.index('src') < declared.index('dest')

    def test_transitive_dependency_order(self):
        plan = TopologyPlan([
            ExchangeBinding(Exchange('c', 'direct'), Exchange('b', 'direct')),
            ExchangeBinding(Exchange('b', 'direct'), Exchange('a', 'direct')),
        ])
        declared = [
            s.exchange.name for s in plan.compile() if s.kind == 'exchange'
        ]
        assert declared == ['a', 'b', 'c']

    def test_cycle_detected(self):
        plan = TopologyPlan([
            ExchangeBinding(Exchange('a', 'direct'), Exchange('b', 'direct')),
            ExchangeBinding(Exchange('b', 'direct'), Exchange('a', 'direct')),
        ])
        with pytest.raises(TopologyCycleError, match='a.*b|cyclic'):
            plan.compile()

    def test_self_binding_is_a_cycle(self):
        plan = TopologyPlan([
            ExchangeBinding(Exchange('a', 'direct'), Exchange('a', 'direct')),
        ])
        with pytest.raises(TopologyCycleError):
            plan.compile()

    def test_binding_to_external_exchange_needs_no_declare(self):
        plan = TopologyPlan([
            ExchangeBinding(Exchange('dest', 'direct'), 'amq.topic'),
        ])
        steps = plan.compile()
        assert [s.kind for s in steps] == ['exchange', 'exchange_bind']


class test_capability_checks:

    def test_unsupported_exchange_type_rejected_statically(self):
        plan = TopologyPlan(
            [Queue('q', Exchange('e', 'topic'), 'rk')],
            transport=DirectOnlyTransport,
        )
        with pytest.raises(TopologyCapabilityError, match='topic'):
            plan.compile()

    def test_supported_exchange_type_accepted(self):
        plan = TopologyPlan(
            [Queue('q', Exchange('e', 'direct'), 'rk')],
            transport=DirectOnlyTransport,
        )
        assert plan.compile()

    def test_memory_transport_supports_standard_types(self):
        plan = TopologyPlan(
            [Queue('q', Exchange('e', 'fanout'), 'rk')],
            transport=memory.Transport,
        )
        assert plan.compile()

    def test_no_transport_skips_capability_checks(self):
        plan = TopologyPlan([Exchange('e', 'x-custom-type')])
        assert plan.compile()


class test_compile_is_read_only:

    def test_compile_does_not_declare_on_memory_transport(self):
        channel = get_memory_channel()
        plan = TopologyPlan([
            Queue('q', Exchange('e', 'direct'), 'rk'),
            Queue(),
        ], transport=memory.Transport)
        steps = plan.compile()
        assert steps
        assert not channel.state.exchanges
        assert not dict(channel.state.bindings)
        assert not channel.queues

    def test_failed_preflight_has_no_side_effects(self):
        conn = Connection(transport=DirectOnlyTransport)
        channel = conn.channel()
        plan = TopologyPlan(
            [Queue('q', Exchange('e', 'topic'), 'rk')],
            transport=DirectOnlyTransport,
        )
        with pytest.raises(TopologyCapabilityError):
            plan.compile()
        assert 'exchange_declare' not in channel
        assert 'queue_declare' not in channel
        assert 'queue_bind' not in channel


class test_step_ordering:

    def test_declaration_order(self):
        plan = TopologyPlan([
            Queue('q1', Exchange('e1', 'direct'), 'rk'),
            Queue('q2', bindings=[binding(Exchange('e2', 'direct'), 'rk2')]),
            ExchangeBinding(Exchange('e4', 'direct'),
                            Exchange('e3', 'direct')),
        ])
        kinds = [s.kind for s in plan.compile()]
        assert kinds == [
            'exchange', 'exchange', 'exchange', 'exchange',
            'queue', 'queue',
            'queue_bind', 'queue_bind',
            'exchange_bind',
        ]

    def test_steps_describe_themselves(self):
        plan = TopologyPlan([
            Queue('q', Exchange('e', 'direct'), 'rk'),
            Queue(),
        ])
        descriptions = [s.describe() for s in plan.compile()]
        assert "declare exchange 'e' (type=direct)" in descriptions
        assert "declare queue 'q'" in descriptions
        assert 'declare server-named queue' in descriptions
        assert (
            "bind queue 'q' to exchange 'e' (routing_key='rk')"
        ) in descriptions


class test_execute:

    def test_execute_declares_in_order(self):
        channel = get_memory_channel()
        queue = Queue('q', Exchange('e', 'direct'), 'rk')
        plan = TopologyPlan([queue], transport=memory.Transport)
        assert plan.execute(channel) is plan
        assert 'e' in channel.state.exchanges
        assert channel.state.has_binding('q', 'e', 'rk')

    def test_execute_acquires_channel_from_connection(self):
        get_memory_channel()
        conn = Connection('memory://')
        queue = Queue('q', Exchange('e', 'direct'), 'rk')
        TopologyPlan([queue]).execute(connection=conn)
        assert conn.channel().state.has_binding('q', 'e', 'rk')

    def test_execute_requires_channel_or_connection(self):
        with pytest.raises(TypeError):
            TopologyPlan([Queue('q')]).execute()

    def test_server_generated_queue_name_backfilled_into_bindings(self):
        channel = get_memory_channel()
        queue = Queue(exchange=Exchange('e', 'direct'), routing_key='rk')
        assert queue.name == ''
        TopologyPlan([queue], transport=memory.Transport).execute(channel)
        assert queue.name
        assert queue.name in channel.queues
        assert channel.state.has_binding(queue.name, 'e', 'rk')

    def test_failure_reports_step_and_keeps_prior_declarations(self):
        channel = get_memory_channel()
        plan = TopologyPlan([
            ExchangeBinding(Exchange('dest', 'direct'),
                            Exchange('src', 'direct')),
        ])
        # memory transport does not implement exchange_bind.
        with pytest.raises(TopologyExecutionError) as excinfo:
            plan.execute(channel)
        err = excinfo.value
        assert err.index == 2
        assert err.total == 3
        assert err.step.kind == 'exchange_bind'
        assert isinstance(err.original, NotImplementedError)
        assert 'step 3/3' in str(err)
        assert "bind exchange 'dest' to exchange 'src'" in str(err)
        # no rollback: exchanges declared by earlier steps remain.
        assert 'dest' in channel.state.exchanges
        assert 'src' in channel.state.exchanges

    def test_exchange_binding_execution(self):
        conn = Connection(transport=Transport)
        channel = conn.channel()
        plan = TopologyPlan([
            ExchangeBinding(Exchange('dest', 'direct'),
                            Exchange('src', 'direct')),
        ])
        plan.execute(channel)
        assert 'exchange_bind' in channel

    def test_entities_are_not_bound_to_channel_by_planning(self):
        exchange = Exchange('e', 'direct')
        queue = Queue('q', exchange, 'rk')
        TopologyPlan([queue]).compile()
        assert not exchange.is_bound
        assert not queue.is_bound
        assert isinstance(queue.exchange, ExchangeEntity)
