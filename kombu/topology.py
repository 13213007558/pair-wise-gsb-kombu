"""Read-only topology planning for groups of declarations.

This module lets you collect a set of related Exchange, Queue and
exchange-to-exchange binding declarations, validate them statically
(without connecting to a broker), and produce an ordered, executable
plan.

Planning is read-only: building a plan never touches a channel, so no
declaration side effects can leak to the broker before you explicitly
call TopologyPlan.execute().

Execution follows the normal channel lifecycle: you pass an open
channel (or a connection to get one from), and declarations are issued
in dependency order.  If a step fails, TopologyExecutionError reports
exactly which step failed.  Entities already declared by earlier steps
are left as-is: no cross-broker rollback is attempted, and objects
that existed on the server before execution are never modified or
removed.
"""

from __future__ import annotations

from collections import OrderedDict
from itertools import chain

from .entity import Exchange, Queue
from .exceptions import (TopologyCapabilityError, TopologyConflictError,
                         TopologyCycleError, TopologyExecutionError)

__all__ = ('ExchangeBinding', 'TopologyPlan')


def _freeze(value):
    """Convert value into a hashable form suitable for dedup keys."""
    if isinstance(value, dict):
        return tuple(sorted(
            (key, _freeze(item)) for key, item in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _exchange_declaration(exchange):
    """The declaration semantics of an exchange."""
    return (
        exchange.type,
        bool(exchange.durable),
        bool(exchange.auto_delete),
        exchange.arguments or {},
    )


def _queue_declaration(queue):
    """The declaration semantics of a queue."""
    return (
        bool(queue.durable),
        bool(queue.exclusive),
        bool(queue.auto_delete),
        queue.queue_arguments or {},
        queue.expires,
        queue.message_ttl,
        queue.max_length,
        queue.max_length_bytes,
        queue.max_priority,
    )


def _entity_name(entity):
    return entity.name if isinstance(entity, Exchange) else entity


def _transport_name(transport):
    return getattr(transport, '__name__', None) or type(transport).__name__


class ExchangeBinding:
    """Exchange-to-exchange binding declaration.

    Binds the destination exchange to the source exchange so that
    messages published to the source are also routed to the
    destination.

    Arguments:
    ---------
        destination (Exchange, str): Exchange (or name) receiving messages.
        source (Exchange, str): Exchange (or name) messages flow from.
        routing_key (str): Routing key used as binding key.
        arguments (Dict): Arguments for the bind operation.
    """

    def __init__(self, destination, source, routing_key='', arguments=None):
        self.destination = destination
        self.source = source
        self.routing_key = routing_key
        self.arguments = arguments

    def __repr__(self):
        return '<ExchangeBinding: {}>'.format(self)

    def __str__(self):
        return '{}->{}'.format(
            _entity_name(self.source), _entity_name(self.destination),
        )


class _Step:
    """A single executable declaration step in a compiled plan."""

    kind = ''

    def describe(self):
        raise NotImplementedError()

    def execute(self, channel, nowait=False):
        raise NotImplementedError()

    def __repr__(self):
        return f'<step: {self.describe()}>'


class _DeclareExchange(_Step):
    kind = 'exchange'

    def __init__(self, exchange):
        self.exchange = exchange

    def describe(self):
        return 'declare exchange {!r} (type={})'.format(
            self.exchange.name, self.exchange.type)

    def execute(self, channel, nowait=False):
        self.exchange.declare(nowait=nowait, channel=channel)


class _DeclareQueue(_Step):
    kind = 'queue'

    def __init__(self, queue):
        self.queue = queue

    def describe(self):
        if self.queue.name:
            return f'declare queue {self.queue.name!r}'
        return 'declare server-named queue'

    def execute(self, channel, nowait=False):
        # Queue.queue_declare() backfills queue.name with the
        # server-generated name, so later binding steps referring to
        # this queue object see the real name.
        self.queue.queue_declare(nowait=nowait, passive=False,
                                 channel=channel)


class _BindQueue(_Step):
    kind = 'queue_bind'

    def __init__(self, queue, exchange, routing_key, arguments):
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.arguments = arguments

    def describe(self):
        return 'bind queue {!r} to exchange {!r} (routing_key={!r})'.format(
            self.queue.name or '<server-named>',
            _entity_name(self.exchange), self.routing_key)

    def execute(self, channel, nowait=False):
        # queue.name is resolved lazily so that server-generated names
        # backfilled by _DeclareQueue are used here.
        self.queue.bind_to(self.exchange, self.routing_key,
                           self.arguments, nowait=nowait, channel=channel)


class _BindExchange(_Step):
    kind = 'exchange_bind'

    def __init__(self, binding):
        self.binding = binding

    def describe(self):
        return 'bind exchange {!r} to exchange {!r} (routing_key={!r})'.format(
            _entity_name(self.binding.destination),
            _entity_name(self.binding.source),
            self.binding.routing_key)

    def execute(self, channel, nowait=False):
        channel.exchange_bind(
            destination=_entity_name(self.binding.destination),
            source=_entity_name(self.binding.source),
            routing_key=self.binding.routing_key,
            arguments=self.binding.arguments,
            nowait=nowait,
        )


class TopologyPlan:
    """Collect declarations and compile them into an executable plan.

    Arguments:
    ---------
        entities (Iterable): Any of Exchange, Queue or ExchangeBinding.
        transport: Transport class or instance used for static
            capability checks (e.g. supported exchange types).
            The transport is only inspected, never connected.

    Declarations are deduplicated by semantics, not just by name:
    two exchanges sharing a name must agree on type/durability/
    arguments, and two queues sharing a name must agree on their
    declaration arguments, otherwise TopologyConflictError is raised.
    Server-named (anonymous) queues are never deduplicated and never
    used as cache keys.
    """

    def __init__(self, entities=(), transport=None):
        self.transport = transport
        self._exchanges = OrderedDict()
        self._queues = OrderedDict()
        self._anonymous_queues = []
        self._queue_bindings = []
        self._queue_binding_keys = set()
        self._exchange_bindings = []
        for entity in entities:
            self.add(entity)

    def add(self, entity):
        """Add an Exchange, Queue or ExchangeBinding to the plan."""
        if isinstance(entity, Queue):
            return self._add_queue(entity)
        if isinstance(entity, Exchange):
            return self._add_exchange(entity)
        if isinstance(entity, ExchangeBinding):
            return self._add_exchange_binding(entity)
        raise TypeError(
            f'cannot add {type(entity).__name__!r} to a topology plan')

    @property
    def queues(self):
        """All queue declarations in the plan, in insertion order."""
        return list(chain(self._queues.values(), self._anonymous_queues))

    @property
    def exchanges(self):
        """All exchange declarations in the plan, in insertion order."""
        return list(self._exchanges.values())

    def _add_exchange(self, exchange):
        if not exchange._can_declare():
            return exchange
        existing = self._exchanges.get(exchange.name)
        if existing is not None:
            if (_exchange_declaration(existing) !=
                    _exchange_declaration(exchange)):
                raise TopologyConflictError(
                    'conflicting declarations for exchange {!r}: '
                    '{!r} != {!r}'.format(
                        exchange.name,
                        _exchange_declaration(existing),
                        _exchange_declaration(exchange)))
            return existing
        self._exchanges[exchange.name] = exchange
        return exchange

    def _add_queue(self, queue):
        if queue.no_declare:
            return queue
        exchange = queue.exchange
        if exchange is not None:
            exchange = self._add_exchange(exchange)
        canonical = queue
        if queue.name:
            existing = self._queues.get(queue.name)
            if existing is not None:
                if _queue_declaration(existing) != _queue_declaration(queue):
                    raise TopologyConflictError(
                        'conflicting declarations for queue {!r}: '
                        '{!r} != {!r}'.format(
                            queue.name,
                            _queue_declaration(existing),
                            _queue_declaration(queue)))
                canonical = existing
            else:
                self._queues[queue.name] = queue
        else:
            # Server-named queues get their name only after declare, so
            # they can never be deduplicated or used as a cache key.
            self._anonymous_queues.append(queue)
        if exchange is not None and exchange.name:
            self._add_queue_binding(
                canonical, exchange,
                queue.routing_key, queue.binding_arguments)
        for bind in queue.bindings:
            bind_exchange = self._add_exchange(bind.exchange)
            self._add_queue_binding(
                canonical, bind_exchange, bind.routing_key, bind.arguments)
        return canonical

    def _add_queue_binding(self, queue, exchange, routing_key, arguments):
        key = (id(queue), _entity_name(exchange),
               routing_key, _freeze(arguments))
        if key not in self._queue_binding_keys:
            self._queue_binding_keys.add(key)
            self._queue_bindings.append(
                (queue, exchange, routing_key, arguments))

    def _add_exchange_binding(self, exchange_binding):
        destination = exchange_binding.destination
        if isinstance(destination, Exchange):
            destination = self._add_exchange(destination)
        source = exchange_binding.source
        if isinstance(source, Exchange):
            source = self._add_exchange(source)
        self._exchange_bindings.append(ExchangeBinding(
            destination, source,
            routing_key=exchange_binding.routing_key,
            arguments=exchange_binding.arguments,
        ))
        return exchange_binding

    def _check_capabilities(self):
        implements = getattr(self.transport, 'implements', None)
        if implements is None:
            return
        supported = implements.get('exchange_type')
        if supported is None:
            return
        for exchange in self._exchanges.values():
            if exchange.type not in supported:
                raise TopologyCapabilityError(
                    'transport {} does not support exchange type {!r} '
                    'required by exchange {!r} (supported: {})'.format(
                        _transport_name(self.transport),
                        exchange.type, exchange.name,
                        ', '.join(sorted(supported))))

    def _exchange_declaration_order(self):
        dependencies = {name: set() for name in self._exchanges}
        for exchange_binding in self._exchange_bindings:
            destination = _entity_name(exchange_binding.destination)
            source = _entity_name(exchange_binding.source)
            if destination in dependencies and source in dependencies:
                dependencies[destination].add(source)
        order, ready = [], [
            name for name in self._exchanges if not dependencies[name]
        ]
        while ready:
            name = ready.pop(0)
            order.append(name)
            for other in self._exchanges:
                if name in dependencies[other]:
                    dependencies[other].discard(name)
                    if not dependencies[other]:
                        ready.append(other)
        remaining = [name for name in self._exchanges if dependencies[name]]
        if remaining:
            raise TopologyCycleError(
                'cyclic exchange-to-exchange bindings between: {}'.format(
                    ', '.join(sorted(remaining))))
        return order

    def compile(self):
        """Compile the plan into an ordered list of executable steps.

        This performs all static checks (declaration conflicts were
        checked when entities were added; transport capabilities and
        exchange binding cycles are checked here) without connecting
        to a broker and without producing any declaration side effects.

        Returns:
        -------
            list: Steps in the order they must execute.  Each step has
                a kind attribute, a describe() method and an
                execute(channel, nowait=False) method.
        """
        self._check_capabilities()
        steps = [
            _DeclareExchange(self._exchanges[name])
            for name in self._exchange_declaration_order()
        ]
        steps.extend(_DeclareQueue(queue) for queue in self.queues)
        steps.extend(
            _BindQueue(queue, exchange, routing_key, arguments)
            for queue, exchange, routing_key, arguments
            in self._queue_bindings
        )
        steps.extend(
            _BindExchange(exchange_binding)
            for exchange_binding in self._exchange_bindings
        )
        return steps

    def execute(self, channel=None, connection=None, nowait=False):
        """Execute the compiled plan on a channel.

        Arguments:
        ---------
            channel: Channel to declare on.  If omitted, a channel is
                acquired from the connection argument.  The channel
                lifecycle follows the usual kombu rules: the caller
                owns the channel and is responsible for closing it.
            connection (kombu.Connection): Used to acquire a channel
                when no channel is given.
            nowait (bool): Do not wait for server replies.

        Raises:
        ------
            TopologyExecutionError: If a step fails.  The error reports
                which step failed; entities declared by earlier steps
                are not rolled back.
        """
        if channel is None:
            if connection is None:
                raise TypeError(
                    'execute() requires a channel or a connection')
            channel = connection.channel()
        steps = self.compile()
        for index, step in enumerate(steps):
            try:
                step.execute(channel, nowait=nowait)
            except Exception as exc:
                raise TopologyExecutionError(
                    step, index, len(steps), exc) from exc
        return self
