"""Read-only topology planning for exchange/queue declarations.

This module provides :class:`TopologyPlan`, a collector that gathers
:class:`~kombu.entity.Exchange`, :class:`~kombu.entity.Queue` and
exchange-to-exchange binding declarations, deduplicates them by
declaration semantics (not only by name), performs static checks
against them -- including transport capability checks, without
connecting to the broker -- and finally produces an ordered,
executable plan.
"""

from __future__ import annotations

from .entity import Exchange, Queue
from .exceptions import (
    TopologyConflict,
    TopologyCycleError,
    TopologyExecutionError,
    UnsupportedExchangeType,
    UnsupportedTopologyFeature,
)
from .transport.virtual.base import Channel as VirtualChannel

__all__ = ('TopologyPlan', 'Operation', 'ExchangeBinding')


def _freeze(value):
    """Freeze declaration arguments into a hashable canonical form."""
    if isinstance(value, dict):
        return frozenset(
            (key, _freeze(item)) for key, item in sorted(value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _exchange_signature(exchange):
    """Semantic fingerprint of an exchange declaration."""
    return {
        'type': exchange.type,
        'durable': exchange.durable,
        'auto_delete': exchange.auto_delete,
        'passive': exchange.passive,
        'arguments': _freeze(exchange.arguments),
    }


def _queue_signature(queue):
    """Semantic fingerprint of a queue declaration."""
    return {
        'durable': queue.durable,
        'exclusive': queue.exclusive,
        'auto_delete': queue.auto_delete,
        'arguments': _freeze(queue.queue_arguments),
        'expires': queue.expires,
        'message_ttl': queue.message_ttl,
        'max_length': queue.max_length,
        'max_length_bytes': queue.max_length_bytes,
        'max_priority': queue.max_priority,
    }


def _conflicting_fields(current, incoming):
    return sorted(
        key for key in current if current[key] != incoming[key]
    )


def _find_cycle(graph):
    """Return a cycle path in *graph* or :const:`None`.

    The returned cycle path repeats the first node at the end
    (e.g. ``['a', 'b', 'a']``).
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    colors = dict.fromkeys(graph, WHITE)
    path = []

    def visit(node):
        colors[node] = GRAY
        path.append(node)
        for successor in sorted(graph[node]):
            if colors[successor] == GRAY:
                start = path.index(successor)
                return path[start:] + [successor]
            if colors[successor] == WHITE:
                cycle = visit(successor)
                if cycle is not None:
                    return cycle
        path.pop()
        colors[node] = BLACK
        return None

    for node in sorted(graph):
        if colors[node] == WHITE:
            cycle = visit(node)
            if cycle is not None:
                return cycle
    return None


def _supports_exchange_bind(transport):
    """Check whether a transport supports exchange-to-exchange bindings.

    The virtual transport base ships an ``exchange_bind`` stub that
    always raises :exc:`NotImplementedError`; transports that inherit
    that stub cannot create such bindings.
    """
    channel_cls = getattr(transport, 'Channel', None)
    method = getattr(channel_cls, 'exchange_bind', None)
    if method is None:
        return False
    return method is not VirtualChannel.exchange_bind


def _resolve_transport(transport):
    """Accept a transport, a :class:`~kombu.Connection` or :const:`None`."""
    if transport is None:
        return None
    if hasattr(transport, 'implements'):
        return transport
    if hasattr(transport, 'transport'):
        # Accessing Connection.transport instantiates the transport
        # class but does not establish a broker connection.
        return transport.transport
    raise TypeError(
        f'expected a transport or connection, got {type(transport)!r}')


class ExchangeBinding:
    """Exchange-to-exchange binding declaration.

    The *destination* exchange is bound to the *source* exchange.
    """

    def __init__(self, destination, source, routing_key='', arguments=None):
        self.destination = destination
        self.source = source
        self.routing_key = routing_key or ''
        self.arguments = arguments

    def __repr__(self):
        return f'<ExchangeBinding: {self}>'

    def __str__(self):
        return f'{self.source}->{self.destination}'


class _QueueRecord:
    """Internal record for a queue and all of its queue bindings."""

    def __init__(self, queue):
        self.queue = queue
        self.signature = _queue_signature(queue)
        self.bindings = []
        self.merge(queue)

    def merge(self, queue):
        if queue.exchange is not None and queue.exchange.name:
            self._add_binding(
                queue.exchange.name, queue.routing_key,
                queue.binding_arguments)
        for bind in queue.bindings:
            if bind.exchange is not None and bind.exchange.name:
                self._add_binding(
                    bind.exchange.name, bind.routing_key, bind.arguments)

    def _add_binding(self, exchange, routing_key, arguments):
        entry = (exchange, routing_key or '', arguments or None)
        if entry not in self.bindings:
            self.bindings.append(entry)


class Operation:
    """A single executable step in an ordered topology plan.

    Attributes:
        kind (str): One of ``'exchange.declare'``, ``'exchange.bind'``,
            ``'queue.declare'`` and ``'queue.bind'``.
        description (str): Human readable description of the step.
        entity: The :class:`~kombu.entity.Exchange` or
            :class:`~kombu.entity.Queue` involved, when applicable.
        binding: Binding payload for bind operations; an
            :class:`ExchangeBinding` for ``'exchange.bind'`` or a
            ``(exchange, routing_key, arguments)`` tuple for
            ``'queue.bind'``.
        record: The internal queue record for queue operations.
    """

    def __init__(self, kind, description, entity=None, binding=None,
                 record=None):
        self.kind = kind
        self.description = description
        self.entity = entity
        self.binding = binding
        self.record = record

    def __repr__(self):
        return f'<Operation: {self.kind}: {self.description}>'

    def __str__(self):
        return self.description


class TopologyPlan:
    """Collects declarations and produces an ordered executable plan.

    Declarations are collected with :meth:`add` (which accepts an
    :class:`~kombu.entity.Exchange` or :class:`~kombu.entity.Queue`),
    :meth:`add_exchange_binding` (for exchange-to-exchange bindings) or
    simply by passing entities to the constructor.

    Declarations that share a name but have different declaration
    semantics are reported as
    :class:`~kombu.exceptions.TopologyConflict`; semantically identical
    declarations are deduplicated.

    Static validation -- exchange-to-exchange dependency cycles and
    transport capability checks -- happens in :meth:`validate` and never
    connects to the broker.  :meth:`plan` returns the ordered list of
    operations actually required and :meth:`execute` runs them on an
    existing channel, backfilling server-generated queue names into the
    dependent bindings.
    """

    def __init__(self, entities=None):
        self._exchanges = {}
        self._exchange_signatures = {}
        self._queue_records = []
        self._queues_by_name = {}
        self._exchange_bindings = []
        for entity in entities or ():
            self.add(entity)

    # -- collection ----------------------------------------------------

    def add(self, entity):
        """Collect an :class:`Exchange` or :class:`Queue` declaration."""
        if isinstance(entity, Exchange):
            return self.add_exchange(entity)
        if isinstance(entity, Queue):
            return self.add_queue(entity)
        raise TypeError(
            f'expected Exchange or Queue instance, got {type(entity)!r}')

    def add_exchange(self, exchange):
        """Collect an exchange declaration.

        Returns the canonical exchange declaration.  Semantically
        identical re-declarations of the same name are deduplicated;
        conflicting re-declarations raise
        :class:`~kombu.exceptions.TopologyConflict`.
        """
        if not exchange._can_declare():
            return None
        name = exchange.name
        signature = _exchange_signature(exchange)
        if name in self._exchanges:
            current = self._exchange_signatures[name]
            if current != signature:
                fields = _conflicting_fields(current, signature)
                raise TopologyConflict(
                    "conflicting declarations for exchange {!r}; "
                    "differing attribute(s): {}".format(
                        name, ', '.join(fields)))
            return self._exchanges[name]
        self._exchanges[name] = exchange
        self._exchange_signatures[name] = signature
        return exchange

    def add_queue(self, queue):
        """Collect a queue declaration.

        Same-name queues must share their declaration semantics;
        otherwise :class:`~kombu.exceptions.TopologyConflict` is raised.
        Compatible re-declarations merge their queue bindings.  Queues
        with an empty (server-generated) name are never deduplicated and
        are never keyed by their empty name.
        """
        if queue.no_declare:
            return None
        if queue.exchange is not None:
            self.add_exchange(queue.exchange)
        for bind in queue.bindings:
            if bind.exchange is not None:
                self.add_exchange(bind.exchange)
        signature = _queue_signature(queue)
        if queue.name:
            record = self._queues_by_name.get(queue.name)
            if record is not None:
                if record.signature != signature:
                    fields = _conflicting_fields(
                        record.signature, signature)
                    raise TopologyConflict(
                        "conflicting declarations for queue {!r}; "
                        "differing attribute(s): {}".format(
                            queue.name, ', '.join(fields)))
                record.merge(queue)
                return record
            record = _QueueRecord(queue)
            self._queues_by_name[queue.name] = record
            self._queue_records.append(record)
            return record
        # Server-named queue: every anonymous queue is a distinct
        # declaration, but adding the same object twice must still
        # deduplicate (object identity, never the empty name).
        for record in self._queue_records:
            if record.queue is queue:
                return record
        record = _QueueRecord(queue)
        self._queue_records.append(record)
        return record

    def add_exchange_binding(self, destination, source,
                             routing_key='', arguments=None):
        """Collect an exchange-to-exchange binding.

        *destination* is bound to *source*.  Endpoints may be
        :class:`Exchange` instances (which are also collected for
        declaration) or plain exchange names (assumed to already exist
        on the broker).
        """
        destination = self._binding_endpoint(destination)
        source = self._binding_endpoint(source)
        binding = ExchangeBinding(
            destination, source, routing_key, arguments or None)
        self._exchange_bindings.append(binding)
        return binding

    def _binding_endpoint(self, endpoint):
        if isinstance(endpoint, Exchange):
            self.add_exchange(endpoint)
            return endpoint.name
        return endpoint or ''

    @property
    def exchanges(self):
        """Collected exchanges in insertion order."""
        return list(self._exchanges.values())

    @property
    def queues(self):
        """Collected queues in insertion order."""
        return [record.queue for record in self._queue_records]

    @property
    def exchange_bindings(self):
        """Collected exchange-to-exchange bindings in insertion order."""
        return list(self._exchange_bindings)

    # -- static validation --------------------------------------------

    def validate(self, transport=None):
        """Run static checks without connecting to the broker.

        *transport* may be a transport instance or a
        :class:`~kombu.Connection`.  When :const:`None`, transport
        capability checks are skipped.  Returns :const:`True` or raises.
        """
        transport = _resolve_transport(transport)
        self._check_exchange_binding_cycles()
        if transport is not None:
            self._check_transport_capabilities(transport)
        return True

    def _check_exchange_binding_cycles(self):
        cycle = _find_cycle(self._exchange_dependency_graph())
        if cycle is not None:
            raise TopologyCycleError(
                'exchange-to-exchange bindings form a cycle: {}'.format(
                    ' -> '.join(repr(node) for node in cycle)))

    def _check_transport_capabilities(self, transport):
        supported_types = set(transport.implements.get('exchange_type') or ())
        for name, exchange in self._exchanges.items():
            if supported_types and exchange.type not in supported_types:
                raise UnsupportedExchangeType(
                    "transport {!r} does not support exchange type {!r} "
                    "required by exchange {!r}".format(
                        transport, exchange.type, name))
        if self._exchange_bindings and not _supports_exchange_bind(transport):
            raise UnsupportedTopologyFeature(
                'transport {!r} does not support exchange-to-exchange '
                'bindings'.format(transport))

    def _exchange_dependency_graph(self):
        # Edge source -> destination: declaring the binding depends on
        # both exchanges already existing.
        graph = {name: set() for name in self._exchanges}
        for binding in self._exchange_bindings:
            if (binding.source in graph
                    and binding.destination in graph
                    and binding.source != binding.destination):
                graph[binding.source].add(binding.destination)
        return graph

    def _exchange_declaration_order(self):
        """Topologically sort exchanges (binding sources declared first)."""
        graph = self._exchange_dependency_graph()
        indegree = {name: 0 for name in graph}
        for successors in graph.values():
            for successor in successors:
                indegree[successor] += 1
        ready = [name for name in self._exchanges if indegree[name] == 0]
        order = []
        while ready:
            node = ready.pop(0)
            order.append(node)
            for successor in sorted(graph[node]):
                indegree[successor] -= 1
                if indegree[successor] == 0:
                    ready.append(successor)
        return order

    # -- plan generation -----------------------------------------------

    def plan(self, transport=None):
        """Validate and return the ordered list of :class:`Operation`.

        The order is: exchange declarations (binding sources first),
        exchange-to-exchange bindings and, for every queue, its
        declaration followed by its queue bindings.
        """
        self.validate(transport)
        operations = []
        for name in self._exchange_declaration_order():
            exchange = self._exchanges[name]
            operations.append(Operation(
                'exchange.declare',
                "declare exchange {!r} (type={!r}, durable={!r})".format(
                    name, exchange.type, exchange.durable),
                entity=exchange,
            ))
        for binding in self._exchange_bindings:
            operations.append(Operation(
                'exchange.bind',
                "bind exchange {!r} to source {!r} (routing_key={!r})"
                .format(binding.destination, binding.source,
                        binding.routing_key),
                binding=binding,
            ))
        for record in self._queue_records:
            queue = record.queue
            label = queue.name or queue.alias or '<server-named>'
            operations.append(Operation(
                'queue.declare',
                "declare queue {!r}".format(label),
                entity=queue, record=record,
            ))
            for exchange_name, routing_key, arguments in record.bindings:
                operations.append(Operation(
                    'queue.bind',
                    "bind queue {!r} to exchange {!r} "
                    "(routing_key={!r})".format(
                        label, exchange_name, routing_key),
                    entity=queue, record=record,
                    binding=(exchange_name, routing_key, arguments),
                ))
        return operations

    # -- execution -----------------------------------------------------

    def execute(self, channel=None, connection=None, nowait=False):
        """Execute the plan using an existing channel.

        When only a connection is given, its ``default_channel`` is
        used, following the usual channel lifecycle.  Transport
        capabilities are re-validated against the channel's transport
        before any declaration is sent.

        Server-generated queue names returned by the broker are
        backfilled onto the queue entities, so the subsequent queue
        bindings use the real name; the empty name is never used as a
        declaration cache key.

        Steps already completed when a later step fails are **not**
        rolled back; :class:`~kombu.exceptions.TopologyExecutionError`
        reports the failed step and the completed steps.

        Returns the list of operations actually executed (excluding
        declarations already present in the channel's declaration
        cache).
        """
        if channel is None:
            if connection is None:
                raise TypeError(
                    'execute() requires a channel or a connection')
            # Static preflight before opening a channel, so an
            # unsupported plan never connects to the broker.
            self.validate(connection)
            channel = connection.default_channel
        client = getattr(getattr(channel, 'connection', None),
                         'client', None)
        transport = client.transport if client is not None else None
        operations = self.plan(transport)
        cache = getattr(client, 'declared_entities', None)

        completed = []
        skipped_records = set()
        for operation in operations:
            try:
                executed = self._execute_operation(
                    operation, channel, nowait, cache, skipped_records)
            except Exception as exc:
                raise TopologyExecutionError(
                    operation, completed, exc) from exc
            if executed:
                completed.append(operation)
        return completed

    def _execute_operation(self, operation, channel, nowait, cache,
                           skipped_records):
        kind = operation.kind
        if kind == 'exchange.declare':
            exchange = operation.entity
            ident = hash(exchange) if exchange.can_cache_declaration else None
            if ident is not None and cache is not None and ident in cache:
                return False
            exchange.declare(nowait=nowait, channel=channel)
            if ident is not None and cache is not None:
                cache.add(ident)
            return True
        if kind == 'exchange.bind':
            binding = operation.binding
            channel.exchange_bind(
                destination=binding.destination, source=binding.source,
                routing_key=binding.routing_key,
                arguments=binding.arguments, nowait=nowait)
            return True
        if kind == 'queue.declare':
            queue = operation.entity
            record = operation.record
            if queue.name:
                ident = (
                    hash(queue) if queue.can_cache_declaration else None)
                if ident is not None and cache is not None \
                        and ident in cache:
                    skipped_records.add(id(record))
                    return False
            # A server-named queue has no stable identity until the
            # broker answers, so it must never be looked up or cached
            # under its empty name.
            queue.queue_declare(nowait=nowait, passive=False,
                                channel=channel)
            # queue_declare backfills the server-generated name on the
            # entity; only now is it a valid cache key, and the queued
            # queue.bind steps below use the real name.
            if queue.name and cache is not None \
                    and queue.can_cache_declaration:
                cache.add(hash(queue))
            return True
        if kind == 'queue.bind':
            if id(operation.record) in skipped_records:
                return False
            queue = operation.entity
            exchange_name, routing_key, arguments = operation.binding
            queue.bind_to(exchange_name, routing_key, arguments,
                          nowait=nowait, channel=channel)
            return True
        raise ValueError(f'unknown operation kind: {kind!r}')
