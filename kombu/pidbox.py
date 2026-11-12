"""Generic process mailbox."""

from __future__ import annotations

import abc
import socket
import warnings
from collections import defaultdict, deque
from contextlib import contextmanager
from copy import copy
from dataclasses import dataclass
from itertools import count
from time import time
from typing import Any

from . import Consumer, Exchange, Producer, Queue
from .clocks import LamportClock
from .common import maybe_declare, oid_from
from .exceptions import InconsistencyError, PolicyDenied
from .log import get_logger
from .matcher import match
from .utils.functional import maybe_evaluate, reprcall
from .utils.objects import cached_property
from .utils.uuid import uuid

REPLY_QUEUE_EXPIRES = 10

W_PIDBOX_IN_USE = """\
A node named {node.hostname} is already using this process mailbox!

Maybe you forgot to shutdown the other node or did not do so properly?
Or if you meant to start multiple nodes on the same host please make sure
you give each node a unique node name!
"""

W_POLICY_DENIED = 'pidbox command %r denied by policy: %s'
W_POLICY_ERROR = 'pidbox authorization policy raised; denying command %r'

__all__ = ('Node', 'Mailbox', 'AuthorizationContext', 'Policy')
logger = get_logger(__name__)
debug, error, warning = logger.debug, logger.error, logger.warning


@dataclass(frozen=True)
class AuthorizationContext:
    """Everything a receiver-side :class:`Policy` may inspect.

    The policy receives one of these for every decoded pidbox command that
    targets this node.  Fields copied from the message
    (:attr:`method`, :attr:`arguments`, :attr:`destination`, :attr:`pattern`,
    :attr:`matcher`, :attr:`reply_to`, :attr:`ticket`, :attr:`headers`,
    :attr:`body`) are attacker-controlled claims: they prove only that the
    sender could write to the mailbox exchange, never who the sender is.
    A role or username carried in :attr:`arguments` or :attr:`headers` must
    never be treated as an authenticated identity.  The only local,
    application-bound inputs are :attr:`hostname` and :attr:`state`.

    Broker authentication (validating the AMQP connection itself) remains the
    responsibility of the application and is out of scope of this interface.
    """

    #: Name of the command the message asks to execute.
    method: str

    #: Keyword arguments decoded from the message.
    arguments: dict

    #: Explicit destination hostnames requested by the sender, if any.
    destination: list | tuple | None

    #: Glob/regexp pattern used for destination selection, if any.
    pattern: str | None

    #: Matcher engine backing :attr:`pattern`, if any.
    matcher: str | None

    #: Reply destination claimed by the sender, or :const:`None` for casts.
    reply_to: dict | None

    #: Correlation ticket for the reply, if any.
    ticket: str | None

    #: AMQP headers of the received message (when available).
    headers: dict

    #: The full decoded message body, for custom claims the policy understands.
    body: dict

    #: Hostname of the node that is about to execute the command.
    hostname: str

    #: Application-bound local context passed to ``Mailbox.Node``.
    state: Any = None


class Policy(abc.ABC):
    """Receiver-side authorization policy for a pidbox :class:`Node`.

    Install a subclass on a :class:`Mailbox` (or on an individual node via
    :meth:`Mailbox.Node`) to decide which decoded commands may run.  The
    policy runs after the message is decoded and the destination check
    succeeds, but *before* any registered handler is called and before any
    reply is published, so a denial has no business side effects and never
    reflects a message back to the sender.

    The same check applies to calls, casts, broadcast (``abcast``) and
    pattern-selected messages, because every delivered command enters
    through :meth:`Node.handle_message`.
    """

    @abc.abstractmethod
    def authorize(self, context: AuthorizationContext) -> bool:
        """Return :const:`True` if the command may execute.

        Returning a false value (or raising :exc:`~kombu.exceptions.PolicyDenied`)
        denies the command.  Any other exception is also treated as a denial
        (fail-closed) and logged.
        """
        raise NotImplementedError


class Node:
    """Mailbox node."""

    #: hostname of the node.
    hostname = None

    #: the :class:`Mailbox` this is a node for.
    mailbox = None

    #: map of method name/handlers.
    handlers = None

    #: current context (passed on to handlers)
    state = None

    #: current channel.
    channel = None

    #: optional receiver-side :class:`Policy` authorizing incoming commands.
    policy = None

    def __init__(self, hostname, state=None, channel=None,
                 handlers=None, mailbox=None, policy=None):
        self.channel = channel
        self.mailbox = mailbox
        self.hostname = hostname
        self.state = state
        self.policy = policy
        self.adjust_clock = self.mailbox.clock.adjust
        if handlers is None:
            handlers = {}
        self.handlers = handlers

    def Consumer(self, channel=None, no_ack=True, accept=None, **options):
        queue = self.mailbox.get_queue(self.hostname)

        def verify_exclusive(name, messages, consumers):
            if consumers:
                warnings.warn(W_PIDBOX_IN_USE.format(node=self))
        queue.on_declared = verify_exclusive

        conn = self.mailbox.connection
        channel_errors = conn.channel_errors if conn else ()
        try:
            return Consumer(
                channel or self.channel, [queue], no_ack=no_ack,
                accept=self.mailbox.accept if accept is None else accept,
                **options
            )
        except channel_errors as exc:
            if getattr(exc, 'code', 0) == 405:
                raise InconsistencyError(
                    W_PIDBOX_IN_USE.format(node=self)
                ) from exc
            raise

    def handler(self, fun):
        self.handlers[fun.__name__] = fun
        return fun

    def on_decode_error(self, message, exc):
        error('Cannot decode message: %r', exc, exc_info=1)

    def listen(self, channel=None, callback=None):
        consumer = self.Consumer(channel=channel,
                                 callbacks=[callback or self.handle_message],
                                 on_decode_error=self.on_decode_error)
        consumer.consume()
        return consumer

    def dispatch(self, method, arguments=None,
                 reply_to=None, ticket=None, **kwargs):
        arguments = arguments or {}
        debug('pidbox received method %s [reply_to:%s ticket:%s]',
              reprcall(method, (), kwargs=arguments), reply_to, ticket)
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, arguments)
        except SystemExit:
            raise
        except Exception as exc:
            error('pidbox command error: %r', exc, exc_info=1)
            reply = {'error': repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                       exchange=reply_to['exchange'],
                       routing_key=reply_to['routing_key'],
                       ticket=ticket)
        return reply

    def handle(self, method, arguments=None):
        arguments = {} if not arguments else arguments
        return self.handlers[method](self.state, **arguments)

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def handle_message(self, body, message=None):
        destination = body.get('destination')
        pattern = body.get('pattern')
        matcher = body.get('matcher')
        if message:
            self.adjust_clock(message.headers.get('clock') or 0)
        hostname = self.hostname
        run_dispatch = False
        if destination:
            if hostname in destination:
                run_dispatch = True
        elif pattern and matcher:
            if match(hostname, pattern, matcher):
                run_dispatch = True
        else:
            run_dispatch = True
        if run_dispatch:
            if not self.authorize_message(body, message):
                return None
            return self.dispatch(**body)
    dispatch_from_message = handle_message

    def authorization_context(self, body, message=None):
        """Build the :class:`AuthorizationContext` for a decoded message."""
        headers = getattr(message, 'headers', None) or {}
        return AuthorizationContext(
            method=body.get('method'),
            arguments=body.get('arguments') or {},
            destination=body.get('destination'),
            pattern=body.get('pattern'),
            matcher=body.get('matcher'),
            reply_to=body.get('reply_to'),
            ticket=body.get('ticket'),
            headers=headers,
            body=body,
            hostname=self.hostname,
            state=self.state,
        )

    def authorize_message(self, body, message=None):
        """Run the receiver-side policy before dispatch.

        Returns :const:`True` when no policy is configured (preserving the
        historical behavior) or the configured policy allows the command.
        Denials and policy errors fail closed: the command is dropped, no
        handler runs and no reply is published.
        """
        policy = self.policy or getattr(self.mailbox, 'policy', None)
        if policy is None:
            return True
        context = self.authorization_context(body, message)
        try:
            allowed = policy.authorize(context)
        except PolicyDenied as exc:
            warning(W_POLICY_DENIED, context.method, exc)
            return False
        except Exception as exc:
            error(W_POLICY_ERROR, context.method, exc_info=1)
            return False
        if not allowed:
            warning(W_POLICY_DENIED, context.method, 'policy returned False')
            return False
        return True

    def reply(self, data, exchange, routing_key, ticket, **kwargs):
        self.mailbox._publish_reply(data, exchange, routing_key, ticket,
                                    channel=self.channel,
                                    serializer=self.mailbox.serializer)


class Mailbox:
    """Process Mailbox."""

    node_cls = Node
    exchange_fmt = '%s.pidbox'
    reply_exchange_fmt = 'reply.%s.pidbox'

    #: Name of application.
    namespace = None

    #: Connection (if bound).
    connection = None

    #: Exchange type (usually direct, or fanout for broadcast).
    type = 'direct'

    #: mailbox exchange (init by constructor).
    exchange = None

    #: exchange to send replies to.
    reply_exchange = None

    #: Only accepts json messages by default.
    accept = ['json']

    #: Message serializer
    serializer = None

    #: Optional receiver-side :class:`Policy` shared by nodes created from
    #: this mailbox.  Nodes may override it with their own policy.
    policy = None

    def __init__(self, namespace,
                 type='direct', connection=None, clock=None,
                 accept=None, serializer=None, producer_pool=None,
                 queue_ttl=None, queue_expires=None,
                 queue_durable=False, queue_exclusive=True,
                 reply_queue_ttl=None, reply_queue_expires=10.0,
                 policy=None):
        self.namespace = namespace
        self.connection = connection
        self.type = type
        self.clock = LamportClock() if clock is None else clock
        self.exchange = self._get_exchange(self.namespace, self.type)
        self.reply_exchange = self._get_reply_exchange(self.namespace)
        self.unclaimed = defaultdict(deque)
        self.accept = self.accept if accept is None else accept
        self.serializer = self.serializer if serializer is None else serializer
        self.queue_ttl = queue_ttl
        self.queue_expires = queue_expires
        self.queue_durable = queue_durable
        self.queue_exclusive = queue_exclusive
        self.reply_queue_ttl = reply_queue_ttl
        self.reply_queue_expires = reply_queue_expires
        self._producer_pool = producer_pool
        self.policy = policy
        if queue_exclusive and queue_durable:
            raise ValueError(
                "queue_exclusive and queue_durable cannot both be True "
                "(exclusive queues are automatically deleted and cannot be durable).",
            )

    def __call__(self, connection):
        bound = copy(self)
        bound.connection = connection
        return bound

    def Node(self, hostname=None, state=None, channel=None, handlers=None,
             policy=None):
        hostname = hostname or socket.gethostname()
        if policy is None:
            policy = self.policy
        return self.node_cls(hostname, state, channel, handlers,
                             mailbox=self, policy=policy)

    def call(self, destination, command, kwargs=None,
             timeout=None, callback=None, channel=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, destination,
                               reply=True, timeout=timeout,
                               callback=callback,
                               channel=channel)

    def cast(self, destination, command, kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, destination, reply=False)

    def abcast(self, command, kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, reply=False)

    def multi_call(self, command, kwargs=None, timeout=1,
                   limit=None, callback=None, channel=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, reply=True,
                               timeout=timeout, limit=limit,
                               callback=callback,
                               channel=channel)

    def get_reply_queue(self):
        oid = self.oid
        return Queue(
            f'{oid}.{self.reply_exchange.name}',
            exchange=self.reply_exchange,
            routing_key=oid,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.reply_queue_expires,
            message_ttl=self.reply_queue_ttl,
        )

    @cached_property
    def reply_queue(self):
        return self.get_reply_queue()

    def get_queue(self, hostname):
        return Queue(
            f'{hostname}.{self.namespace}.pidbox',
            exchange=self.exchange,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.queue_expires,
            message_ttl=self.queue_ttl,
        )

    @contextmanager
    def producer_or_acquire(self, producer=None, channel=None):
        if producer:
            yield producer
        elif self.producer_pool:
            with self.producer_pool.acquire() as producer:
                yield producer
        else:
            yield Producer(channel, auto_declare=False)

    def _publish_reply(self, reply, exchange, routing_key, ticket,
                       channel=None, producer=None, **opts):
        chan = channel or self.connection.default_channel
        exchange = Exchange(exchange, exchange_type='direct',
                            delivery_mode='transient',
                            durable=False)
        with self.producer_or_acquire(producer, chan) as producer:
            try:
                producer.publish(
                    reply, exchange=exchange, routing_key=routing_key,
                    declare=[exchange], headers={
                        'ticket': ticket, 'clock': self.clock.forward(),
                    }, retry=True,
                    **opts
                )
            except InconsistencyError:
                # queue probably deleted and no one is expecting a reply.
                pass

    def _publish(self, type, arguments, destination=None,
                 reply_ticket=None, channel=None, timeout=None,
                 serializer=None, producer=None, pattern=None, matcher=None):
        message = {'method': type,
                   'arguments': arguments,
                   'destination': destination,
                   'pattern': pattern,
                   'matcher': matcher}
        chan = channel or self.connection.default_channel
        exchange = self.exchange
        if reply_ticket:
            maybe_declare(self.reply_queue(chan))
            message.update(ticket=reply_ticket,
                           reply_to={'exchange': self.reply_exchange.name,
                                     'routing_key': self.oid})
        serializer = serializer or self.serializer
        with self.producer_or_acquire(producer, chan) as producer:
            producer.publish(
                message, exchange=exchange.name, declare=[exchange],
                headers={'clock': self.clock.forward(),
                         'expires': time() + timeout if timeout else 0},
                serializer=serializer, retry=True,
            )

    def _broadcast(self, command, arguments=None, destination=None,
                   reply=False, timeout=1, limit=None,
                   callback=None, channel=None, serializer=None,
                   pattern=None, matcher=None):
        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError(
                'destination must be a list/tuple not {}'.format(
                    type(destination)))
        if (pattern is not None and not isinstance(pattern, str) and
                matcher is not None and not isinstance(matcher, str)):
            raise ValueError(
                'pattern and matcher must be '
                'strings not {}, {}'.format(type(pattern), type(matcher))
            )

        arguments = arguments or {}
        reply_ticket = reply and uuid() or None
        chan = channel or self.connection.default_channel

        # Set reply limit to number of destinations (if specified)
        if limit is None and destination:
            limit = destination and len(destination) or None

        serializer = serializer or self.serializer
        self._publish(command, arguments, destination=destination,
                      reply_ticket=reply_ticket,
                      channel=chan,
                      timeout=timeout,
                      serializer=serializer,
                      pattern=pattern,
                      matcher=matcher)

        if reply_ticket:
            return self._collect(reply_ticket, limit=limit,
                                 timeout=timeout,
                                 callback=callback,
                                 channel=chan)

    def _collect(self, ticket,
                 limit=None, timeout=1, callback=None,
                 channel=None, accept=None):
        if accept is None:
            accept = self.accept
        chan = channel or self.connection.default_channel
        queue = self.reply_queue
        consumer = Consumer(chan, [queue], accept=accept, no_ack=True)
        responses = []
        unclaimed = self.unclaimed
        adjust_clock = self.clock.adjust

        try:
            return unclaimed.pop(ticket)
        except KeyError:
            pass

        def on_message(body, message):
            # ticket header added in kombu 2.5
            header = message.headers.get
            adjust_clock(header('clock') or 0)
            expires = header('expires')
            if expires and time() > expires:
                return
            this_id = header('ticket', ticket)
            if this_id == ticket:
                if callback:
                    callback(body)
                responses.append(body)
            else:
                unclaimed[this_id].append(body)

        consumer.register_callback(on_message)
        try:
            with consumer:
                for i in limit and range(limit) or count():
                    try:
                        self.connection.drain_events(timeout=timeout)
                    except socket.timeout:
                        break
                return responses
        finally:
            chan.after_reply_message_received(queue.name)

    def _get_exchange(self, namespace, type):
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        delivery_mode='transient')

    def _get_reply_exchange(self, namespace):
        return Exchange(self.reply_exchange_fmt % namespace,
                        type='direct',
                        durable=False,
                        delivery_mode='transient')

    @property
    def oid(self):
        return oid_from(self)

    @cached_property
    def producer_pool(self):
        return maybe_evaluate(self._producer_pool)
