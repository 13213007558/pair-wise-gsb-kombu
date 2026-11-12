=========================================
 Pidbox - ``kombu.pidbox``
=========================================


.. currentmodule:: kombu.pidbox

.. automodule:: kombu.pidbox

    .. contents::
        :local:

    Introduction
    ------------

    Creating the applications Mailbox
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    .. code-block:: python

        >>> mailbox = pidbox.Mailbox('celerybeat', type='direct')

        >>> @mailbox.handler
        >>> def reload_schedule(state, **kwargs):
        ...     state['beat'].reload_schedule()

        >>> @mailbox.handler
        >>> def connection_info(state, **kwargs):
        ...     return {'connection': state['connection'].info()}

    Example Node
    ~~~~~~~~~~~~

    .. code-block:: python

        >>> connection = kombu.Connection()
        >>> state = {'beat': beat,
                     'connection': connection}
        >>> consumer = mailbox(connection).Node(hostname).listen()
        >>> try:
        ...     while True:
        ...         connection.drain_events(timeout=1)
        ... finally:
        ...     consumer.cancel()

    Example Client
    ~~~~~~~~~~~~~~

    .. code-block:: python

        >>> mailbox.cast('reload_schedule')   # cast is async.
        >>> info = celerybeat.call('connection_info', timeout=1)

    Mailbox
    -------

    .. autoclass:: Mailbox

        .. autoattribute:: namespace
        .. autoattribute:: connection
        .. autoattribute:: type
        .. autoattribute:: exchange
        .. autoattribute:: reply_exchange

        .. automethod:: Node
        .. automethod:: call
        .. automethod:: cast
        .. automethod:: abcast
        .. automethod:: multi_call
        .. automethod:: get_reply_queue
        .. automethod:: get_queue

    Mailbox Options
    ~~~~~~~~~~~~~~~

    .. versionadded:: 5.6.0

    The `Mailbox` supports several configuration options that affect
    the behavior of its queues.

    - ``queue_durable``: If True, declares durable queues that survive broker restarts.
      Defaults to False.
    - ``queue_exclusive``: If True, declares exclusive queues (usable by only
      one connection, auto-deleted on disconnect). Defaults to True.

    .. versionchanged:: 5.7.0
       ``queue_exclusive`` now defaults to ``True`` for RabbitMQ 4.3.0
       compatibility. RabbitMQ 4.3.0 rejects transient non-exclusive queues
       by default.

    Receiver-side authorization
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    .. versionadded:: 5.8.0

    A decodable pidbox message is not, by itself, proof that its sender is
    allowed to run a handler.  An optional :class:`Policy` can be installed
    on a :class:`Mailbox` (and inherited by every node it creates) or passed
    directly to :meth:`Mailbox.Node` to override it:

    .. code-block:: python

        >>> from kombu.pidbox import AuthorizationContext, Policy

        >>> class ReadOnlyOpsPolicy(Policy):
        ...     def authorize(self, context: AuthorizationContext) -> bool:
        ...         if context.state.get('role') != 'ops-readonly':
        ...             return False
        ...         return context.method in {'ping', 'stats'}

        >>> mailbox = pidbox.Mailbox(
        ...     'celerybeat', type='direct', policy=ReadOnlyOpsPolicy(),
        ... )

    The policy receives an :class:`AuthorizationContext` carrying the
    command name, arguments, destination selection (``destination``,
    ``pattern``/``matcher``), the claimed ``reply_to`` and ticket, the raw
    headers and body, plus the node's hostname and locally bound ``state``.
    It runs after decoding and destination matching, but **before** any
    handler executes and before any reply is published, so a denial never
    produces business side effects or reflected messages.  Calls, casts,
    :meth:`Mailbox.abcast` broadcasts and pattern-selected messages all pass
    through the same check.

    Returning a false value or raising
    :class:`~kombu.exceptions.PolicyDenied` denies the command; any other
    exception from the policy also fails closed.  When no policy is
    configured the historical behavior is preserved and every decodable
    command is dispatched.

    .. warning::

        Roles or usernames declared in the message body, arguments or
        headers are attacker-controlled claims, not authenticated
        identities.  Base authorization on the application-bound
        :attr:`AuthorizationContext.state` instead.  This interface does
        not replace broker authentication — TLS, AMQP credentials and
        broker vhost/exchange permissions remain the application's
        responsibility.

    See ``examples/pidbox_authorization.py`` for a complete read-only
    operations role, including reply-destination validation.

    .. autoclass:: AuthorizationContext

        .. autoattribute:: method
        .. autoattribute:: arguments
        .. autoattribute:: destination
        .. autoattribute:: pattern
        .. autoattribute:: matcher
        .. autoattribute:: reply_to
        .. autoattribute:: ticket
        .. autoattribute:: headers
        .. autoattribute:: body
        .. autoattribute:: hostname
        .. autoattribute:: state

    .. autoclass:: Policy

        .. automethod:: authorize

    Node
    ----

    .. autoclass:: Node

        .. autoattribute:: hostname
        .. autoattribute:: mailbox
        .. autoattribute:: handlers
        .. autoattribute:: state
        .. autoattribute:: channel
        .. autoattribute:: policy

        .. automethod:: Consumer
        .. automethod:: handler
        .. automethod:: listen
        .. automethod:: dispatch
        .. automethod:: dispatch_from_message
        .. automethod:: handle_call
        .. automethod:: handle_cast
        .. automethod:: handle
        .. automethod:: handle_message
        .. automethod:: authorize_message
        .. automethod:: authorization_context
        .. automethod:: reply
