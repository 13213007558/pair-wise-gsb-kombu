.. _guide-consumers:

===========
 Consumers
===========

.. _consumer-basics:

Basics
======

The :class:`~kombu.messaging.Consumer` takes a connection (or channel) and a list of queues to
consume from. Several consumers can be mixed to consume from different
channels, as they all bind to the same connection, and ``drain_events`` will
drain events from all channels on that connection.

.. note::

    Kombu since 3.0 will only accept json/binary or text messages by default,
    to allow deserialization of other formats you have to specify them
    in the ``accept`` argument (in addition to setting the right content type for your messages):

    .. code-block:: python

        >>> Consumer(conn, accept=['json', 'pickle', 'msgpack', 'yaml'])

You can create a consumer using a Connection. This consumer is consuming from a single queue with name `'queue'`:

.. code-block:: python

    >>> queue = Queue('queue', routing_key='queue')
    >>> consumer = connection.Consumer(queue)

You can also instantiate Consumer directly, it takes a channel or a connection as an argument. This consumer also
consumes from single queue with name `'queue'`:

.. code-block:: python

    >>> queue = Queue('queue', routing_key='queue')
    >>> with Connection('amqp://') as conn:
    ...     with conn.channel() as channel:
    ...         consumer = Consumer(channel, queue)

A consumer needs to specify a handler for received data. This handler is specified in the form of a callback. The callback function is called
by kombu every time a new message is received. The callback is called with two parameters: ``body``, containing deserialized
data sent by a producer, and a :class:`~kombu.message.Message` instance ``message``. The user is responsible for acknowledging messages when manual
acknowledgement is set.

.. code-block:: python

    >>> def callback(body, message):
    ...     print(body)
    ...     message.ack()

    >>> consumer.register_callback(callback)

Draining events from a single consumer
--------------------------------------

The method ``drain_events`` blocks indefinitely by default. This example sets the timeout to 1 second:
 
.. code-block:: python

    >>> with consumer:
    ...     connection.drain_events(timeout=1)

Draining events from several consumers
--------------------------------------

Each consumer has its own list of queues. Each consumer accepts data in `'json'` format:

.. code-block:: python

    >>> from kombu.utils.compat import nested

    >>> queues1 = [Queue('queue11', routing_key='queue11'),
                   Queue('queue12', routing_key='queue12')]
    >>> queues2 = [Queue('queue21', routing_key='queue21'),
                   Queue('queue22', routing_key='queue22')]
    >>> with connection.channel(), connection.channel() as (channel1, channel2):
    ...     with nested(Consumer(channel1, queues1, accept=['json']),
    ...                 Consumer(channel2, queues2, accept=['json'])):
    ...         connection.drain_events(timeout=1)

The full example will look as follows:

.. code-block:: python

    from kombu import Connection, Consumer, Queue

    def callback(body, message):
        print('RECEIVED MESSAGE: {0!r}'.format(body))
        message.ack()

    queue1 = Queue('queue1', routing_key='queue1')
    queue2 = Queue('queue2', routing_key='queue2')

    with Connection('amqp://') as conn:
        with conn.channel() as channel:
            consumer = Consumer(conn, [queue1, queue2], accept=['json'])
            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)

Strict content admission
========================

Passing ``accept`` limits which content types are deserialized, but the
message body is normally decompressed as soon as the
:class:`~kombu.message.Message` is constructed, before the accept list is
consulted. A consumer created with ``strict=True`` closes that gap:

.. code-block:: python

    >>> consumer = Consumer(
    ...     connection, [queue], accept=['json'], strict=True,
    ...     callbacks=[handle_message],
    ... )

In strict mode the admission policy (the consumer's ``accept`` set) is
bound to each message when the message is constructed, and every content
conversion is deferred until the consumer first needs the body. Before
either the decompressor or a serialization codec runs, the message's
content type is checked against the explicit ``accept`` set. A message
whose content type is not admitted raises
:class:`~kombu.exceptions.ContentDisallowed` at that gate, so neither the
decompressor nor the decoder is ever executed for it.

Which processing stage is intercepted
-------------------------------------

Strict mode sits between message delivery and content conversion:

1. The message is received and its delivery tag, headers and properties
   are made available, but its body is left untouched.
2. The content type is checked against ``accept`` before decompression.
3. Only an admitted message is decompressed (on first body access) and
   then decoded.

The policy therefore covers both delivery styles:

* With regular :attr:`~kombu.messaging.Consumer.callbacks`, the gate runs
  before the automatic decompression and decode that produce the
  callback's ``body`` argument.
* With :attr:`~kombu.messaging.Consumer.on_message`, the gate also runs
  before the raw message is handed to the callback. Accessing
  ``message.payload`` (or calling :meth:`~kombu.message.Message.decode`)
  is what triggers the deferred decompression and decode, and the result
  is memoized, so reading the payload repeatedly converts the body only
  once.

A rejected message is still a normal, fully addressable
:class:`~kombu.message.Message` carrying its original delivery tag, so an
``on_decode_error`` handler (or surrounding code) can call
:meth:`~kombu.message.Message.reject`, :meth:`~kombu.message.Message.requeue`
or :meth:`~kombu.message.Message.ack` exactly as for any other delivery
error. When no ``on_decode_error`` callback is configured, the
:class:`~kombu.exceptions.ContentDisallowed` exception propagates out of
:meth:`~kombu.Connection.drain_events`.

The explicit ``accept`` set is the only authority in strict mode. The
built-in trusted content types (``application/data`` and
``application/text``) receive no special treatment, and a ``force=True``
argument cannot bypass the check: content types outside ``accept`` are
refused regardless. Strict mode defaults to off, so existing consumers
keep their historical, non-strict behavior unless they opt in.

What strict mode does not do
----------------------------

Strict admission controls *which* decompressors and codecs may run; it
does not limit *how much* work they do. In particular, it provides no
compression-ratio or message-size limit. An admitted but highly
compressible (a "zip bomb") payload is still expanded normally. Apply
broker- or application-level size limits if you need protection against
resource exhaustion.

Consumer mixin classes
======================

Kombu provides predefined mixin classes in module :py:mod:`~kombu.mixins`. It contains two classes:
:class:`~kombu.mixins.ConsumerMixin` for creating consumers and :class:`~kombu.mixins.ConsumerProducerMixin`
for creating consumers supporting also publishing messages. Consumers can be created just by subclassing
mixin class and overriding some of the methods:

.. code-block:: python

    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, Consumer, channel):
            return [
                Consumer(channel, callbacks=[self.on_message], accept=['json']),
            ]

        def on_message(self, body, message):
            print('RECEIVED MESSAGE: {0!r}'.format(body))
            message.ack()

    C(connection).run()


and with multiple channels again:

.. code-block:: python

    from kombu import Consumer
    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):
        channel2 = None

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, _, default_channel):
            self.channel2 = default_channel.connection.channel()
            return [Consumer(default_channel, queues1,
                             callbacks=[self.on_message],
                             accept=['json']),
                    Consumer(self.channel2, queues2,
                             callbacks=[self.on_special_message],
                             accept=['json'])]

        def on_consume_end(self, connection, default_channel):
            if self.channel2:
                self.channel2.close()

    C(connection).run()


The main use of :class:`~kombu.mixins.ConsumerProducerMixin` is to create consumers
that need to also publish messages on a separate connection (e.g. sending rpc
replies, streaming results):

.. code-block:: python

    from kombu import Producer, Queue
    from kombu.mixins import ConsumerProducerMixin

    rpc_queue = Queue('rpc_queue')

    class Worker(ConsumerProducerMixin):

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, Consumer, channel):
            return [Consumer(
                queues=[rpc_queue],
                on_message=self.on_request,
                accept={'application/json'},
                prefetch_count=1,
            )]

        def on_request(self, message):
            n = message.payload['n']
            print(' [.] fib({0})'.format(n))
            result = fib(n)

            self.producer.publish(
                {'result': result},
                exchange='', routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json',
                retry=True,
            )
            message.ack()

.. seealso::

    :file:`examples/rpc-tut6/` in the Github repository.


Advanced Topics
===============

RabbitMQ
--------

Consumer Priorities
~~~~~~~~~~~~~~~~~~~

RabbitMQ defines a consumer priority extension to the amqp protocol,
that can be enabled by setting the ``x-priority`` argument to
``basic.consume``.

In kombu you can specify this argument on the :class:`~kombu.Queue`, like
this:

.. code-block:: python

    queue = Queue('name', Exchange('exchange_name', type='direct'),
                  consumer_arguments={'x-priority': 10})

Read more about consumer priorities here:
https://www.rabbitmq.com/consumer-priority.html


Reference
=========

.. autoclass:: kombu.Consumer
    :noindex:
    :members:
