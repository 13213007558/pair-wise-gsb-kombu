"""Virtual AMQ Exchange.

Implementations of the standard exchanges defined
by the AMQ protocol.
"""

from __future__ import annotations

import re

from kombu.utils.text import escape_regex


class ExchangeType:
    """Base class for exchanges.

    Implements the specifics for an exchange type.

    Arguments:
    ---------
        channel (ChannelT): AMQ Channel.
    """

    type = None

    def __init__(self, channel):
        self.channel = channel

    def lookup(self, table, exchange, routing_key, default):
        """Lookup all queues matching `routing_key` in `exchange`.

        Returns
        -------
            str: queue name, or 'default' if no queues matched.
        """
        raise NotImplementedError('subclass responsibility')

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        """Prepare queue-binding.

        Returns
        -------
            Tuple[str, Pattern, str]: of `(routing_key, regex, queue)`
                to be stored for bindings to this exchange.
        """
        return routing_key, None, queue

    def equivalent(self, prev, exchange, type,
                   durable, auto_delete, arguments):
        """Return true if `prev` and `exchange` is equivalent."""
        return (type == prev['type'] and
                durable == prev['durable'] and
                auto_delete == prev['auto_delete'] and
                (arguments or {}) == (prev['arguments'] or {}))


class DirectExchange(ExchangeType):
    """Direct exchange.

    The `direct` exchange routes based on exact routing keys.
    """

    type = 'direct'

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, _, queue in table
            if rkey == routing_key
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        for queue in _lookup(exchange, routing_key):
            _put(queue, message, **kwargs)


class TopicExchange(ExchangeType):
    """Topic exchange.

    The `topic` exchange routes messages based on words separated by
    dots, using wildcard characters ``*`` (any single word), and ``#``
    (one or more words).
    """

    type = 'topic'

    #: map of wildcard to regex conversions
    wildcards = {'*': r'[^\.]+',
                 '#': r'.*?'}

    #: compiled regex cache
    _compiled = {}

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, pattern, queue in table
            if self._match(pattern, routing_key)
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        deadletter = self.channel.deadletter_queue
        for queue in [q for q in _lookup(exchange, routing_key)
                      if q and q != deadletter]:
            _put(queue, message, **kwargs)

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, self.key_to_pattern(routing_key), queue

    def key_to_pattern(self, rkey):
        """Get the corresponding regex for any routing key."""
        return '^%s$' % (r'\.'.join(
            self.wildcards.get(word, word)
            for word in escape_regex(rkey, '.#*').split('.')
        ))

    def _match(self, pattern, string):
        """Match regular expression (cached).

        Same as :func:`re.match`, except the regex is compiled and cached,
        then reused on subsequent matches with the same pattern.
        """
        try:
            compiled = self._compiled[pattern]
        except KeyError:
            compiled = self._compiled[pattern] = re.compile(pattern, re.U)
        return compiled.match(string)


class FanoutExchange(ExchangeType):
    """Fanout exchange.

    The `fanout` exchange implements broadcast messaging by delivering
    copies of all messages to all queues bound to the exchange.

    To support fanout the virtual channel needs to store the table
    as shared state.  This requires that the `Channel.supports_fanout`
    attribute is set to true, and the `Channel._queue_bind` and
    `Channel.get_table` methods are implemented.

    See Also
    --------
        the redis backend for an example implementation of these methods.
    """

    type = 'fanout'

    def lookup(self, table, exchange, routing_key, default):
        return {queue for _, _, queue in table}

    def deliver(self, message, exchange, routing_key, **kwargs):
        if self.channel.supports_fanout:
            self.channel._put_fanout(
                exchange, message, routing_key, **kwargs)


def amqp_field_value_equal(binding_value, header_value):
    """Compare two AMQP field values for equality.

    Values of different AMQP field types are never equal, even when
    Python would consider them so: a boolean is not an integer
    (``True != 1``), ``null`` only ever equals ``null``, and arrays
    only equal arrays of the same length with element-wise equal items.
    """
    if isinstance(binding_value, bool) or isinstance(header_value, bool):
        return (isinstance(binding_value, bool) and
                isinstance(header_value, bool) and
                binding_value == header_value)
    if binding_value is None or header_value is None:
        return binding_value is None and header_value is None
    if (isinstance(binding_value, (list, tuple)) or
            isinstance(header_value, (list, tuple))):
        if not (isinstance(binding_value, (list, tuple)) and
                isinstance(header_value, (list, tuple))):
            return False
        return (len(binding_value) == len(header_value) and
                all(amqp_field_value_equal(binding_item, header_item)
                    for binding_item, header_item
                    in zip(binding_value, header_value)))
    return (type(binding_value) is type(header_value) and
            binding_value == header_value)


class HeadersExchange(ExchangeType):
    """Headers exchange.

    The `headers` exchange routes messages based on a match between the
    message headers and the arguments queues were bound with, ignoring
    the routing key.

    The special binding argument ``x-match`` selects the matching
    algorithm:

    ``all`` (default)
        Every match field must equal the corresponding message header.
    ``any``
        At least one match field must equal the corresponding
        message header.
    ``all-with-x`` / ``any-with-x``
        RabbitMQ extension: like ``all``/``any``, but binding arguments
        prefixed with ``x-`` take part in matching as well (the plain
        ``all``/``any`` modes ignore them).

    With an empty set of match fields ``all`` modes match every
    message, while ``any`` modes match nothing.

    Binding arguments and message headers are compared using AMQP
    field-value semantics (see :func:`amqp_field_value_equal`):
    a missing header never matches, not even against a ``null``
    binding value.
    """

    type = 'headers'

    #: ``x-match`` modes requiring every match field to match.
    MATCH_ALL = frozenset({'all', 'all-with-x'})

    #: ``x-match`` modes requiring at least one match field to match.
    MATCH_ANY = frozenset({'any', 'any-with-x'})

    #: ``x-match`` modes where ``x-`` prefixed fields take part
    #: in matching.
    MATCH_X = frozenset({'all-with-x', 'any-with-x'})

    def lookup(self, table, exchange, routing_key, default):
        # Routing requires the message headers, which are not
        # available to ``Channel._lookup``, so nothing can be matched.
        return set()

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, arguments, queue

    def deliver(self, message, exchange, routing_key, **kwargs):
        # Note: only the headers take part in routing, the message
        # body is never inspected.
        headers = message.get('headers') or {}
        _put = self.channel._put
        for queue in self._lookup_queues(exchange, headers):
            _put(queue, message, **kwargs)

    def _lookup_queues(self, exchange, headers):
        try:
            table = self.channel.get_table(exchange)
        except KeyError:
            return set()
        # A queue with multiple matching bindings still only
        # receives one copy of the message.
        return {
            queue for _, arguments, queue in table
            if self.matches(arguments, headers)
        }

    def matches(self, arguments, headers):
        """Return true if message `headers` match binding `arguments`."""
        arguments = arguments or {}
        mode = arguments.get('x-match', 'all')
        with_x = mode in self.MATCH_X
        fields = tuple(
            (key, value) for key, value in arguments.items()
            if key != 'x-match' and (with_x or not key.startswith('x-'))
        )
        matched = (
            key in headers and amqp_field_value_equal(value, headers[key])
            for key, value in fields
        )
        if mode in self.MATCH_ALL:
            return all(matched)
        if mode in self.MATCH_ANY:
            return any(matched)
        return False  # unknown x-match mode never matches


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {
    'direct': DirectExchange,
    'topic': TopicExchange,
    'fanout': FanoutExchange,
    'headers': HeadersExchange,
}
