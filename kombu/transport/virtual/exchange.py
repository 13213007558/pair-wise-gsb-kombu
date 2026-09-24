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

    def lookup(self, table, exchange, routing_key, default, headers=None):
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

    def lookup(self, table, exchange, routing_key, default, headers=None):
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

    def lookup(self, table, exchange, routing_key, default, headers=None):
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

    def lookup(self, table, exchange, routing_key, default, headers=None):
        return {queue for _, _, queue in table}

    def deliver(self, message, exchange, routing_key, **kwargs):
        if self.channel.supports_fanout:
            self.channel._put_fanout(
                exchange, message, routing_key, **kwargs)


class HeadersExchange(ExchangeType):
    """Headers exchange.

    The 'headers' exchange routes messages based on their headers,
    which are matched against the binding arguments using the algorithm
    selected by the special 'x-match' binding argument:

    'all' (the default)
        every match field must be present in the message headers and
        equal to the bound value.
    'any'
        at least one match field must be present and equal.

    The RabbitMQ extensions 'all-with-x' and 'any-with-x' work the
    same way, except that match fields whose name starts with 'x-'
    are also considered (the plain 'all'/'any' modes ignore them).

    A binding without any match fields matches every message in
    'all' mode, and no message in 'any' mode.

    Values are compared using AMQP field value semantics: missing
    headers never match, 'null' only matches 'null', and booleans
    are distinct from integers ('True' does not match '1').
    """

    type = 'headers'

    #: valid values for the 'x-match' binding argument.
    MATCH_ALGORITHMS = frozenset({
        'all', 'any', 'all-with-x', 'any-with-x',
    })

    def lookup(self, table, exchange, routing_key, default, headers=None):
        headers = headers or {}
        return {
            queue for _, arguments, queue in table
            if self._matches(arguments, headers)
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        for queue in _lookup(exchange, routing_key,
                             headers=message.get('headers')):
            _put(queue, message, **kwargs)

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        arguments = arguments or {}
        match = arguments.get('x-match', 'all')
        if match not in self.MATCH_ALGORITHMS:
            raise ValueError(
                "x-match binding argument must be one of 'all', 'any', "
                "'all-with-x' or 'any-with-x', got {!r}".format(match))
        return routing_key, arguments, queue

    def _matches(self, arguments, headers):
        match = arguments.get('x-match', 'all')
        with_x = match.endswith('-with-x')
        fields = {
            key: value for key, value in arguments.items()
            if key != 'x-match' and (with_x or not key.startswith('x-'))
        }
        if match.startswith('all'):
            return all(
                self._field_matches(headers, key, value)
                for key, value in fields.items()
            )
        return any(
            self._field_matches(headers, key, value)
            for key, value in fields.items()
        )

    def _field_matches(self, headers, key, expected):
        if key not in headers:
            return False
        return self._field_equal(expected, headers[key])

    @classmethod
    def _field_equal(cls, expected, actual):
        """Compare two AMQP field values, honoring their types.

        Unlike Python, AMQP booleans are not integers and null is
        not equal to any other value, so True never matches 1
        and None never matches 0 or ''.
        """
        if isinstance(expected, bool) or isinstance(actual, bool):
            return (isinstance(expected, bool) and
                    isinstance(actual, bool) and
                    expected == actual)
        if (isinstance(expected, (list, tuple)) and
                isinstance(actual, (list, tuple))):
            return (len(expected) == len(actual) and
                    all(cls._field_equal(expect_item, actual_item)
                        for expect_item, actual_item
                        in zip(expected, actual)))
        if type(expected) is not type(actual):
            return False
        return expected == actual


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {
    'direct': DirectExchange,
    'topic': TopicExchange,
    'fanout': FanoutExchange,
    'headers': HeadersExchange,
}
