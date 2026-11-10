"""Virtual AMQ Exchange.

Implementations of the standard exchanges defined
by the AMQ protocol  (excluding the `headers` exchange).
"""

from __future__ import annotations

from kombu.utils.text import escape_regex


def _split_words(key):
    """Split a routing key into dot-separated words.

    An empty routing key has zero words.
    """
    return key.split('.') if key else []


def topic_match(pattern, routing_key, _visits=None):
    """Match a topic binding key against a routing key.

    ``*`` matches exactly one (non-empty) word and ``#`` matches zero
    or more words; every other word matches itself literally.

    The work is a dynamic program over word positions, so a match
    costs at most ``(len(pattern_words) + 1) * (len(key_words) + 1)``
    state evaluations regardless of how many ``#`` wildcards are used.

    If ``_visits`` is a list, the number of evaluated states is
    appended to it (used by tests to verify the growth of the work).
    """
    pattern_words = _split_words(pattern)
    key_words = _split_words(routing_key)
    # previous[j] is true when the pattern words seen so far match
    # the first j words of the routing key.
    previous = [True] + [False] * len(key_words)
    visits = 0
    for word in pattern_words:
        current = [False] * (len(key_words) + 1)
        if word == '#':
            # '#' absorbs zero or more words: once a key prefix is
            # matched, every extension of that prefix matches too.
            reachable = False
            for j in range(len(key_words) + 1):
                reachable = reachable or previous[j]
                current[j] = reachable
                visits += 1
        elif word == '*':
            for j, key_word in enumerate(key_words):
                current[j + 1] = previous[j] and bool(key_word)
                visits += 1
        else:
            for j, key_word in enumerate(key_words):
                current[j + 1] = previous[j] and key_word == word
                visits += 1
        previous = current
    if _visits is not None:
        _visits.append(visits)
    return previous[-1]


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
    dots, using wildcard characters ``*`` (exactly one word), and ``#``
    (zero or more words).
    """

    type = 'topic'

    #: map of wildcard to regex conversions used by :meth:`key_to_pattern`.
    #: The regex is no longer evaluated for routing; it is kept because
    #: transports such as redis and mongodb persist bindings as
    #: ``(routing_key, pattern, queue)`` member strings and can only
    #: update or remove bindings created by older versions when this
    #: serialization stays byte-for-byte identical.
    wildcards = {'*': r'[^\.]+',
                 '#': r'.*?'}

    def lookup(self, table, exchange, routing_key, default):
        # Matching uses the stored routing key (first field), so
        # binding records persisted by older versions -- which carry a
        # regex in the pattern field -- keep routing correctly.
        return {
            queue for rkey, _, queue in table
            if topic_match(rkey, routing_key)
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
        """Get the corresponding regex for any routing key.

        Kept for backward compatibility of persisted binding records;
        routing itself uses :func:`topic_match` on the routing key.
        """
        return '^%s$' % (r'\.'.join(
            self.wildcards.get(word, word)
            for word in escape_regex(rkey, '.#*').split('.')
        ))


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


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {
    'direct': DirectExchange,
    'topic': TopicExchange,
    'fanout': FanoutExchange,
}
