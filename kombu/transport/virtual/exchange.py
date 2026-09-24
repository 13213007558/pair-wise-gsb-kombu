"""Virtual AMQ Exchange.

Implementations of the standard exchanges defined
by the AMQ protocol  (excluding the `headers` exchange).
"""

from __future__ import annotations


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
            Tuple[str, object, str]: of `(routing_key, pattern, queue)`
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

    #: compiled topic matcher cache
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
        """Get a cached matcher for a routing key."""
        try:
            return self._compiled[rkey]
        except KeyError:
            matcher = self._compiled[rkey] = TopicMatcher(rkey)
            return matcher

    def _match(self, pattern, string):
        """Match a routing key against a cached topic matcher."""
        if isinstance(pattern, str):
            pattern = self.key_to_pattern(pattern)
        return pattern.match(string)


class TopicMatcher:
    """Match dot-delimited topic words without regular-expression backtracking."""

    __slots__ = ('routing_key', 'words')

    def __init__(self, routing_key):
        self.routing_key = routing_key
        self.words = routing_key.split('.') if routing_key else []

    def initial_states(self):
        states = {0}
        self._extend_hash_states(states)
        return frozenset(states)

    def next_states(self, states, word):
        next_states = set()
        for state in states:
            if state >= len(self.words):
                continue
            pattern_word = self.words[state]
            if (pattern_word == '*' and word) or pattern_word == word:
                next_states.add(state + 1)
            elif pattern_word == '#':
                next_states.add(state)
        self._extend_hash_states(next_states)
        return frozenset(next_states)

    def match(self, routing_key):
        states = self.initial_states()
        words = routing_key.split('.') if routing_key else []
        for word in words:
            states = self.next_states(states, word)
        return len(self.words) in states

    def _extend_hash_states(self, states):
        pending = list(states)
        while pending:
            state = pending.pop()
            if (state < len(self.words)
                    and self.words[state] == '#'
                    and state + 1 not in states):
                    states.add(state + 1)
                    pending.append(state + 1)

    def __eq__(self, other):
        return isinstance(other, TopicMatcher) and self.routing_key == other.routing_key

    def __hash__(self):
        return hash(self.routing_key)

    def __str__(self):
        return self.routing_key

    def __json__(self):
        return self.routing_key


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
