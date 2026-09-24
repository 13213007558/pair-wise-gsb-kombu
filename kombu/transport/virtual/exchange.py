
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

    Patterns are matched by simulating a small NFA over the words of
    the routing key instead of using a backtracking regular
    expression: at most ``len(tokens) + 1`` states are active at any
    time, so matching takes ``O(len(tokens) * len(words))`` steps no
    matter how many wildcards the pattern contains.
    """

    type = 'topic'

    #: compiled pattern cache, mapping binding pattern to word tokens.
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
        """Get the corresponding match pattern for any routing key.

        The binding key is itself the pattern language, so it is kept
        as-is (some transports serialize it into the binding table);
        it is compiled to word tokens lazily by :meth:`_match`.
        """
        return rkey

    def _compile(self, pattern):
        """Compile pattern into a tuple of word tokens (cached)."""
        try:
            return self._compiled[pattern]
        except KeyError:
            tokens = self._compiled[pattern] = tuple(pattern.split('.'))
            return tokens

    def _match(self, pattern, string):
        """Match routing key against pattern (compiled and cached).

        Returns true if the routing key matches the pattern.
        """
        matched, _ = self._match_steps(pattern, string)
        return matched

    def _match_steps(self, pattern, string):
        """Match routing key, returning ``(matched, steps)``.

        The ``steps`` count is the number of NFA states processed,
        bounded by ``2 * (len(tokens) + 1) * (len(words) + 1)``.
        """
        tokens = self._compile(pattern)
        last = len(tokens)
        steps = 0

        def closure(states):
            # '#' matches zero words: propagate past '#' tokens.
            nonlocal steps
            stack = list(states)
            while stack:
                steps += 1
                state = stack.pop()
                if state < last and tokens[state] == '#':
                    nxt = state + 1
                    if nxt not in states:
                        states.add(nxt)
                        stack.append(nxt)
            return states

        states = closure({0})
        for word in string.split('.'):
            nxt = set()
            for state in states:
                steps += 1
                if state >= last:
                    continue
                token = tokens[state]
                if token == '#':
                    nxt.add(state)  # consume one word, stay
                elif token == '*' or token == word:
                    nxt.add(state + 1)
            if not nxt:
                return False, steps
            states = closure(nxt)
        return last in states, steps


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
