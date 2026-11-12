"""Receiver-side authorization for a pidbox mailbox.

A node on a shared management bus must not run every handler just because
a pidbox message decoded successfully.  This example installs a read-only
operations policy on a :class:`~kombu.pidbox.Mailbox`: only safe,
allow-listed commands are executed, mutating commands are dropped before
their handler runs, and no reply is published for a rejected command.

Important security notes
------------------------
* The trusted role lives in the application-bound ``state`` of the node.
  A ``role`` or ``username`` field arriving inside the message body or
  headers is attacker-controlled and is never used as an identity.
* This policy authorizes *commands* after the message has reached the
  mailbox exchange.  It does **not** authenticate the broker connection
  itself.  Broker authentication (TLS, AMQP credentials, vhost and
  exchange permissions) stays the responsibility of your application and
  deployment.
"""

from __future__ import annotations

from kombu import Connection
from kombu.pidbox import AuthorizationContext, Mailbox, Policy

#: Commands a read-only operator is allowed to invoke.
READ_ONLY_COMMANDS = frozenset({'ping', 'stats', 'active_queues'})


class ReadOnlyOpsPolicy(Policy):
    """Allow-listed, context-bound read-only operations role."""

    def authorize(self, context: AuthorizationContext) -> bool:
        # Trust only the identity the application bound locally.
        if context.state.get('role') != 'ops-readonly':
            return False

        # Unknown or mutating commands are refused.
        if context.method not in READ_ONLY_COMMANDS:
            return False

        # A call may only ask for replies on this mailbox's own reply
        # exchange; never reflect traffic to an attacker-chosen target.
        if context.reply_to is not None:
            expected_exchange = 'reply.{}.pidbox'.format(
                context.state['namespace'])
            if context.reply_to.get('exchange') != expected_exchange:
                return False

        return True


def build_node(connection: Connection, namespace: str):
    # The role is asserted by the application when it wires the node,
    # not extracted from any incoming message.
    state = {'role': 'ops-readonly', 'namespace': namespace}

    mailbox = Mailbox(
        namespace, type='direct', policy=ReadOnlyOpsPolicy())(connection)
    node = mailbox.Node('{}-worker'.format(namespace), state=state)

    @node.handler
    def ping(state):
        return 'pong'

    @node.handler
    def stats(state):
        return {'queue_depth': 0}

    @node.handler
    def shutdown(state):
        # Registered, but never reachable under the read-only policy.
        raise RuntimeError('shutdown must not run for read-only operators')

    return mailbox, node


def main() -> None:
    with Connection('memory://') as connection:
        mailbox, node = build_node(connection, 'example_pidbox_auth')
        channel = connection.channel()
        node.channel = channel
        consumer = node.listen()
        mailbox.reply_queue(channel).declare()

        # Allowed: an allow-listed cast runs the handler.
        mailbox.cast(['example_pidbox_auth-worker'], 'ping')

        # Refused before the handler (and without any reply): a mutating
        # command, even if the message claims an elevated role.
        mailbox.cast(
            ['example_pidbox_auth-worker'], 'shutdown',
            kwargs={'role': 'admin'},
        )

        import socket

        for _ in range(4):
            try:
                connection.drain_events(timeout=0.1)
            except socket.timeout:
                break

        consumer.cancel()


if __name__ == '__main__':
    main()
