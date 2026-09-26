"""Exceptions."""

from __future__ import annotations

from socket import timeout as TimeoutError
from types import TracebackType
from typing import TYPE_CHECKING, TypeVar

from amqp import ChannelError, ConnectionError, ResourceError

if TYPE_CHECKING:
    from kombu.asynchronous.http import Response

__all__ = (
    'reraise', 'KombuError', 'OperationalError', 'BatchPublishError',
    'NotBoundError', 'MessageStateError', 'TimeoutError',
    'LimitExceeded', 'ConnectionLimitExceeded',
    'ChannelLimitExceeded', 'ConnectionError', 'ChannelError',
    'VersionMismatch', 'SerializerNotInstalled', 'ResourceError',
    'SerializationError', 'EncodeError', 'DecodeError', 'HttpError',
    'InconsistencyError',
    'TopologyError', 'TopologyConflict', 'TopologyCycleError',
    'UnsupportedExchangeType', 'UnsupportedTopologyFeature',
    'TopologyExecutionError',
)

BaseExceptionType = TypeVar('BaseExceptionType', bound=BaseException)


def reraise(
    tp: type[BaseExceptionType],
    value: BaseExceptionType,
    tb: TracebackType | None = None
) -> BaseExceptionType:
    """Reraise exception."""
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class KombuError(Exception):
    """Common subclass for all Kombu exceptions."""


class OperationalError(KombuError):
    """Recoverable message transport connection error."""


class BatchPublishError(KombuError):
    """Batch publication failed with an uncertain delivery outcome."""


class SerializationError(KombuError):
    """Failed to serialize/deserialize content."""


class EncodeError(SerializationError):
    """Cannot encode object."""


class DecodeError(SerializationError):
    """Cannot decode object."""


class NotBoundError(KombuError):
    """Trying to call channel dependent method on unbound entity."""


class MessageStateError(KombuError):
    """The message has already been acknowledged."""


class LimitExceeded(KombuError):
    """Limit exceeded."""


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous channels exceeded."""


class VersionMismatch(KombuError):
    """Library dependency version mismatch."""


class SerializerNotInstalled(KombuError):
    """Support for the requested serialization type is not installed."""


class ContentDisallowed(SerializerNotInstalled):
    """Consumer does not allow this content-type."""


class InconsistencyError(ConnectionError):
    """Data or environment has been found to be inconsistent.

    Depending on the cause it may be possible to retry the operation.
    """


class TopologyError(KombuError):
    """Topology plan validation or execution failed."""


class TopologyConflict(TopologyError):
    """Conflicting declarations for the same entity name.

    Raised when two declarations share a name but differ in their
    declaration semantics (e.g. exchange type/durability, or queue
    arguments).
    """


class TopologyCycleError(TopologyError):
    """Exchange-to-exchange bindings form a dependency cycle."""


class UnsupportedExchangeType(TopologyError):
    """The transport does not support a declared exchange type."""


class UnsupportedTopologyFeature(TopologyError):
    """The transport does not support a required topology feature."""


class TopologyExecutionError(TopologyError):
    """A topology plan failed while executing.

    Already executed steps are *not* rolled back: objects that were
    successfully declared on the server stay declared.

    Attributes:
        operation: The plan operation that failed.
        completed (list): Operations successfully executed before the
            failure, in execution order.
        original (Exception): The original error raised by the failed
            step.
    """

    def __init__(self, operation, completed, original):
        self.operation = operation
        self.completed = list(completed)
        self.original = original
        super().__init__(
            'topology execution failed at step {} ({}); '
            '{} step(s) already executed: {}'.format(
                len(self.completed) + 1, operation,
                len(self.completed), original))
class HttpError(Exception):
    """HTTP Client Error."""

    def __init__(
        self,
        code: int,
        message: str | None = None,
        response: Response | None = None
    ) -> None:
        self.code = code
        self.message = message
        self.response = response
        super().__init__(code, message, response)

    def __str__(self) -> str:
        return 'HTTP {0.code}: {0.message}'.format(self)
