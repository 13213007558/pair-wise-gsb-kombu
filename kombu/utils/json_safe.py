"""Collision-free JSON serialization (wire format v1).

The legacy :mod:`kombu.utils.json` codec marks extended types with a plain
two-key dictionary, ``{"__type__": marker, "__value__": value}``.  Any user
payload that happens to be a dictionary with exactly those two keys is
indistinguishable from a tagged value, so audit originals can silently turn
into dates, bytes or other registered types after a round trip.

This module fixes that with an explicit, versioned wire format whose grammar
never lets user dictionaries overlap the type-tag representation:

* the whole message is wrapped in a versioned envelope, so the format boundary
  is structural rather than guessed from key names;
* user dictionaries are encoded as *arrays of key/value pairs* under a
  structural tag, while type tags are *objects*.  Arrays and objects are
  disjoint JSON types, so no user value can ever look like a type tag;
* user lists use their own structural tag, distinct from dictionaries.

Wire format (version 1)::

    {"__kombu_json__": 1, "payload": <encoded>}

where ``<encoded>`` follows:

* ``str``, ``int``, ``float``, ``bool`` and ``None`` are emitted unchanged;
* a dict is ``{"type": "dict", "value": [[encoded_key, encoded_value], ...]}``;
* a list/tuple is ``{"type": "list", "value": [encoded_item, ...]}``;
* a registered extended type is
  ``{"type": marker, "value": encoded_encoder_result}``.

The type registry is shared with :mod:`kombu.utils.json`, so every type
registered through :func:`kombu.utils.json.register_type` (including custom
application types) keeps working without being re-registered.
"""

from __future__ import annotations

import json
from typing import Any

from ..exceptions import DecodeError
from . import json as _legacy_json

# Re-exported so callers can register custom types from either module.
from .json import _decoders, _encoders, register_type, textual_types

__all__ = (
    'FORMAT_MAGIC', 'FORMAT_VERSION', 'SERIALIZER_NAME', 'CONTENT_TYPE',
    'dumps', 'loads', 'loads_legacy', 'register', 'register_type',
)

#: Name under which the codec is registered in the serializer registry.
SERIALIZER_NAME = 'json-v2'
#: Distinct MIME type; it never overlaps the legacy ``application/json``.
CONTENT_TYPE = 'application/vnd.kombu.json-v2+json'

#: Explicit format boundary: only an object carrying exactly these two keys is
#: accepted as a v1 message. Nothing is inferred from payload contents.
FORMAT_MAGIC = '__kombu_json__'
FORMAT_VERSION = 1

_PAYLOAD_KEY = 'payload'
_TAG_KEY = 'type'
_VALUE_KEY = 'value'
_DICT_TAG = 'dict'
_LIST_TAG = 'list'


def _tag(marker: str, value: Any) -> dict:
    return {_TAG_KEY: marker, _VALUE_KEY: value}


def _encode(obj: Any) -> Any:
    """Translate a Python value into the collision-free wire grammar."""
    # Native JSON scalars pass through untouched.
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj

    reducer = getattr(obj, '__json__', None)
    if reducer is not None:
        # The reduced value may itself be an extended type, so encode it again.
        return _encode(reducer())

    if isinstance(obj, textual_types):
        return str(obj)

    if isinstance(obj, dict):
        return _tag(_DICT_TAG, [
            [_encode(key), _encode(value)] for key, value in obj.items()
        ])

    if isinstance(obj, (list, tuple)):
        return _tag(_LIST_TAG, [_encode(item) for item in obj])

    for registered_type, (marker, encoder) in _encoders.items():
        if isinstance(obj, registered_type):
            encoded = encoder(obj)
            if marker is None:
                # Pure transformation: the encoded value owns the round trip.
                return _encode(encoded)
            return _tag(marker, _encode(encoded))

    # Bytes is handled separately, mirroring the utf-8/base64 split of the
    # legacy codec so existing consumers of the markers stay compatible.
    if isinstance(obj, (bytes, bytearray)):
        raw = bytes(obj)
        try:
            return _tag('bytes', raw.decode('utf-8'))
        except UnicodeDecodeError:
            import base64
            return _tag('base64', base64.b64encode(raw).decode('utf-8'))

    raise TypeError(
        f'Object of type {type(obj).__name__!r} is not JSON serializable')


def _decode(node: Any) -> Any:
    """Inverse of :func:`_encode`, applied to an already parsed node."""
    if not isinstance(node, dict):
        # Native scalar; lists only reach us through the structural tag.
        return node

    if set(node) != {_TAG_KEY, _VALUE_KEY}:
        raise DecodeError(
            f'Invalid {SERIALIZER_NAME} node: expected exactly '
            f'{_TAG_KEY!r} and {_VALUE_KEY!r} keys, got {sorted(node)}')

    marker = node[_TAG_KEY]
    value = node[_VALUE_KEY]
    if not isinstance(marker, str):
        raise DecodeError(
            f'Invalid {SERIALIZER_NAME} tag: marker must be a string, '
            f'got {type(marker).__name__!r}')

    if marker == _DICT_TAG:
        if not isinstance(value, list):
            raise DecodeError(
                f'Invalid {SERIALIZER_NAME} dict node: value must be a list')
        result = {}
        for pair in value:
            if not isinstance(pair, list) or len(pair) != 2:
                raise DecodeError(
                    f'Invalid {SERIALIZER_NAME} dict entry: '
                    f'expected [key, value] pair, got {pair!r}')
            result[_decode(pair[0])] = _decode(pair[1])
        return result

    if marker == _LIST_TAG:
        if not isinstance(value, list):
            raise DecodeError(
                f'Invalid {SERIALIZER_NAME} list node: value must be a list')
        return [_decode(item) for item in value]

    decoder = _decoders.get(marker)
    if decoder is None:
        raise DecodeError(
            f'Unsupported {SERIALIZER_NAME} type marker {marker!r}')
    # Only genuinely tagged extended types reach a registered decoder;
    # plain dicts and lists take the structural branches above.
    return decoder(_decode(value))


def dumps(obj: Any, **kwargs: Any) -> str:
    """Serialize ``obj`` to a versioned, collision-free JSON string."""
    envelope = {
        FORMAT_MAGIC: FORMAT_VERSION,
        _PAYLOAD_KEY: _encode(obj),
    }
    return json.dumps(envelope, **kwargs)


def loads(data: Any) -> Any:
    """Deserialize a message produced by :func:`dumps`.

    Accepts ``str``, ``bytes``, ``bytearray`` or ``memoryview``.  Payloads
    without the versioned envelope are rejected rather than guessed at, so
    legacy messages can never be misinterpreted as this format (and vice
    versa).  Use :func:`loads_legacy` for the explicit legacy entry point.
    """
    if isinstance(data, memoryview):
        data = data.tobytes().decode('utf-8')
    elif isinstance(data, bytearray):
        data = data.decode('utf-8')
    elif isinstance(data, bytes):
        data = data.decode('utf-8')

    try:
        root = json.loads(data)
    except ValueError as exc:
        raise DecodeError(f'Malformed {SERIALIZER_NAME} payload: {exc}')

    if not isinstance(root, dict) or set(root) != {
            FORMAT_MAGIC, _PAYLOAD_KEY}:
        raise DecodeError(
            f'Not a {SERIALIZER_NAME} message: missing the '
            f'{FORMAT_MAGIC!r}/{_PAYLOAD_KEY!r} envelope')
    if root[FORMAT_MAGIC] != FORMAT_VERSION:
        raise DecodeError(
            f'Unsupported {SERIALIZER_NAME} format version '
            f'{root[FORMAT_MAGIC]!r}; supported version is {FORMAT_VERSION}')

    return _decode(root[_PAYLOAD_KEY])


def loads_legacy(data: Any) -> Any:
    """Explicit entry point for messages stored on disk by the legacy codec.

    Legacy audit originals use the old ``{"__type__", "__value__}`` grammar
    and must be read deliberately through this function instead of being
    auto-detected by :func:`loads`.
    """
    return _legacy_json.loads(data)


def register() -> None:
    """Register the collision-free codec without touching the default.

    The legacy ``json`` serializer and its ``application/json`` content type
    remain the default and unchanged; old consumers therefore keep decoding
    old messages exactly as before.
    """
    from ..serialization import registry
    registry.register(
        SERIALIZER_NAME, dumps, loads,
        content_type=CONTENT_TYPE,
        content_encoding='utf-8',
    )
