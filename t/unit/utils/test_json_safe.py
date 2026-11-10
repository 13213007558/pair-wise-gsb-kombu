from __future__ import annotations

import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from kombu.exceptions import DecodeError
from kombu.utils import json as legacy_json
from kombu.utils import json_safe
from kombu.utils.json import _decoders, _encoders, register_type
from kombu.utils.json_safe import (CONTENT_TYPE, FORMAT_MAGIC,
                                   FORMAT_VERSION, dumps, loads,
                                   loads_legacy)


@pytest.fixture
def restore_registry():
    """Snapshot and restore the shared type registry around a test."""
    encoders = dict(_encoders)
    decoders = dict(_decoders)
    try:
        yield
    finally:
        _encoders.clear()
        _encoders.update(encoders)
        _decoders.clear()
        _decoders.update(decoders)


# ---------------------------------------------------------------------------
# Collision pairs: structurally different inputs that the legacy codec merges.
# ---------------------------------------------------------------------------

class test_collision_pairs:
    def test_date_marker_dict_vs_real_date(self):
        plain = {"__type__": "date", "__value__": "2026-11-10"}
        real = date(2026, 11, 10)

        # Legacy codec cannot tell them apart: the plain dict becomes a date.
        assert legacy_json.loads(legacy_json.dumps(plain)) == real

        # The new codec keeps them distinct through encode/decode.
        assert loads(dumps(plain)) == plain
        assert isinstance(loads(dumps(plain)), dict)
        assert loads(dumps(real)) == real
        assert isinstance(loads(dumps(real)), date)
        assert dumps(plain) != dumps(real)

    def test_bytes_marker_dict_vs_real_bytes(self):
        plain = {"__type__": "bytes", "__value__": "abc"}
        real = b"abc"
        assert loads(dumps(plain)) == plain
        assert loads(dumps(real)) == real
        assert isinstance(loads(dumps(real)), bytes)
        assert dumps(plain) != dumps(real)

    def test_base64_marker_dict_vs_binary_bytes(self):
        import base64
        raw = b"\xff\x00\xfe"
        plain = {
            "__type__": "base64",
            "__value__": base64.b64encode(raw).decode("utf-8"),
        }
        assert loads(dumps(plain)) == plain
        assert loads(dumps(raw)) == raw

    def test_uuid_marker_dict_vs_real_uuid(self):
        real = uuid.UUID("054c451b-21aa-464d-b989-969691add273")
        plain = {"__type__": "uuid", "__value__": {"hex": real.hex}}

        # Both map to a UUID under the legacy codec.
        assert legacy_json.loads(legacy_json.dumps(plain)) == real

        assert loads(dumps(plain)) == plain
        assert loads(dumps(real)) == real
        assert isinstance(loads(dumps(real)), uuid.UUID)
        assert dumps(plain) != dumps(real)

    def test_new_internal_tag_shape_also_survives_as_plain_dict(self):
        # A user dict that mimics the *new* internal tag object must still be
        # a dict after a round trip, never dispatched to a type decoder.
        plain = {"type": "dict", "value": [["k", "v"]]}
        assert loads(dumps(plain)) == plain

    def test_collision_nested_inside_lists_and_dicts(self):
        plain = {"__type__": "datetime", "__value__": "2026-11-10T12:30:00+00:00"}
        real = datetime(2026, 11, 10, 12, 30, tzinfo=timezone.utc)
        nested = {
            "records": [plain, {"at": real}, [plain, real]],
            "deep": {"again": plain},
        }
        restored = loads(dumps(nested))
        assert restored == nested
        assert isinstance(restored["records"][0], dict)
        assert isinstance(restored["records"][1]["at"], datetime)
        assert isinstance(restored["records"][2][0], dict)
        assert isinstance(restored["records"][2][1], datetime)

    def test_int_keyed_dict_round_trips(self):
        plain = {1: "one", 2: {"__type__": "date", "__value__": "x"}}
        assert loads(dumps(plain)) == plain


# ---------------------------------------------------------------------------
# Extended types still work and stay lossless.
# ---------------------------------------------------------------------------

class test_extended_types:
    def test_builtin_types_round_trip(self):
        payload = {
            "date": date(2026, 11, 10),
            "datetime": datetime(2026, 11, 10, 12, 30, tzinfo=timezone.utc),
            "time": time(12, 30, 45),
            "decimal": Decimal("1234.5678"),
            "uuid": uuid.uuid4(),
            "utf8_bytes": b"plain text",
            "binary_bytes": b"\xff\x00\xfe",
            "list": [1, "two", b"three"],
            "tuple": (1, 2, 3),
            "none": None,
            "bool": True,
        }
        restored = loads(dumps(payload))
        expected = dict(payload)
        expected["tuple"] = [1, 2, 3]
        assert restored == expected
        assert isinstance(restored["date"], date)
        assert isinstance(restored["datetime"], datetime)
        assert isinstance(restored["uuid"], uuid.UUID)
        assert isinstance(restored["utf8_bytes"], bytes)
        assert isinstance(restored["binary_bytes"], bytes)

    def test_custom_registered_type_still_supported(self, restore_registry):
        class Money:
            def __init__(self, amount, currency):
                self.amount = amount
                self.currency = currency

            def __eq__(self, other):
                return (isinstance(other, Money) and other.amount == self.amount
                        and other.currency == self.currency)

        calls = {"count": 0}

        def decode_money(value):
            calls["count"] += 1
            return Money(value["amount"], value["currency"])

        register_type(Money, "money",
                      lambda o: {"amount": o.amount, "currency": o.currency},
                      decode_money)

        money = Money("9.99", "USD")
        restored = loads(dumps({"price": money, "qty": 2}))
        assert restored == {"price": money, "qty": 2}
        assert calls["count"] == 1

    def test_plain_dict_never_invokes_custom_decoder(self, restore_registry):
        calls = {"count": 0}

        def decode_marker(value):
            calls["count"] += 1
            return ("decoded", value)

        register_type(type("MarkerType", (), {}), "money",
                      lambda o: o, decode_marker)

        # Every shape that could be mistaken for a type tag must stay a dict.
        legacy_shape = {"__type__": "money", "__value__": {"amount": "1"}}
        new_shape = {"type": "money", "value": {"amount": "1"}}
        nested = {"batch": [legacy_shape, new_shape]}

        assert loads(dumps(legacy_shape)) == legacy_shape
        assert loads(dumps(new_shape)) == new_shape
        assert loads(dumps(nested)) == nested
        assert calls["count"] == 0

    def test_json_dunder_object(self):
        class Custom:
            def __json__(self):
                return {"ok": True}

        assert loads(dumps(Custom())) == {"ok": True}

    def test_unserializable_raises_type_error(self):
        with pytest.raises(TypeError):
            dumps(object())


# ---------------------------------------------------------------------------
# Explicit format boundary: no guessing between old and new messages.
# ---------------------------------------------------------------------------

class test_format_boundary:
    def test_envelope_is_explicit(self):
        wire = dumps({"k": "v"})
        assert wire.startswith('{"%s": %d' % (FORMAT_MAGIC, FORMAT_VERSION))

    def test_rejects_legacy_payload(self):
        legacy_wire = legacy_json.dumps(
            {"__type__": "date", "__value__": "2026-11-10"})
        with pytest.raises(DecodeError):
            loads(legacy_wire)

    def test_rejects_plain_json_without_envelope(self):
        with pytest.raises(DecodeError):
            loads('{"__type__": "date", "__value__": "2026-11-10"}')
        with pytest.raises(DecodeError):
            loads('[1, 2, 3]')

    def test_rejects_wrong_version(self):
        import json
        bad = json.dumps({FORMAT_MAGIC: 99, "payload": None})
        with pytest.raises(DecodeError):
            loads(bad)

    def test_rejects_extra_envelope_keys(self):
        import json
        bad = json.dumps({FORMAT_MAGIC: FORMAT_VERSION,
                          "payload": None, "sneaky": 1})
        with pytest.raises(DecodeError):
            loads(bad)

    def test_rejects_malformed_internal_node(self):
        import json
        bad = json.dumps({FORMAT_MAGIC: FORMAT_VERSION,
                          "payload": {"type": "dict"}})
        with pytest.raises(DecodeError):
            loads(bad)

    def test_rejects_unknown_marker(self):
        import json
        bad = json.dumps({FORMAT_MAGIC: FORMAT_VERSION,
                          "payload": {"type": "no-such-marker", "value": 1}})
        with pytest.raises(DecodeError):
            loads(bad)

    def test_malformed_json_raises_decode_error(self):
        with pytest.raises(DecodeError):
            loads("{not valid json")

    @pytest.mark.parametrize("converter", [
        lambda s: s,
        lambda s: s.encode("utf-8"),
        lambda s: bytearray(s, "utf-8"),
        lambda s: memoryview(s.encode("utf-8")),
    ])
    def test_accepts_str_bytes_bytearray_memoryview(self, converter):
        assert loads(converter(dumps({"k": "v"}))) == {"k": "v"}

    def test_legacy_entry_point_reads_old_messages(self):
        # On-disk legacy audit originals are read through the explicit entry.
        old = legacy_json.dumps({"__type__": "date", "__value__": "2026-11-10"})
        assert loads_legacy(old) == date(2026, 11, 10)
        assert isinstance(loads_legacy(old), date)


# ---------------------------------------------------------------------------
# Registry wiring: separate name/content type, default json untouched.
# ---------------------------------------------------------------------------

class test_registry:
    def test_separately_registered_with_distinct_content_type(self):
        from kombu.serialization import (prepare_accept_content, registry)

        assert registry.name_to_type["json-v2"] == CONTENT_TYPE
        assert registry.name_to_type["json"] == "application/json"
        assert CONTENT_TYPE != "application/json"
        assert prepare_accept_content(["json-v2"]) == {CONTENT_TYPE}

    def test_default_serializer_remains_legacy_json(self):
        from kombu.serialization import registry
        assert registry._default_content_type == "application/json"

    def test_round_trip_through_registry_requires_accept(self):
        from kombu.exceptions import ContentDisallowed
        from kombu.serialization import (dumps as reg_dumps, loads as reg_loads,
                                         registry)

        plain = {"__type__": "date", "__value__": "2026-11-10"}
        content_type, content_encoding, body = reg_dumps(
            plain, serializer="json-v2")
        assert content_type == CONTENT_TYPE

        # Opt-in content: when the content type is disabled and no explicit
        # accept is given, decoding must be refused. Restore the flag after.
        registry.disable("json-v2")
        try:
            with pytest.raises(ContentDisallowed):
                reg_loads(body, content_type=content_type,
                          content_encoding=content_encoding)
        finally:
            registry.enable("json-v2")

        restored = reg_loads(body, content_type=content_type,
                             content_encoding=content_encoding,
                             accept={CONTENT_TYPE})
        assert restored == plain

    def test_legacy_json_content_type_unchanged(self):
        from kombu.serialization import dumps as reg_dumps
        content_type, _, _ = reg_dumps({"k": "v"}, serializer="json")
        assert content_type == "application/json"

    def test_register_is_idempotent_and_keeps_default(self):
        json_safe.register()
        from kombu.serialization import registry
        assert registry.name_to_type["json-v2"] == CONTENT_TYPE
        assert registry._default_content_type == "application/json"
