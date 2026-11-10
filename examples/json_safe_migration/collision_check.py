"""Paired, collision-prone inputs and custom-decoder call accounting.

For every pair, the two values have *different* Python types but the legacy
``json`` serializer collapses them onto the same bytes (that is the bug).
``json-v2`` must keep them distinguishable through encode/decode. A custom
decoder counter proves that the plain-dictionary path never dispatches into a
registered type decoder.
"""

from __future__ import annotations

import base64
import uuid
from datetime import date

from kombu.utils import json as legacy_json
from kombu.utils.json import _decoders, _encoders, register_type
from kombu.utils.json_safe import dumps, loads

PAIRS = []


def pair(name, plain, extended):
    PAIRS.append((name, plain, extended))


pair('date', {'__type__': 'date', '__value__': '2026-11-10'},
     date(2026, 11, 10))
pair('bytes', {'__type__': 'bytes', '__value__': 'abc'}, b'abc')
pair('base64', {'__type__': 'base64',
                '__value__': base64.b64encode(b'\xff\x00').decode()},
     b'\xff\x00')
_u = uuid.UUID('054c451b-21aa-464d-b989-969691add273')
pair('uuid', {'__type__': 'uuid', '__value__': {'hex': _u.hex}}, _u)
# A dict that mimics the *new* internal tag shape must also stay a plain dict.
pair('new-tag-shape', {'type': 'dict', 'value': [['k', 'v']]},
     {'k': 'v'})


def main():
    saved_encoders, saved_decoders = dict(_encoders), dict(_decoders)
    decoder_calls = {'count': 0}

    class Flag:
        def __init__(self, value):
            self.value = value

    def decode_flag(value):
        decoder_calls['count'] += 1
        return Flag(value)

    register_type(Flag, 'flag', lambda o: o.value, decode_flag)
    try:
        print(f'{"pair":<14}{"legacy merges?":<16}{"v2 distinct?":<14}'
              f'{"plain stays dict?"}')
        all_distinct = True
        for name, plain, extended in PAIRS:
            legacy_merges = legacy_json.dumps(plain) == \
                legacy_json.dumps(extended)
            v2_distinct = dumps(plain) != dumps(extended) and \
                loads(dumps(plain)) == plain and \
                loads(dumps(extended)) == extended
            stays_dict = isinstance(loads(dumps(plain)), dict)
            all_distinct &= v2_distinct and stays_dict
            print(f'{name:<14}{str(legacy_merges):<16}{str(v2_distinct):<14}'
                  f'{stays_dict}')

        # Decoder accounting: plain-dict paths must not invoke the decoder.
        before = decoder_calls['count']
        for _, plain, _ in PAIRS:
            loads(dumps({'batch': [plain, {'nested': plain}]}))
        plain_dict_decoder_calls = decoder_calls['count'] - before
        print('custom decoder calls while decoding plain dicts:',
              plain_dict_decoder_calls)
        assert plain_dict_decoder_calls == 0

        # A genuinely tagged custom type must still reach the decoder once.
        loads(dumps(Flag('real')))
        assert decoder_calls['count'] == 1
        print('custom decoder calls for one real tagged object: 1')

        assert all_distinct
        print('all paired inputs are collision-free under json-v2')
    finally:
        _encoders.clear(); _encoders.update(saved_encoders)
        _decoders.clear(); _decoders.update(saved_decoders)


if __name__ == '__main__':
    main()
