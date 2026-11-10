=============================
 Rolling migration to json-v2
=============================

This example shows how to move producers and consumers from the legacy
``json`` serializer to the collision-free ``json-v2`` serializer without
downtime and without ever rewriting the default ``json`` serializer.

Why
===
The legacy codec marks extended types as a plain two-key dictionary
(``{"__type__", "__value__"}``). An audit original that happens to use those
same two keys is silently turned into a date, bytes or another registered
type when it is decoded. ``json-v2`` keeps user dictionaries structurally
distinct from type tags and wraps every message in an explicit versioned
envelope, so the two cannot collide and formats are never guessed.

Content types
=============
* ``json``    -> ``application/json``              (legacy, stays default)
* ``json-v2`` -> ``application/vnd.kombu.json-v2+json`` (new)

A consumer selects which formats it will decode through ``accept``. The
serializer name or the MIME type may be used there.

The phases
==========
1. **Dual-read, old-write.** Deploy the new library everywhere and widen the
   consumers first::

       accept = ['json', 'json-v2']
       serializer = 'json'        # producers unchanged

   No v2 messages exist yet, so behaviour is identical; the fleet simply
   becomes capable of reading them.

2. **Dual-read, new-write.** Flip producers to ``serializer='json-v2'`` while
   consumers keep both formats accepted. Slow consumers and in-flight legacy
   messages are still decoded, while every newly published message uses the
   safe format::

       accept = ['json', 'json-v2']
       serializer = 'json-v2'

3. **New-only.** Once no legacy message can remain (queues drained and the
   retention window elapsed), drop the legacy accept::

       accept = ['json-v2']
       serializer = 'json-v2'

A legacy consumer (``accept=['json']``) never misreads a v2 message: the
content type differs, so it is rejected with ``ContentDisallowed`` rather than
parsed. That hard boundary is what makes the phases safe to roll out
incrementally.

On-disk legacy records
======================
Messages already persisted on disk in the old format are *not* sniffed by the
new decoder. Read them deliberately through the explicit legacy entry point::

    from kombu.utils.json_safe import loads_legacy
    original = loads_legacy(on_disk_legacy_bytes)

Run it
======
::

    python examples/json_safe_migration/rolling_migration.py
    python examples/json_safe_migration/collision_check.py

The second script pairs structurally different values that the legacy codec
encodes to identical bytes, confirms ``json-v2`` keeps them distinct, and
counts calls to a registered custom decoder: decoding plain dictionaries
never invokes it, while one genuinely tagged object invokes it exactly once.
