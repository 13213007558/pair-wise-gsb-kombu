from __future__ import annotations

import sys
from unittest.mock import Mock, patch

import pytest

from kombu.exceptions import ContentDisallowed
from kombu.message import Message


class test_Message:

    def test_repr(self):
        assert repr(Message('b', channel=Mock()))

    def test_decode(self):
        m = Message('body', channel=Mock())
        decode = m._decode = Mock()
        assert m._decoded_cache is None
        assert m.decode() is m._decode.return_value
        assert m._decoded_cache is m._decode.return_value
        m._decode.assert_called_with()
        m._decode = Mock()
        assert m.decode() is decode.return_value

    def test_reraise_error(self):
        m = Message('body', channel=Mock())
        callback = Mock(name='callback')
        try:
            raise KeyError('foo')
        except KeyError:
            m.errors.append(sys.exc_info())
        m._reraise_error(callback)
        callback.assert_called()

        with pytest.raises(KeyError):
            m._reraise_error(None)

    @patch('kombu.message.decompress')
    def test_decompression_stores_error(self, decompress):
        decompress.side_effect = RuntimeError()
        m = Message('body', channel=Mock(), headers={'compression': 'zlib'})
        with pytest.raises(RuntimeError):
            m._reraise_error(None)

    @patch('kombu.message.decompress')
    def test_strict_defers_decompression_until_payload_access(
            self, decompress):
        decompress.return_value = b'decompressed'
        m = Message(
            b'compressed',
            channel=Mock(),
            headers={'compression': 'zlib'},
            content_type='application/json',
            accept={'application/json'},
            strict=True,
        )
        decompress.assert_not_called()
        assert m.body == b'compressed'
        assert m._decompressed is False

        with patch('kombu.message.loads') as loads:
            m.payload
        decompress.assert_called_once_with(b'compressed', 'zlib')
        loads.assert_called_once_with(
            b'decompressed', 'application/json', None,
            accept={'application/json'}, strict=True)

    @patch('kombu.message.decompress')
    def test_strict_rejected_content_never_decompresses(self, decompress):
        m = Message(
            b'compressed',
            channel=Mock(),
            headers={'compression': 'zlib'},
            content_type='application/x-python-serialize',
            accept={'application/json'},
            strict=True,
        )
        with pytest.raises(ContentDisallowed):
            m._check_accept()
        decompress.assert_not_called()

    @patch('kombu.message.decompress')
    def test_strict_payload_access_is_memoized(self, decompress):
        decompress.return_value = b'decompressed'
        m = Message(
            b'compressed',
            channel=Mock(),
            headers={'compression': 'zlib'},
            content_type='application/json',
            accept={'application/json'},
            strict=True,
        )
        with patch('kombu.message.loads') as loads:
            m.payload
            m.payload
            m.decode()
        assert decompress.call_count == 1
        assert loads.call_count == 1
