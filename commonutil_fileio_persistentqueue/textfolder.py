# -*- coding: utf-8 -*-
""" Persistent queue on file system """

import os
import io
import fcntl
import logging

_log = logging.getLogger(__name__)

_UNWANT_CHAR = "\n\r\x00"

SERIAL_VALUEMASK = 0xFFFFFF
SERIAL_BOUNDMASK = 0xFFF000


def sanitize(v, replace_char=None):
	if replace_char is None:
		for c in _UNWANT_CHAR:
			if c in v:
				raise ValueError("found unwanted character %r in serialized string: %r" % (
						c,
						v,
				))
		return v
	return ''.join([replace_char if (c in _UNWANT_CHAR) else c for c in v])


def compute_p2m16(v):
	for idx in xrange(1, 16):
		if v >> idx:
			continue
		return idx
	return 16


def invoke_with_lock(lock_filepath, invoke_callable, *args, **kwds):
	with io.open(lock_filepath, "w") as lfp:
		fcntl.flock(lfp, fcntl.LOCK_EX)
		v = invoke_callable(*args, **kwds)
		fcntl.flock(lfp, fcntl.LOCK_UN)
	return v


def read_serial(filepath, default_value=None):
	try:
		with io.open(filepath, "r") as fp:
			l = fp.read()
		return int(l.strip())
	except Exception as e:
		_log.warning("failed on read serial (%r): %r", filepath, e)
	return default_value


def write_serial(filepath, v):
	try:
		aux = str(v) + "\n"
		with io.open(filepath, "w") as fp:
			fp.write(aux)
		return True
	except Exception:
		_log.exception("failed on write head serial (%r)", filepath)
	return False


def increment_serial(filepath):
	v = read_serial(filepath, 0)
	n = (v + 1) & SERIAL_VALUEMASK
	update_success = write_serial(filepath, n)
	return (v, update_success)


class PersistentQueueViaTextFolder(object):
	# pylint: disable=too-many-arguments
	def __init__(self, folder_path, unserializer_callable=None, serializer_callable=None, collection_size=0x40, special_char_replacement=" ", *args, **kwds):
		super(PersistentQueueViaTextFolder, self).__init__(*args, **kwds)
		self.folder_path = folder_path
		self.unserializer_callable = unserializer_callable
		self.serializer_callable = serializer_callable
		self.collection_size_shift = compute_p2m16(collection_size)
		self.special_char_replacement = special_char_replacement
		self._tip_filepath, self._tip_lockpath = self._make_serial_path_pair("tip")
		self._commit_filepath, self._commit_lockpath = self._make_serial_path_pair("commit")
		self._progress_filepath, self._progress_lockpath = self._make_serial_path_pair("progress")

	def _make_serial_path_pair(self, name):
		s_path = os.path.join(self.folder_path, name + ".txt")
		l_path = os.path.join(self.folder_path, "." + name + ".lock")
		return (s_path, l_path)
