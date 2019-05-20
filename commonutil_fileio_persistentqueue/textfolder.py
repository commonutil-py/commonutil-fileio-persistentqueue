# -*- coding: utf-8 -*-
""" Persistent queue on file system """

import os
import fcntl
import logging
from binascii import crc32

_log = logging.getLogger(__name__)

_UNWANT_CHAR = "\n\r\x00"

SERIAL_VALUEMASK = 0xFFFFFF
SERIAL_BOUNDMASK = 0xFFF000
SERIAL_RINGSHIFT = 0x001000


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
	v = v - 1
	for idx in range(1, 16):
		if v >> idx:
			continue
		return idx
	return 16


def cmp_serial(a, b):
	am = a & SERIAL_BOUNDMASK
	bm = b & SERIAL_BOUNDMASK
	if (am == SERIAL_BOUNDMASK) and (bm == 0):
		return -1
	if (bm == SERIAL_BOUNDMASK) and (am == 0):
		return 1
	if a < b:
		return -1
	if a == b:
		return 0
	return 1


def invoke_with_lock(lock_filepath, invoke_callable, *args, **kwds):
	with open(lock_filepath, "w") as lfp:
		fcntl.flock(lfp, fcntl.LOCK_EX)
		v = invoke_callable(*args, **kwds)
		fcntl.flock(lfp, fcntl.LOCK_UN)
	return v


def read_serial(filepath, default_value=None):
	try:
		with open(filepath, "r", encoding="ascii") as fp:
			l = fp.read()
		return int(l.strip())
	except Exception as e:
		_log.warning("failed on read serial (%r): %r", filepath, e)
	return default_value


def write_serial(filepath, v):
	try:
		aux = str(v) + "\n"
		with open(filepath, "w", encoding="ascii") as fp:
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


def update_serial(filepath, n):
	v = read_serial(filepath, None)
	if (v is not None) and (cmp_serial(n, v) <= 0):
		return True
	return write_serial(filepath, n)


def check_skip_record(rec_sn, pick_rec_sn, progress_sn, bound_sn):
	if cmp_serial(rec_sn, bound_sn) > 0:
		return True
	if (progress_sn is not None) and (cmp_serial(rec_sn, progress_sn) <= 0):
		return True
	if (pick_rec_sn is not None) and (cmp_serial(pick_rec_sn, rec_sn) < 0):
		return True
	return False


class PersistentQueueViaTextFolder:
	# pylint: disable=too-many-arguments
	def __init__(self, folder_path, unserializer_callable=None, serializer_callable=None, collection_size=0x40, special_char_replacement=" ", *args, **kwds):
		super().__init__(*args, **kwds)
		self.folder_path = folder_path
		self.unserializer_callable = unserializer_callable
		self.serializer_callable = serializer_callable
		self.collection_size_shift = compute_p2m16(collection_size)
		self.special_char_replacement = special_char_replacement
		self._tip_filepath, self._tip_lockpath = self._make_serial_path_pair("tip")
		self._commit_filepath, self._commit_lockpath = self._make_serial_path_pair("commit")
		self._progress_filepath, self._progress_lockpath = self._make_serial_path_pair("progress")
		if (SERIAL_RINGSHIFT >> self.collection_size_shift) == 0:
			raise ValueError("collection size too large")

	def _make_serial_path_pair(self, name):
		s_path = os.path.join(self.folder_path, name + ".txt")
		l_path = os.path.join(self.folder_path, "." + name + ".lock")
		return (s_path, l_path)

	def _pack(self, serial_val, d):
		aux = self.serializer_callable(d)
		checksum = crc32(aux.encode("utf-8", "ignore")) & 0xFFFFFFFF
		v = (str(serial_val), str(checksum), aux)
		return "\t".join(v) + "\n"

	def _unpack0(self, l):
		rec_sn, remain = l.split("\t", 1)
		rec_sn = int(rec_sn)
		return (rec_sn, remain)

	def _unpack1(self, l):
		checksum, remain = l.split("\t", 1)
		checksum = int(checksum)
		lpkg = remain.rstrip(_UNWANT_CHAR)
		r_cksum = crc32(lpkg.encode("utf-8", "ignore")) & 0xFFFFFFFF
		if r_cksum != checksum:
			raise ValueError("checksum mis-match: %r vs. %r" % (r_cksum, checksum))
		return lpkg

	def _enqueue_impl(self, d):
		tip_sn, update_success = increment_serial(self._tip_filepath)
		if not update_success:
			raise ValueError("cannot update tip serial: %r" % (self._tip_filepath, ))
		page_id = tip_sn >> self.collection_size_shift
		qfname = "q-%d.txt" % (page_id, )
		qfpath = os.path.join(self.folder_path, qfname)
		l = self._pack(tip_sn, d)
		with open(qfpath, "a", encoding="utf-8", errors="ignore") as fp:
			fp.write(l)
		if not invoke_with_lock(self._commit_lockpath, update_serial, self._commit_filepath, tip_sn):
			raise ValueError("failed on updating serial %r into file %r" % (tip_sn, self._commit_filepath))
		return tip_sn

	def enqueue(self, d):
		if not self.serializer_callable:
			raise ValueError("need serializer_callable for enqueue")
		return invoke_with_lock(self._tip_lockpath, self._enqueue_impl, d)

	def cmp_page_id(self, a, b):
		a = a << self.collection_size_shift
		b = b << self.collection_size_shift
		return cmp_serial(a, b)

	def _dequeue_impl_linescan(self, progress_sn, bound_sn, fp, pick_rec_sn, pick_lpkg):
		for l in fp:
			try:
				rec_sn, aux = self._unpack0(l)
			except Exception:
				_log.exception("failed on unpack-0: %r", l)
				continue
			if check_skip_record(rec_sn, pick_rec_sn, progress_sn, bound_sn):
				continue
			try:
				rec_lpkg = self._unpack1(aux)
			except Exception:
				_log.exception("failed on unpack-1 %r", aux)
				continue
			pick_rec_sn = rec_sn
			pick_lpkg = rec_lpkg
		return (pick_rec_sn, pick_lpkg)

	def _dequeue_impl_check_qfile_page_rng(self, min_page_id, bound_page_id, f_page_id, f_name):
		if self.cmp_page_id(f_page_id, bound_page_id) > 0:
			return None
		qfpath = os.path.join(self.folder_path, f_name)
		if (min_page_id is not None) and (self.cmp_page_id(f_page_id, min_page_id) < 0):
			try:
				os.unlink(qfpath)
			except Exception:
				_log.exception("failed on deleting expired queue file: %r", qfpath)
			return None
		return qfpath

	def _dequeue_impl_filescan(self, min_page_id, bound_page_id, progress_sn, bound_sn, f_page_id, f_name, pick_rec_sn, pick_lpkg):
		qfpath = self._dequeue_impl_check_qfile_page_rng(min_page_id, bound_page_id, f_page_id, f_name)
		if qfpath:
			with open(qfpath, "r", encoding="utf-8", errors="ignore") as fp:
				pick_rec_sn, pick_lpkg = self._dequeue_impl_linescan(progress_sn, bound_sn, fp, pick_rec_sn, pick_lpkg)
		return (pick_rec_sn, pick_lpkg)

	def _dequeue_impl_sort_filename(self, filenames):
		result = []
		rotated_mask = SERIAL_BOUNDMASK >> self.collection_size_shift
		rotated_rgft = SERIAL_RINGSHIFT >> self.collection_size_shift
		sort_mode = 0
		for f_name in filenames:
			if f_name[:2] != "q-":
				continue
			try:
				f_page_id = int(f_name[2:-4])
				f_rgft_id = (f_page_id + rotated_rgft) & rotated_mask
			except Exception:
				_log.exception("failed on getting page id from file %r", f_name)
			aux = (f_page_id, f_name, f_rgft_id)
			if (f_page_id & rotated_mask) == rotated_mask:
				sort_mode = 2
			result.append(aux)
		result.sort(key=lambda x: x[sort_mode])
		return result

	def _dequeue_impl(self):
		bound_sn = invoke_with_lock(self._commit_lockpath, read_serial, self._commit_filepath, None)
		if bound_sn is None:
			return None
		progress_sn = read_serial(self._progress_filepath)
		min_page_id = (progress_sn >> self.collection_size_shift) if (progress_sn is not None) else None
		bound_page_id = bound_sn >> self.collection_size_shift
		fl = os.listdir(self.folder_path)
		pick_rec_sn = None
		pick_lpkg = None
		fl = self._dequeue_impl_sort_filename(fl)
		for f_page_id, f_name, _f_rgft_id in fl:
			pick_rec_sn, pick_lpkg = self._dequeue_impl_filescan(min_page_id, bound_page_id, progress_sn, bound_sn, f_page_id, f_name, pick_rec_sn, pick_lpkg)
			if pick_rec_sn is not None:
				break
		if pick_rec_sn is None:
			return None
		if not write_serial(self._progress_filepath, pick_rec_sn):
			raise ValueError("cannot write progress serial %r" % (pick_rec_sn, ))
		return None if (pick_lpkg is None) else self.unserializer_callable(pick_lpkg)

	def dequeue(self):
		if not self.unserializer_callable:
			raise ValueError("need unserializer_callable for dequeue")
		return invoke_with_lock(self._progress_lockpath, self._dequeue_impl)
