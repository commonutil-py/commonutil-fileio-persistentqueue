# -*- coding: utf-8 -*-
""" Consumer side of test program """

import sys
import os
import datetime
import time
from json import dumps as json_dumps
import logging

from commonutil_fileio_persistentqueue.textfolder import PersistentQueueViaTextFolder

_QFOLDER = "/tmp/persistq-test-1"
RANGE_END = 8589934592

_log = logging.getLogger(__name__)


def get_options():
	rng_start = 0
	producer_id = datetime.datetime.now().strftime(r"P%d%H%M%S")
	for arg in sys.argv[1:]:
		try:
			aux = int(arg)
			if aux < RANGE_END:
				rng_start = aux
				continue
		except Exception:
			pass
		producer_id = arg
	return (rng_start, producer_id)


def main():
	logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
	rng_start, producer_id = get_options()
	try:
		os.makedirs(_QFOLDER)
	except Exception as e:
		_log.warning("caught exception on making queue folder: %r", e)
	_log.info("queue folder: [%s]", _QFOLDER)
	_log.info("producer_id: [%s], range-start: %d.", producer_id, rng_start)
	pq = PersistentQueueViaTextFolder(_QFOLDER, serializer_callable=json_dumps)
	for idx in range(rng_start, RANGE_END):
		aux = {
				"producer_id": producer_id,
				"value": idx,
		}
		t_eq_s = time.time()
		pq.enqueue(aux)
		t_eq_e = time.time()
		if (idx & 0x7FF) == 0x7FF:
			sys.stdout.write("\033[2K\renqueue: (%r) %r (took %f)." % (
					producer_id,
					idx,
					(t_eq_e - t_eq_s),
			))
			sys.stdout.flush()
			time.sleep(2)
	sys.stdout.write("\nExit.\n")


if __name__ == '__main__':
	main()
