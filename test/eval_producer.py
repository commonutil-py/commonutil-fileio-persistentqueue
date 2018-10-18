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

_log = logging.getLogger(__name__)


def main():
	logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
	try:
		producer_id = sys.argv[1]
	except Exception:
		producer_id = datetime.datetime.now().strftime(r"P%d%H%M%S")
	try:
		os.makedirs(_QFOLDER)
	except Exception as e:
		_log.warning("caught exception on making queue folder: %r", e)
	_log.info("queue folder: [%s]", _QFOLDER)
	_log.info("producer_id: [%s]", producer_id)
	pq = PersistentQueueViaTextFolder(_QFOLDER, serializer_callable=json_dumps)
	for idx in xrange(8589934592):
		aux = {
				"producer_id": producer_id,
				"value": idx,
		}
		pq.enqueue(aux)
		if (idx & 0xFFF) == 0xFFF:
			sys.stdout.write("\033[2K\renqueue: (%r) %r." % (
					producer_id,
					idx,
			))
			sys.stdout.flush()
			time.sleep(3)
	sys.stdout.write("\nExit.\n")


if __name__ == '__main__':
	main()
