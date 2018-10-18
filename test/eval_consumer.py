# -*- coding: utf-8 -*-
""" Consumer side of test program """

import sys
import os
import datetime
import time
from json import loads as json_loads
import logging

from commonutil_fileio_persistentqueue.textfolder import PersistentQueueViaTextFolder

_QFOLDER = "/tmp/persistq-test-1"

_log = logging.getLogger(__name__)


def main():
	logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
	_log.info("queue folder: [%s]", _QFOLDER)
	pq = PersistentQueueViaTextFolder(_QFOLDER, unserializer_callable=json_loads)
	miss_dequeue = 0
	last_fetch = None
	display_t = time.time() * 10
	while True:
		t_dq_s = time.time()
		v = pq.dequeue()
		t_dq_e = time.time()
		if v is None:
			miss_dequeue = miss_dequeue + 1
			if miss_dequeue > 3:
				miss_dequeue = 0
				time.sleep(3)
			continue
		sn = v["value"]
		if last_fetch is not None:
			if (last_fetch + 1) != sn:
				sys.stdout.write("\033[2K\r[\033[93mERR\033[0m]: serial not continue: [%r - %r]\n" % (last_fetch, sn))
		current_t = time.time() * 10
		if current_t - display_t > 2:
			sys.stdout.write("\033[2K\rdequeue: (%r) %r (took %f)." % (
					v["producer_id"],
					sn,
					(t_dq_e - t_dq_s),
			))
			sys.stdout.flush()
			display_t = current_t
		last_fetch = sn


if __name__ == '__main__':
	main()
