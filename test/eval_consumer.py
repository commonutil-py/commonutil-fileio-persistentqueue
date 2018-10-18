# -*- coding: utf-8 -*-
""" Consumer side of test program """

import sys
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
	last_fetches = {}
	display_t = time.time() * 10
	display_p = "-"
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
		p_id = v["producer_id"]
		sn = v["value"]
		last_fetch = last_fetches.get(p_id)
		if last_fetch is not None:
			if (last_fetch + 1) != sn:
				sys.stdout.write("\033[2K\r[\033[93mERR\033[0m]: serial not continue: [%r - %r](%r)\n" % (last_fetch, sn, p_id))
		current_t = time.time() * 10
		t_dq = current_t - display_t
		if (t_dq > 5) or ((t_dq > 2) and (display_p != p_id)):
			sys.stdout.write("\033[2K\rdequeue: (%r) %r (took %f)." % (
					p_id,
					sn,
					(t_dq_e - t_dq_s),
			))
			sys.stdout.flush()
			display_t = current_t
			display_p = p_id
		last_fetches[p_id] = sn


if __name__ == '__main__':
	main()
