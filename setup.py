#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup  # pylint: disable=import-error

setup(
		name="commonutil-fileio-persistentqueue",
		version="3.0.2",  # REV-CONSTANT:rev 5d022db7d38f580a850cd995e26a6c2f
		description="Persistent queue on file system",
		packages=[
				"commonutil_fileio_persistentqueue",
		],
		classifiers=[
				"Development Status :: 5 - Production/Stable",
				"Intended Audience :: Developers",
				"License :: OSI Approved :: MIT License",
				"Operating System :: POSIX",
				"Programming Language :: Python :: 3.6",
				"Programming Language :: Python :: 3.7",
		],
		license="MIT License",
)
