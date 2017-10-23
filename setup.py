#!/usr/bin/env python
import os
import sys

from pyredis import __version__

try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand

    class PyTest(TestCommand):
        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_args = []
            self.test_suite = True

        def run_tests(self):
            # import here, because outside the eggs aren't loaded
            import pytest
            errno = pytest.main(self.test_args)
            sys.exit(errno)

except ImportError:

    from distutils.core import setup

    def PyTest(x):
        x

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='pythonic_redis',
    version=__version__,
    description='Pythonic client for Redis key-value store',
    long_description=long_description,
    url='http://github.com/ke4roh/pythonic_redis',
    author='Jim Scarborough',
    author_email='jimes@hiwaay.net',
    maintainer='Jim Scarborough',
    maintainer_email='jimes@hiwaay.net',
    keywords=['Redis', 'key-value store'],
    license='MIT',
    packages=['pyredis'],
    extras_require={
        'redis': [
            "redis>=2.10.0",
        ],
    },
    tests_require=[
        'mock',
        'pytest>=2.5.0',
    ],
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)