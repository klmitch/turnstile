#!/usr/bin/env python

import os

from setuptools import setup


def readreq(filename):
    result = []
    with open(filename) as f:
        for req in f:
            req = req.partition('#')[0].strip()
            if not req:
                continue
            result.append(req)
    return result


def readfile(filename):
    with open(filename) as f:
        return f.read()


setup(
    name='turnstile',
    version='0.7.0b1',
    author='Kevin L. Mitchell',
    author_email='kevin.mitchell@rackspace.com',
    url='https://github.com/klmitch/turnstile',
    description="Distributed rate-limiting middleware",
    long_description=readfile('README.rst'),
    license='Apache License (2.0)',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Paste',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
    ],
    packages=['turnstile'],
    install_requires=readreq('.requires'),
    tests_require=readreq('.test-requires'),
    entry_points={
        'paste.filter_factory': [
            'turnstile = turnstile.middleware:turnstile_filter',
        ],
        'console_scripts': [
            'setup_limits = turnstile.tools:setup_limits.console',
            'dump_limits = turnstile.tools:dump_limits.console',
            'remote_daemon = turnstile.tools:remote_daemon.console',
            'turnstile_command = turnstile.tools:turnstile_command.console',
            'compactor_daemon = turnstile.tools:compactor.console',
        ],
        'turnstile.limit': [
            'limit = turnstile.limits:Limit',
        ],
        'turnstile.middleware': [
            'turnstile = turnstile.middleware:TurnstileMiddleware',
        ],
    },
)
