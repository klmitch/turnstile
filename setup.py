import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='turnstile',
    version='0.1',
    author='Kevin L. Mitchell',
    author_email='kevin.mitchell@rackspace.com',
    description="Distributed rate-limiting middleware",
    license='',
    packages=['turnstile'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Paste',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
        ],
    url='https://github.com/klmitch/turnstile',
    long_description=read('README.rst'),
    scripts=['bin/setup_limits', 'bin/dump_limits'],
    install_requires=[
        'argparse',
        'eventlet',
        'lxml',
        'metatools',
        'msgpack',
        'redis',
        'routes',
        ],
    tests_require=[
        'stubout',
        ],
    )
