import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='turnstile',
    version='0.6.1',
    author='Kevin L. Mitchell',
    author_email='kevin.mitchell@rackspace.com',
    description="Distributed rate-limiting middleware",
    license='Apache License (2.0)',
    packages=['turnstile'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Paste',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
        ],
    url='https://github.com/klmitch/turnstile',
    long_description=read('README.rst'),
    entry_points={
        'paste.filter_factory': [
            'turnstile = turnstile.middleware:turnstile_filter',
            ],
        'console_scripts': [
            'setup_limits = turnstile.tools:setup_limits',
            'dump_limits = turnstile.tools:dump_limits',
            'remote_daemon = turnstile.tools:remote_daemon',
            ],
        },
    install_requires=[
        'argparse',
        'eventlet',
        'lxml>=2.3',
        'metatools',
        'msgpack-python',
        'redis',
        'routes',
        ],
    tests_require=[
        'mox',
        'nose',
        'unittest2>=0.5.1',
        ],
    )
