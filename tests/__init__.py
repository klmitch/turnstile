import logging

import stubout
import unittest2

from turnstile import config
from turnstile import utils


LOG = logging.getLogger('turnstile')


class TestHandler(logging.Handler, object):
    def __init__(self):
        super(TestHandler, self).__init__(logging.DEBUG)

        self.messages = []

    def emit(self, record):
        try:
            self.messages.append(self.format(record))
        except Exception:
            pass

    def get_messages(self, clear=False):
        # Get the list of messages and clear it
        messages = self.messages
        if clear:
            self.messages = []
        return messages


# Set up basic logging for tests
test_handler = TestHandler()
LOG.addHandler(test_handler)
LOG.setLevel(logging.DEBUG)
LOG.propagate = False


class TestCase(unittest2.TestCase):
    imports = {}

    def setUp(self):
        self.stubs = stubout.StubOutForTesting()

        def fake_import(import_str):
            try:
                return self.imports[import_str]
            except KeyError as exc:
                # Convert into an ImportError
                raise ImportError("Failed to import %s: %s" %
                                  (import_str, exc))

        self.stubs.Set(utils, 'import_class', fake_import)

        # Clear the log messages
        test_handler.get_messages(True)

    def tearDown(self):
        self.stubs.UnsetAll()

        # Clear the log messages
        test_handler.get_messages(True)

    @property
    def log_messages(self):
        # Retrieve and clear test log messages
        return test_handler.get_messages()


class GenericFakeClass(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeConfig(config.Config):
    def __init__(self, conf, db=None):
        conf = dict(('control.%s' % k, v) for k, v in conf.items())
        super(FakeConfig, self).__init__(conf_dict=conf)
        self._db = db

    def get_database(self, override=None):
        return self._db


class FakeMiddleware(object):
    def __init__(self, conf=None):
        self.config = conf or FakeConfig({})

    @property
    def db(self):
        return self.config.get_database()
