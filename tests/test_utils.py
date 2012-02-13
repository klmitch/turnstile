from turnstile import utils

import tests


def throw_except(exc_type):
    if exc_type:
        raise exc_type('Thrown')


class TestIgnoreExcept(tests.TestCase):
    def test_ignore_except_no_error(self):
        # Shouldn't raise any exceptions
        finished = False
        with utils.ignore_except():
            throw_except(None)
            finished = True

        self.assertEqual(finished, True)

    def test_ignore_except_with_error(self):
        # Should raise an exception which should be ignored
        finished = False
        with utils.ignore_except():
            throw_except(Exception)
            finished = True

        # Yes, this should be False here...
        self.assertEqual(finished, False)
