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


class TestToBool(tests.TestCase):
    def test_integers(self):
        self.assertEqual(utils.to_bool('0'), False)
        self.assertEqual(utils.to_bool('1'), True)
        self.assertEqual(utils.to_bool('123412341234'), True)

    def test_true(self):
        self.assertEqual(utils.to_bool('t'), True)
        self.assertEqual(utils.to_bool('true'), True)
        self.assertEqual(utils.to_bool('on'), True)
        self.assertEqual(utils.to_bool('y'), True)
        self.assertEqual(utils.to_bool('yes'), True)

    def test_false(self):
        self.assertEqual(utils.to_bool('f'), False)
        self.assertEqual(utils.to_bool('false'), False)
        self.assertEqual(utils.to_bool('off'), False)
        self.assertEqual(utils.to_bool('n'), False)
        self.assertEqual(utils.to_bool('no'), False)

    def test_invalid(self):
        self.assertRaises(ValueError, utils.to_bool, 'invalid')

    def test_invalid_noraise(self):
        self.assertEqual(utils.to_bool('invalid', False), False)
