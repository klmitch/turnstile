import sys


def import_class(import_str):
    """Returns a class from a string including module and class."""

    mod_str, _sep, class_str = import_str.rpartition(':')
    try:
        __import__(mod_str)
        return getattr(sys.modules[mod_str], class_str)
    except (ImportError, ValueError, AttributeError) as exc:
        # Convert it into an import error
        raise ImportError("Failed to import %s: %s" % (import_str, exc))


class ignore_except(object):
    """Context manager to ignore all exceptions."""

    def __enter__(self):
        """Entry does nothing."""

        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Return True to mark the exception as handled."""

        return True
