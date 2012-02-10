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
