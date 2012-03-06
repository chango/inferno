from functools import wraps


def lazy_property(f):
    @property
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        lazy = '_lazy_' + f.__name__
        try:
            return getattr(self, lazy)
        except AttributeError:
            result = f(self, *args, **kwargs)
            setattr(self, lazy, result)
            return result
    return wrapper
