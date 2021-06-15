from datetime import timedelta


class Literal(object):
    """String wrapper to make a query parameter literal."""

    __slots__ = "s",

    def __init__(self, s):
        self.s = str(s).encode('utf-8')

    def __conform__(self, quote):
        return self

    def __str__(self):
        return self.s.decode('utf-8')

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return self.s


def utc_format(dt):
    dt = dt.replace(tzinfo=None) - (
        dt.tzinfo.utcoffset(dt) if dt.tzinfo else timedelta()
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
