class EmptyObject:
    pass


EMPTY = EmptyObject()  # sentinel object to represent empty node output


def is_empty(obj):
    return isinstance(obj, EmptyObject)
