from six import iteritems

def invert(d):
    """
    Invert a dictionary into a dictionary of sets.
    >>> invert({'a': 1, 'b': 2, 'c': 1})  # doctest: +SKIP
    {1: {'a', 'c'}, 2: {'b'}}
    """
    out = {}
    for k, v in iteritems(d):
        try:
            out[v].add(k)
        except KeyError:
            out[v] = {k}
    return out
