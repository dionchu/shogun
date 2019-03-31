from six import iteritems, string_types, PY3
from operator import attrgetter
from shogun.utils.preprocess import call, preprocess
from toolz import valmap, complement, compose

if PY3:
    _qualified_name = attrgetter('__qualname__')
else:
    def _qualified_name(obj):
        """
        Return the fully-qualified name (ignoring inner classes) of a type.
        """
        # If the obj has an explicitly-set __qualname__, use it.
        try:
            return getattr(obj, '__qualname__')
        except AttributeError:
            pass

        # If not, build our own __qualname__ as best we can.
        module = obj.__module__
        if module in ('__builtin__', '__main__', 'builtins'):
            return obj.__name__
        return '.'.join([module, obj.__name__])

def expect_types(__funcname=_qualified_name, **named):
    """
    Preprocessing decorator that verifies inputs have expected types.
    Examples
    --------
    >>> @expect_types(x=int, y=str)
    ... def foo(x, y):
    ...    return x, y
    ...
    >>> foo(2, '3')
    (2, '3')
    >>> foo(2.0, '3')  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    TypeError: ...foo() expected a value of type int for argument 'x',
    but got float instead.
    Notes
    -----
    A special argument, __funcname, can be provided as a string to override the
    function name shown in error messages.  This is most often used on __init__
    or __new__ methods to make errors refer to the class name instead of the
    function name.
    """
    for name, type_ in iteritems(named):
        if not isinstance(type_, (type, tuple)):
            raise TypeError(
                "expect_types() expected a type or tuple of types for "
                "argument '{name}', but got {type_} instead.".format(
                    name=name, type_=type_,
                )
            )

    def _expect_type(type_):
        # Slightly different messages for type and tuple of types.
        _template = (
            "%(funcname)s() expected a value of type {type_or_types} "
            "for argument '%(argname)s', but got %(actual)s instead."
        )
        if isinstance(type_, tuple):
            template = _template.format(
                type_or_types=' or '.join(map(_qualified_name, type_))
            )
        else:
            template = _template.format(type_or_types=_qualified_name(type_))

        return make_check(
            exc_type=TypeError,
            template=template,
            pred=lambda v: not isinstance(v, type_),
            actual=compose(_qualified_name, type),
            funcname=__funcname,
        )

    return preprocess(**valmap(_expect_type, named))

def make_check(exc_type, template, pred, actual, funcname):
    """
    Factory for making preprocessing functions that check a predicate on the
    input value.
    Parameters
    ----------
    exc_type : Exception
        The exception type to raise if the predicate fails.
    template : str
        A template string to use to create error messages.
        Should have %-style named template parameters for 'funcname',
        'argname', and 'actual'.
    pred : function[object -> bool]
        A function to call on the argument being preprocessed.  If the
        predicate returns `True`, we raise an instance of `exc_type`.
    actual : function[object -> object]
        A function to call on bad values to produce the value to display in the
        error message.
    funcname : str or callable
        Name to use in error messages, or function to call on decorated
        functions to produce a name.  Passing an explicit name is useful when
        creating checks for __init__ or __new__ methods when you want the error
        to refer to the class name instead of the method name.
    """
    if isinstance(funcname, str):
        def get_funcname(_):
            return funcname
    else:
        get_funcname = funcname

    def _check(func, argname, argvalue):
        if pred(argvalue):
            raise exc_type(
                template % {
                    'funcname': get_funcname(func),
                    'argname': argname,
                    'actual': actual(argvalue),
                },
            )
        return argvalue
    return _check
