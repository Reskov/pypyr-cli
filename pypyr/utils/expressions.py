"""Utility functions for evaluating expressions."""


def eval_string(input_string, globals, locals):
    """Dynamically evaluates the input_string expression.

    This provides dynamic python eval of an input expression. The return is
    whatever the result of the expression is.

    Use with caution: since input_string executes any arbitrary code object,
    the potential for damage is great.

    For reasons best known to eval(), it EAFPs the locals() look-up 1st and if
    it raises a KeyNotFound error, moves on to globals.

    This means if you pass in a custom dict type for locals that does not
    raise a KeyNotFound or derived exception,  and thus eval doesn't work.

    Therefore, in order to be used as the locals arg, if you use a dict with
    a customized exception when look-up fails that is not an instance of
    KeyNotFound, you should initialize to a new dict like this:
        out = eval_string('1==1', dict(mydict))

    Args:
        input_string: str. expression to evaluate.
        locals: dict-like. Mapping object containing scope for eval.

    Returns:
        Whatever object results from the string expression valuation.

    """
    # empty globals arg will append __builtins__ by default
    return eval(input_string, globals, locals)
