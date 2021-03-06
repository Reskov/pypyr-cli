"""moduleloader.py unit tests."""
from pathlib import Path
import sys

import pytest

from pypyr.errors import PyModuleNotFoundError
import pypyr.moduleloader as moduleloader
# from pypyr.cache.namespacecache import

# region ImportVisitor


def test_import_visitor_empty():
    """Empty source parses to empty namespace."""
    assert moduleloader.ImportVisitor().get_namespace('') == {}


def test_import_visitor():
    """Import visitor parses python import/import from syntax variations.

    Do NOT import tests.arbpack.arbmod4 anywhere else in the code.
    """
    source = """\
import operator
import itertools as itools

import urllib.parse
import tests.arbpack.arbmod as z

# from mod import submod
from tests.arbpack import arbmod2
from tests.arbpack import arbmod3 as ab3
from tests.arbpack import arbmod4_avoid

# from mod import attr
from decimal import Decimal
from fractions import Fraction as myfraction

from math import ceil, floor
from tests.arbpack.arbmultiattr import arb_attr as x, arb_func as y
"""

    visitor = moduleloader.ImportVisitor()
    ns = visitor.get_namespace(source)

    assert len(ns) == 13

    exec_me = """\
preexisting = 'updated'
arb = len('hello')

modded = operator.mod(6, 4)
prod = list(itools.product('AB', 'ab'))
urlhost = urllib.parse.urlparse('http://arbhost/blah').netloc
z.arbmod_attribute()
ab2 = arbmod2.arb_func_in_arbmod2('ab2 value')
ab3_out = ab3.arb_func_in_arbmod3(123)
ab4 = arbmod4_avoid.arb_func_in_arbmod4(True)
dec = int(Decimal(4).sqrt())
frac = myfraction(1, 3).denominator

ceiling = ceil(6.1)
thefloor = floor(6.1)
arb_attr_out = x
func_res = y('test me')
    """

    locals = {'preexisting': 'initial value'}
    exec(exec_me, ns, locals)

    assert locals['preexisting'] == 'updated'
    assert locals['arb'] == 5
    assert locals['modded'] == 2
    assert locals['prod'] == [('A', 'a'), ('A', 'b'), ('B', 'a'), ('B', 'b')]
    assert locals['urlhost'] == 'arbhost'
    assert locals['ab2'] == 'ab2 value'
    assert locals['ab4'] is True
    assert locals['ab3_out'] == 123
    assert locals['dec'] == 2
    assert locals['frac'] == 3
    assert locals['ceiling'] == 7
    assert locals['thefloor'] == 6
    assert locals['arb_attr_out'] == 123.456
    assert locals['func_res'] == 'test me'


def test_import_visitor_repeating_parent():
    """Repeating parent only shows up 1x in namespace."""
    source = """\
import tests.arbpack.arbmod
import tests.arbpack.arbmod2
import tests.arbpack.arbmod3
"""

    ns = moduleloader.ImportVisitor().get_namespace(source)

    assert len(ns) == 1
    # just see it passes, no return value to assert
    ns['tests'].arbpack.arbmod.arbmod_attribute()
    assert ns['tests'].arbpack.arbmod2.arb_func_in_arbmod2('ab2') == 'ab2'
    assert ns['tests'].arbpack.arbmod3.arb_func_in_arbmod3('ab3') == 'ab3'

    # parent did not import anything NOT specified.
    # tests.arbpack.arbstep exists but wasn't specified for import.
    assert not hasattr(ns['tests'].arbpack, 'arbstep')


def test_import_visitor_relative_raises():
    """Relative imports not supported."""
    source = "from .errors_test import arbthing"

    visitor = moduleloader.ImportVisitor()
    with pytest.raises(TypeError) as err:
        visitor.get_namespace(source)

    assert str(err.value) == ("you can't use relative imports here. use "
                              "absolute imports instead.")

# endregion ImportVisitor

# region get_module


def test_get_module_raises():
    """On get_module ModuleNotFoundError on module not found."""
    with pytest.raises(PyModuleNotFoundError) as err:
        moduleloader.get_module('unlikelyblahmodulenameherexxssz')

    assert str(err.value) == (
        "unlikelyblahmodulenameherexxssz.py should be in your working "
        "dir or it should be installed to the python path."
        "\nIf you have 'package.sub.mod' your current working "
        "dir should contain ./package/sub/mod.py\n"
        "If you specified 'mymodulename', your current "
        "working dir should contain ./mymodulename.py\n"
        "If the module is not in your current working dir, it "
        "must exist in your current python path - so you "
        "should have run pip install or setup.py")


def test_get_module_raises_compatible_error():
    """get_module should raise error compatible with ModuleNotFoundError."""
    with pytest.raises(ModuleNotFoundError):
        moduleloader.get_module('unlikelyblahmodulenameherexxssz')


def test_get_module_raises_friendly_on_package_import():
    """get_module should not obscure missing module in existing package."""
    p = Path.cwd().joinpath('tests')
    moduleloader.set_working_directory(p)

    with pytest.raises(PyModuleNotFoundError) as err:
        moduleloader.get_module('arbpack.idontexist')

    assert str(err.value) == (
        "arbpack.idontexist.py should be in your working "
        "dir or it should be installed to the python path."
        "\nIf you have 'package.sub.mod' your current working "
        "dir should contain ./package/sub/mod.py\n"
        "If you specified 'mymodulename', your current "
        "working dir should contain ./mymodulename.py\n"
        "If the module is not in your current working dir, it "
        "must exist in your current python path - so you "
        "should have run pip install or setup.py")

    sys.path.remove(str(p))


def test_get_module_raises_on_inner_import():
    """get_module should not hide failing import statements in imported mod."""
    p = Path.cwd().joinpath('tests')
    moduleloader.set_working_directory(p)

    with pytest.raises(PyModuleNotFoundError) as err:
        moduleloader.get_module('arbpack.arbinvalidimportmod')

    assert str(err.value) == (
        'error importing module blahblah in arbpack.arbinvalidimportmod')

    sys.path.remove(str(p))


def test_get_module_pass():
    """Pass when get_module finds a module in cwd."""
    p = Path.cwd().joinpath('tests', 'testfiles')
    moduleloader.set_working_directory(p)

    arb_module = moduleloader.get_module('arb')

    assert arb_module
    assert arb_module.__name__ == 'arb'
    assert hasattr(arb_module, 'arb_attribute')

    sys.path.remove(str(p))


def test_get_module_in_package_pass():
    """See get_module find a module in a package in cwd using dot notation."""
    p = Path.cwd().joinpath('tests')
    moduleloader.set_working_directory(p)
    arb_module = moduleloader.get_module('arbpack.arbmod')

    assert arb_module
    assert arb_module.__name__ == 'arbpack.arbmod'
    assert hasattr(arb_module, 'arbmod_attribute')

    sys.path.remove(str(p))

# endregion get_module

# region WorkingDir


def test_working_dir_set_default():
    """Set working dir to cwd if not specified."""
    w = moduleloader.WorkingDir()
    w.set_working_directory()

    cwd = Path.cwd()
    assert w.get_working_directory() == cwd

    cwd = str(cwd)
    assert cwd in sys.path
    sys.path.remove(cwd)


def test_working_dir_set_explicit_none():
    """Set working dir to cwd if None."""
    w = moduleloader.WorkingDir()
    w.set_working_directory(None)

    cwd = Path.cwd()
    assert w.get_working_directory() == cwd
    cwd = str(cwd)
    assert cwd in sys.path
    sys.path.remove(cwd)


def test_working_dir_get_before_set():
    """Get working dir before set raises."""
    with pytest.raises(ValueError) as err:
        w = moduleloader.WorkingDir()
        w.get_working_directory()

    assert str(err.value) == 'working directory not set.'


def test_set_working_dir():
    """Working dir added to sys paths."""
    p = '/arb/path'
    assert p not in sys.path
    moduleloader.set_working_directory(p)
    assert p in sys.path
    sys.path.remove(p)

# endregion WorkingDir
