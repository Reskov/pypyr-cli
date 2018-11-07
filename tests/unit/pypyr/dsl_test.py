"""dsl.py unit tests."""
from copy import deepcopy
import logging
import pytest
from asynctest.mock import call, patch, CoroutineMock
from pypyr.context import Context
from pypyr.dsl import Step, WhileDecorator
from pypyr.errors import PipelineDefinitionError, LoopMaxExhaustedError

pytestmark = pytest.mark.asyncio


class DeepCopyCoroutineMock(CoroutineMock):
    """Derive a new CoroutineMock doing a deepcopy of args to calls.

    MagicMocks store a reference to a mutable object - so on multiple calls to
    the mock the call history isn't maintained if the same obj mutates as an
    arg to those calls. https://bugs.python.org/issue33667

    It's probably not sensible to deepcopy all mock calls. So this little class
    is for patching the MagicMock class specifically, where it will do the
    deepcopy only where specifically patched.

    See here:
    https://docs.python.org/3/library/unittest.mock-examples.html#coping-with-mutable-arguments
    """

    def __call__(self, *args, **kwargs):
        return super(DeepCopyCoroutineMock, self).__call__(*deepcopy(args),
                                                           **deepcopy(kwargs))


# ------------------- test context -------------------------------------------#


def get_test_context():
    """Return a pypyr context for testing."""
    return Context({
        'key1': 'value1',
        'key2': 'value2',
        'key3': 'value3',
        'key4': [
            {'k4lk1': 'value4',
             'k4lk2': 'value5'},
            {'k4lk1': 'value6',
             'k4lk2': 'value7'}
        ],
        'key5': False,
        'key6': True,
        'key7': 77
    })


# ------------------- test context -------------------------------------------#

# ------------------- step mocks ---------------------------------------------#


def mock_run_step(context):
    """Arbitrary mock function to execute instead of run_step"""
    context['test_run_step'] = 'this was set in step'


def mock_run_step_empty_context(context):
    """Clear the context in the step."""
    context.clear()


def mock_run_step_none_context(context):
    """None the context in the step"""
    # ignore the context is not used flake8 warning
    context = None  # noqa: F841


# ------------------- step mocks ---------------------------------------------#

# ------------------- Step----------------------------------------------------#
# ------------------- Step: init ---------------------------------------------#


@patch('pypyr.moduleloader.get_module', return_value='iamamodule')
async def test_simple_step_init_defaults(mocked_moduleloader):
    """Simple step initializes with defaults as expected."""
    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        step = Step('blah')

    mock_logger_debug.assert_any_call("blah is a simple string.")

    assert step.name == 'blah'
    assert step.module == 'iamamodule'
    assert step.foreach_items is None
    assert step.in_parameters is None
    assert step.run_me
    assert not step.skip_me
    assert not step.swallow_me
    assert not step.while_decorator

    mocked_moduleloader.assert_called_once_with('blah')


@patch('pypyr.moduleloader.get_module', return_value='iamamodule')
async def test_complex_step_init_defaults(mocked_moduleloader):
    """Complex step initializes with defaults as expected."""
    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        step = Step({'name': 'blah'})

    mock_logger_debug.assert_any_call("blah is complex.")

    assert step.name == 'blah'
    assert step.module == 'iamamodule'
    assert step.foreach_items is None
    assert step.in_parameters is None
    assert step.run_me
    assert not step.skip_me
    assert not step.swallow_me
    assert not step.while_decorator

    mocked_moduleloader.assert_called_once_with('blah')


@patch('pypyr.moduleloader.get_module', return_value='iamamodule')
async def test_complex_step_init_with_decorators(mocked_moduleloader):
    """Complex step initializes with decorators set."""
    step = Step({'name': 'blah',
                 'in': {'k1': 'v1', 'k2': 'v2'},
                 'foreach': [0],
                 'run': False,
                 'skip': True,
                 'swallow': True,
                 'while': {'stop': 'stop condition',
                           'errorOnMax': True,
                           'sleep': 3,
                           'max': 4}
                 })
    assert step.name == 'blah'
    assert step.module == 'iamamodule'
    assert step.foreach_items == [0]
    assert step.in_parameters == {'k1': 'v1', 'k2': 'v2'}
    assert not step.run_me
    assert step.skip_me
    assert step.swallow_me
    assert step.while_decorator.stop == 'stop condition'
    assert step.while_decorator.error_on_max
    assert step.while_decorator.sleep == 3
    assert step.while_decorator.max == 4

    mocked_moduleloader.assert_called_once_with('blah')


# ------------------- Step: init ---------------------------------------------#

# ------------------- Step: run_step: foreach --------------------------------#


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'run_conditional_decorators')
@patch.object(Step, 'foreach_loop')
async def test_foreach_none(mock_foreach, mock_run, mock_moduleloader):
    """Simple step with None foreach decorator doesn't loop."""
    step = Step('step1')

    context = get_test_context()
    original_len = len(context)

    await step.run_step(context)

    mock_foreach.assert_not_called()

    mock_run.assert_called_once_with(get_test_context())

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'run_conditional_decorators')
@patch.object(Step, 'foreach_loop')
async def test_foreach_empty(mock_foreach, mock_run, mock_moduleloader):
    """Complex step with empty foreach decorator doesn't loop."""
    step = Step({'name': 'step1',
                 'foreach': []})

    context = get_test_context()
    original_len = len(context)

    await step.run_step(context)

    mock_foreach.assert_not_called()
    mock_run.assert_called_once_with(get_test_context())

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'run_conditional_decorators')
async def test_foreach_once(mock_run, mock_moduleloader):
    """foreach loops once."""
    step = Step({'name': 'step1',
                 'foreach': ['one']})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 1 times.'),
        call('foreach: running step one')]

    assert mock_run.call_count == 1
    mutated_context = get_test_context()
    mutated_context['i'] = 'one'
    mock_run.assert_called_once_with(mutated_context)

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    assert context['i'] == 'one'


@patch('pypyr.moduleloader.get_module')
@patch('asynctest.mock.CoroutineMock', new=DeepCopyCoroutineMock)
async def test_foreach_twice(mock_moduleloader):
    """foreach loops twice."""
    step = Step({'name': 'step1',
                 'foreach': ['one', 'two']})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(Step, 'run_conditional_decorators') as mock_run:
        with patch.object(logger, 'info') as mock_logger_info:
            await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 2 times.'),
        call('foreach: running step one'),
        call('foreach: running step two')]

    assert mock_run.call_count == 2
    mutated_context = get_test_context()
    mutated_context['i'] = 'one'

    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = 'two'
    mock_run.assert_any_call(mutated_context)

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'two'


@patch('pypyr.moduleloader.get_module')
@patch('asynctest.mock.CoroutineMock', new=DeepCopyCoroutineMock)
async def test_foreach_thrice_with_substitutions(mock_moduleloader):
    """foreach loops thrice with substitutions inside a list."""
    step = Step({'name': 'step1',
                 'foreach': ['{key1}', '{key2}', 'key3']})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with patch.object(Step, 'run_conditional_decorators') as mock_run:
            await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('foreach: running step key3')]

    assert mock_run.call_count == 3
    mutated_context = get_test_context()
    mutated_context['i'] = 'value1'

    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = 'value2'
    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = 'key3'
    mock_run.assert_any_call(mutated_context)

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'key3'


@patch('pypyr.moduleloader.get_module')
@patch('asynctest.mock.CoroutineMock', new=DeepCopyCoroutineMock)
async def test_foreach_with_single_key_substitution(mock_moduleloader):
    """foreach gets list from string format expression."""
    step = Step({'name': 'step1',
                 'foreach': '{list}'})

    context = get_test_context()
    context['list'] = [99, True, 'string here', 'formatted {key1}']
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(Step, 'run_conditional_decorators') as mock_run:
        with patch.object(logger, 'info') as mock_logger_info:
            await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 4 times.'),
        call('foreach: running step 99'),
        call('foreach: running step True'),
        call('foreach: running step string here'),
        call('foreach: running step formatted value1')]

    assert mock_run.call_count == 4
    mutated_context = get_test_context()
    mutated_context['list'] = [99, True, 'string here', 'formatted {key1}']

    mutated_context['i'] = 99
    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = True
    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = 'string here'
    mock_run.assert_any_call(mutated_context)

    mutated_context['i'] = 'formatted value1'
    mock_run.assert_any_call(mutated_context)

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'formatted value1'


def mock_step_mutating_run(context):
    """Mock a step's run_step by setting a context value False"""
    context['dynamic_run_expression'] = False


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=mock_step_mutating_run)
async def test_foreach_evaluates_run_decorator(mock_invoke, mock_moduleloader):
    """foreach evaluates run_me expression on each loop iteration."""
    step = Step({'name': 'step1',
                 'run': '{dynamic_run_expression}',
                 'foreach': ['{key1}', '{key2}', 'key3']})

    context = get_test_context()
    context['dynamic_run_expression'] = True
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('step1 not running because run is False.'),
        call('foreach: running step key3'),
        call('step1 not running because run is False.')]

    assert mock_invoke.call_count == 1

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'key3'


def mock_step_mutating_skip(context):
    """Mock a step's run_step by setting a context value False"""
    context['dynamic_skip_expression'] = True


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=mock_step_mutating_skip)
async def test_foreach_evaluates_skip_decorator(mock_invoke,
                                                mock_moduleloader):
    """foreach evaluates skip expression on each loop iteration."""
    step = Step({'name': 'step1',
                 'skip': '{dynamic_skip_expression}',
                 'foreach': ['{key1}', '{key2}', 'key3']})

    context = get_test_context()
    context['dynamic_skip_expression'] = False
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('step1 not running because skip is True.'),
        call('foreach: running step key3'),
        call('step1 not running because skip is True.')]

    assert mock_invoke.call_count == 1

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'key3'


def mock_step_deliberate_error(context):
    """Mock step's run_step by setting swallow False and raising err."""
    if context['i'] == 'value2':
        context['dynamic_swallow_expression'] = True
    elif context['i'] == 'key3':
        raise ValueError('arb error')


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=mock_step_deliberate_error)
async def test_foreach_evaluates_swallow_decorator(mock_invoke,
                                                   mock_moduleloader):
    """foreach evaluates skip expression on each loop iteration."""
    step = Step({'name': 'step1',
                 'swallow': '{dynamic_swallow_expression}',
                 'foreach': ['{key1}', '{key2}', 'key3']})

    context = get_test_context()
    context['dynamic_swallow_expression'] = False
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with patch.object(logger, 'error') as mock_logger_error:
            await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('foreach: running step key3')]

    assert mock_invoke.call_count == 3

    assert mock_logger_error.call_count == 1
    mock_logger_error.assert_called_once_with(
        'step1 Ignoring error '
        'because swallow is True for this step.\nValueError: arb error')

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'key3'


# ------------------- Step: run_step: foreach --------------------------------#

# ------------------- Step: run_step: while ----------------------------------#


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_while_max(mock_invoke, mock_moduleloader):
    """while runs to max."""
    step = Step({'name': 'step1',
                 'while': {'max': 3}})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]

    assert mock_invoke.call_count == 3

    # validate all the in params ended up in context as intended, plus counter
    assert len(context) == original_len + 1
    # after the looping's done, the counter value will be the last iterator
    assert context['whileCounter'] == 3


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=mock_step_mutating_run)
async def test_while_evaluates_run_decorator(mock_invoke, mock_moduleloader):
    """while evaluates run_me expression on each loop iteration."""
    step = Step({'name': 'step1',
                 'run': '{dynamic_run_expression}',
                 'while': {'max': '{whileMax}', 'stop': '{key5}'}})

    context = get_test_context()
    context['dynamic_run_expression'] = True
    context['whileMax'] = 3
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times, or until {key5} evaluates to '
             'True at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('step1 not running because run is False.'),
        call('while: running step with counter 3'),
        call('step1 not running because run is False.'),
        call('while decorator looped 3 times, and {key5} never evaluated to '
             'True.')]

    assert mock_invoke.call_count == 1

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['whileCounter'] == 3


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=[None, ValueError('whoops')])
async def test_while_error_kicks_loop(mock_invoke, mock_moduleloader):
    """Error during while kicks loop."""
    step = Step({'name': 'step1',
                 'while': {'max': 3}})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with pytest.raises(ValueError) as err_info:
            await step.run_step(context)

    assert str(err_info.value) == "whoops"

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2')]

    assert mock_invoke.call_count == 2

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['whileCounter'] == 2


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_while_exhausts(mock_invoke, mock_moduleloader):
    """while exhausts throws error on errorOnMax."""
    step = Step({'name': 'step1',
                 'while': {'max': '{whileMax}',
                           'stop': '{key5}',
                           'errorOnMax': '{key6}'}})

    context = get_test_context()
    context['whileMax'] = 3
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with pytest.raises(LoopMaxExhaustedError) as err_info:
            await step.run_step(context)

    assert str(err_info.value) == ("while loop reached "
                                   "3 and {key5} never evaluated to True.")

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times, or until {key5} evaluates to '
             'True at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]

    assert mock_invoke.call_count == 3

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['whileCounter'] == 3


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_while_exhausts_hard_true(mock_invoke, mock_moduleloader):
    """while evaluates run_me expression on each loop iteration, no format."""
    step = Step({'name': 'step1',
                 'while': {'max': '{whileMax}',
                           'stop': False,
                           'errorOnMax': True}})

    context = get_test_context()
    context['whileMax'] = 3
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with pytest.raises(LoopMaxExhaustedError) as err_info:
            await step.run_step(context)

    assert str(err_info.value) == "while loop reached 3."

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]

    assert mock_invoke.call_count == 3

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 1
    # after the looping's done, the i value will be the last iterator value
    assert context['whileCounter'] == 3


@patch('pypyr.moduleloader.get_module')
@patch('asynctest.mock.CoroutineMock', new=DeepCopyCoroutineMock)
async def test_while_nests_foreach_with_substitutions(mock_moduleloader):
    """while loops twice, foreach thrice with substitutions inside a list."""
    step = Step({'name': 'step1',
                 'foreach': ['{key1}', '{key2}', 'key3'],
                 'while': {'max': 2}
                 })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(Step, 'run_conditional_decorators') as mock_run:
        with patch.object(logger, 'info') as mock_logger_info:
            await step.run_step(context)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 2 times at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('foreach: running step key3'),
        call('while: running step with counter 2'),
        call('foreach decorator will loop 3 times.'),
        call('foreach: running step value1'),
        call('foreach: running step value2'),
        call('foreach: running step key3')]

    assert mock_run.call_count == 6
    mutated_context = get_test_context()
    mutated_context['whileCounter'] = 1
    mutated_context['i'] = 'value1'
    mock_run.assert_any_call(mutated_context)
    mutated_context['i'] = 'value2'
    mock_run.assert_any_call(mutated_context)
    mutated_context['i'] = 'key3'
    mock_run.assert_any_call(mutated_context)

    mutated_context['whileCounter'] = 2
    mutated_context['i'] = 'value1'
    mock_run.assert_any_call(mutated_context)
    mutated_context['i'] = 'value2'
    mock_run.assert_any_call(mutated_context)
    mutated_context['i'] = 'key3'
    mock_run.assert_any_call(mutated_context)

    # validate all the in params ended up in context as intended, plus i
    assert len(context) == original_len + 2
    # after the looping's done, the i value will be the last iterator value
    assert context['i'] == 'key3'
    assert context['whileCounter'] == 2


# ------------------- Step: run_step: while ----------------------------------#

# ------------------- Step: invoke_step---------------------------------------#


@patch('pypyr.moduleloader.get_module')
async def test_invoke_step_pass(mocked_moduleloader):
    """run_pipeline_step test pass."""
    step = Step('mocked.step')
    await step.invoke_step(get_test_context())

    mocked_moduleloader.assert_called_once_with('mocked.step')
    mocked_moduleloader.return_value.run_step.assert_called_once_with(
        {'key1': 'value1',
         'key2': 'value2',
         'key3': 'value3',
         'key4': [
             {'k4lk1': 'value4', 'k4lk2': 'value5'},
             {'k4lk1': 'value6', 'k4lk2': 'value7'}],
         'key5': False,
         'key6': True,
         'key7': 77})


@patch('pypyr.moduleloader.get_module', return_value=3)
async def test_invoke_step_no_run_step(mocked_moduleloader):
    """run_pipeline_step fails if no run_step on imported module."""
    step = Step('mocked.step')

    with pytest.raises(AttributeError) as err_info:
        await step.invoke_step(get_test_context())

    mocked_moduleloader.assert_called_once_with('mocked.step')

    assert str(err_info.value) == "'int' object has no attribute 'run_step'"


@patch('pypyr.moduleloader.get_module')
async def test_invoke_step_context_abides(mocked_moduleloader):
    """Step mutates context & mutation abides after run_pipeline_step."""
    mocked_moduleloader.return_value.run_step = mock_run_step
    context = get_test_context()

    step = Step('mocked.step')
    await step.invoke_step(context)

    mocked_moduleloader.assert_called_once_with('mocked.step')
    assert context['test_run_step'] == 'this was set in step'


@patch('pypyr.moduleloader.get_module')
async def test_invoke_step_empty_context(mocked_moduleloader):
    """Empty context in step (i.e count == 0, but not is None)"""
    mocked_moduleloader.return_value.run_step = mock_run_step_empty_context
    context = get_test_context()

    step = Step('mocked.step')
    await step.invoke_step(context)

    mocked_moduleloader.assert_called_once_with('mocked.step')
    assert len(context) == 0
    assert context is not None


@patch('pypyr.moduleloader.get_module')
async def test_invoke_step_none_context(mocked_moduleloader):
    """Step rebinding context to None doesn't affect the caller Context."""
    mocked_moduleloader.return_value.run_step = mock_run_step_none_context
    context = get_test_context()

    step = Step('mocked.step')
    await step.invoke_step(False)

    mocked_moduleloader.assert_called_once_with('mocked.step')
    assert context == {'key1': 'value1',
                       'key2': 'value2',
                       'key3': 'value3',
                       'key4': [
                           {'k4lk1': 'value4', 'k4lk2': 'value5'},
                           {'k4lk1': 'value6', 'k4lk2': 'value7'}],
                       'key5': False,
                       'key6': True,
                       'key7': 77}


# ------------------- Step: invoke_step---------------------------------------#

# ------------------- Step: run_step: run ------------------------------------#


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_true(mock_invoke_step,
                                                        mock_get_module):
    """Complex step with run decorator set true will run step."""
    step = Step({'name': 'step1',
                 'run': True})

    context = get_test_context()
    original_len = len(context)

    await step.run_step(context)

    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_false(mock_invoke_step,
                                                         mock_get_module):
    """Complex step with run decorator set false doesn't run step."""
    step = Step({'name': 'step1',
                 'run': False})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call("step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_str_formatting_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run formatting expression false doesn't run step."""
    step = Step({
        'name': 'step1',
        # name will evaluate False because it's a string and it's not 'True'.
        'run': '{key1}'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call("step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_str_false(mock_invoke_step,
                                                             mock_get_module):
    """Complex step with run set to string False doesn't run step."""
    step = Step({
        'name': 'step1',
        # name will evaluate False because it's a string and it's not 'True'.
        'run': 'False'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_str_lower_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run set to string false doesn't run step."""
    step = Step({
        'name': 'step1',
        # name will evaluate False because it's a string and it's not 'True'.
        'run': 'false'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_bool_formatting_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run formatting expression false doesn't run step."""
    step = Step({
        'name': 'step1',
        # key5 will evaluate False because it's a bool and it's False
        'run': '{key5}'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_bool_formatting_true(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run formatting expression true runs step."""
    step = Step({
        'name': 'step1',
        # key6 will evaluate True because it's a bool and it's True
        'run': '{key6}'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_string_true(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run formatting expression True runs step."""
    step = Step({
        'name': 'step1',
        # 'True' will evaluate bool True
        'run': 'True'})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_1_true(mock_invoke_step,
                                                          mock_get_module):
    """Complex step with run 1 runs step."""
    step = Step({
        'name': 'step1',
        # 1 will evaluate True because it's an int and 1
        'run': 1})

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_99_true(mock_invoke_step,
                                                           mock_get_module):
    """Complex step with run 99 runs step."""
    step = Step({
        'name': 'step1',
        # 99 will evaluate True because it's an int and > 0
        'run': 99
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_run_neg1_true(mock_invoke_step,
                                                             mock_get_module):
    """Complex step with run -1 runs step."""
    step = Step({
        'name': 'step1',
        # -1 will evaluate True because it's an int and != 0
        'run': -1
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


# ------------------- Step: run_step: run ------------------------------------#


# ------------------- Step: run_step: skip -----------------------------------#
@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_false(mock_invoke_step,
                                                          mock_get_module):
    """Complex step with skip decorator set false will run step."""
    step = Step({
        'name': 'step1',
        'skip': False
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_true(mock_invoke_step,
                                                         mock_get_module):
    """Complex step with skip decorator set true runa step."""
    step = Step({
        'name': 'step1',
        'skip': True
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_str_formatting_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with skip formatting expression false doesn't run step."""
    step = Step({
        'name': 'step1',
        # name will evaluate True
        'skip': '{key6}'
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_str_true(mock_invoke_step,
                                                             mock_get_module):
    """Complex step with skip set to string False doesn't run step."""
    step = Step({
        'name': 'step1',
        # skip evaluates True because it's a string and TRUE parses to True.
        'skip': 'TRUE'
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_str_lower_true(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run set to string true doesn't run step."""
    step = Step({
        'name': 'step1',
        # skip will evaluate true because it's a string and true is True.
        'skip': 'true'
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_run_and_skip_bool_formatting_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run doesn't run step, evals before skip."""
    step = Step({
        'name': 'step1',
        # key5 will evaluate False because it's a bool and it's False
        'run': '{key5}',
        'skip': True
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because run is False.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_skip_bool_formatting_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with skip formatting expression true runs step."""
    step = Step({
        'name': 'step1',
        # key5 will evaluate False because it's a bool and it's False
        'skip': '{key5}'
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_string_false(
        mock_invoke_step,
        mock_get_module):
    """Complex step with skip formatting expression False runs step."""
    step = Step({
        'name': 'step1',
        # 'False' will evaluate bool False
        'skip': 'False'
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_0_true(
        mock_invoke_step,
        mock_get_module):
    """Complex step with run 1 runs step."""
    step = Step({
        'name': 'step1',
        # 0 will evaluate False because it's an int and 0
        'skip': 0
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_99_true(
        mock_invoke_step,
        mock_get_module):
    """Complex step with skip 99 doesn't run step."""
    step = Step({
        'name': 'step1',
        # 99 will evaluate True because it's an int and > 0
        'skip': 99
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call(
        "step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_with_skip_neg1_true(mock_invoke_step,
                                                              mock_get_module):
    """Complex step with run -1 runs step."""
    step = Step({
        'name': 'step1',
        # -1 will evaluate True because it's an int and != 0
        'skip': -1
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await step.run_step(context)

    mock_logger_info.assert_any_call("step1 not running because skip is True.")
    mock_invoke_step.assert_not_called()

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


# ------------------- Step: run_step: skip -----------------------------------#

# ------------------- Step: run_step: swallow --------------------------------#
@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_swallow_true(mock_invoke_step,
                                                       mock_get_module):
    """Complex step with swallow true runs normally even without error."""
    step = Step({
        'name': 'step1',
        'swallow': True
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step')
async def test_run_pipeline_steps_complex_swallow_false(mock_invoke_step,
                                                        mock_get_module):
    """Complex step with swallow false runs normally even without error."""
    step = Step({
        'name': 'step1',
        'swallow': False
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=ValueError('arb error here'))
async def test_run_pipeline_steps_complex_swallow_true_error(mock_invoke_step,
                                                             mock_get_module):
    """Complex step with swallow true swallows error."""
    step = Step({
        'name': 'step1',
        'swallow': 1
    })

    context = get_test_context()
    original_len = len(context)

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        with patch.object(logger, 'error') as mock_logger_error:
            await step.run_step(context)

    mock_logger_debug.assert_any_call("done")
    mock_logger_error.assert_called_once_with(
        "step1 Ignoring error because swallow is True "
        "for this step.\n"
        "ValueError: arb error here")
    mock_invoke_step.assert_called_once_with(
        context={'key1': 'value1',
                 'key2': 'value2',
                 'key3': 'value3',
                 'key4': [
                     {'k4lk1': 'value4',
                      'k4lk2': 'value5'},
                     {'k4lk1': 'value6',
                      'k4lk2': 'value7'}
                 ],
                 'key5': False,
                 'key6': True,
                 'key7': 77})

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=ValueError('arb error here'))
async def test_run_pipeline_steps_complex_swallow_false_error(mock_invoke_step,
                                                              mock_get_module):
    """Complex step with swallow false raises error."""
    step = Step({
        'name': 'step1',
        'swallow': 0
    })

    context = get_test_context()
    original_len = len(context)

    with pytest.raises(ValueError) as err_info:
        await step.run_step(context)

        assert str(err_info.value) == "arb error here"

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=ValueError('arb error here'))
async def test_run_pipeline_steps_complex_swallow_defaults_false_error(
        mock_invoke_step,
        mock_get_module):
    """Complex step with swallow not specified still raises error."""
    step = Step({
        'name': 'step1'
    })

    context = get_test_context()
    original_len = len(context)

    with pytest.raises(ValueError) as err_info:
        await step.run_step(context)

    assert str(err_info.value) == "arb error here"

    # validate all the in params ended up in context as intended
    assert len(context) == original_len


@patch('pypyr.moduleloader.get_module')
@patch.object(Step, 'invoke_step', side_effect=ValueError('arb error here'))
async def test_run_pipeline_steps_simple_with_error(mock_invoke_step,
                                                    mock_get_module):
    """Simple step run with error should not swallow."""
    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'debug') as mock_logger_debug:
        step = Step('step1')
        with pytest.raises(ValueError) as err_info:
            await step.run_step(Context({'k1': 'v1'}))

            assert str(err_info.value) == "arb error here"

    mock_logger_debug.assert_any_call('step1 is a simple string.')
    mock_invoke_step.assert_called_once_with(
        context={'k1': 'v1'})


# ------------------- Step: run_step: swallow --------------------------------#

# ------------------- Step: set_step_input_context ---------------------------#


@patch('pypyr.moduleloader.get_module')
async def test_set_step_input_context_no_in_simple(mocked_moduleloader):
    """Set step context does nothing if no in key found in simple step."""
    step = Step('blah')
    context = get_test_context()
    step.set_step_input_context(context)

    assert context == get_test_context()


@patch('pypyr.moduleloader.get_module')
async def test_set_step_input_context_no_in_complex(mocked_moduleloader):
    """Set step context does nothing if no in key found in complex step."""
    step = Step({'name': 'blah'})
    context = get_test_context()
    step.set_step_input_context(context)

    assert context == get_test_context()


@patch('pypyr.moduleloader.get_module')
async def test_set_step_input_context_in_empty(mocked_moduleloader):
    """Set step context does nothing if in key found but it's empty."""
    step = Step({'name': 'blah', 'in': {}})
    context = get_test_context()
    step.set_step_input_context(context)

    assert context == get_test_context()


@patch('pypyr.moduleloader.get_module')
async def test_set_step_input_context_with_in(mocked_moduleloader):
    """Set step context adds in to context."""
    context = get_test_context()
    original_len = len(context)
    in_args = {'newkey1': 'v1',
               'newkey2': 'v2',
               'key3': 'updated in',
               'key4': [0, 1, 2, 3],
               'key5': True,
               'key6': False,
               'key7': 88}
    step = Step({'name': 'blah', 'in': in_args})
    step.set_step_input_context(context)

    assert len(context) - 2 == original_len
    assert context['newkey1'] == 'v1'
    assert context['newkey2'] == 'v2'
    assert context['key1'] == 'value1'
    assert context['key2'] == 'value2'
    assert context['key3'] == 'updated in'
    assert context['key4'] == [0, 1, 2, 3]
    assert context['key5']
    assert not context['key6']
    assert context['key7'] == 88


# ------------------- Step: set_step_input_context ---------------------------#
# ------------------- Step----------------------------------------------------#

# ------------------- WhileDecorator -----------------------------------------#
# ------------------- WhileDecorator: init -----------------------------------#


async def test_while_init_defaults_stop():
    """WhileDecorator ctor sets defaults with only stop set."""
    wd = WhileDecorator({'stop': 'arb'})
    assert wd.stop == 'arb'
    assert wd.sleep == 0
    assert wd.max is None
    assert not wd.error_on_max


async def test_while_init_defaults_max():
    """WhileDecorator ctor sets defaults with only max set."""
    wd = WhileDecorator({'max': 3})
    assert wd.stop is None
    assert wd.sleep == 0
    assert wd.max == 3
    assert not wd.error_on_max


async def test_while_init_all_attributes():
    """WhileDecorator ctor with all props set."""
    wd = WhileDecorator(
        {'errorOnMax': True, 'max': 3, 'sleep': 4.4, 'stop': '5'})
    assert wd.stop == '5'
    assert wd.sleep == 4.4
    assert wd.max == 3
    assert wd.error_on_max


async def test_while_init_not_a_dict():
    """WhileDecorator raises PipelineDefinitionError on bad ctor input."""
    with pytest.raises(PipelineDefinitionError) as err_info:
        WhileDecorator('arb')

    assert str(err_info.value) == (
        "while decorator must be a dict (i.e a map) type.")


async def test_while_init_no_max_no_stop():
    """WhileDecorator raises PipelineDefinitionError on no max and no stop."""
    with pytest.raises(PipelineDefinitionError) as err_info:
        WhileDecorator({'arb': 'arbv'})

    assert str(err_info.value) == (
        "the while decorator must have either max or "
        "stop, or both. But not neither. Note that setting stop: False with "
        "no max is an infinite loop. If an infinite loop is really what you "
        "want, set stop: \'{ContextKeyWithFalseValue}\'")


# ------------------- WhileDecorator: init -----------------------------------#

# ------------------- WhileDecorator: exec_iteration -------------------------#
async def test_while_exec_iteration_no_stop():
    """exec_iteration returns False when no stop condition given."""
    wd = WhileDecorator({'max': 3})

    context = Context({})
    mock = CoroutineMock()
    assert not await wd.exec_iteration(2, context, mock)
    # context endures
    assert context['whileCounter'] == 2
    assert len(context) == 1
    # step_method called once and only once with updated context
    mock.assert_called_once_with({'whileCounter': 2})


async def test_while_exec_iteration_stop_true():
    """exec_iteration returns True when stop is bool True."""
    wd = WhileDecorator({'stop': True})

    context = Context({})
    mock = CoroutineMock()
    assert await wd.exec_iteration(2, context, mock)
    # context endures
    assert context['whileCounter'] == 2
    assert len(context) == 1
    # step_method called once and only once with updated context
    mock.assert_called_once_with({'whileCounter': 2})


async def test_while_exec_iteration_stop_evals_true():
    """exec_iteration True when stop evals True from formatting expr."""
    wd = WhileDecorator({'stop': '{stop}'})

    context = Context({'stop': True})
    mock = CoroutineMock()
    assert await wd.exec_iteration(2, context, mock)
    # context endures
    assert context['whileCounter'] == 2
    assert len(context) == 2
    # step_method called once and only once with updated context
    mock.assert_called_once_with({'stop': True, 'whileCounter': 2})


async def test_while_exec_iteration_stop_false():
    """exec_iteration False when stop is False."""
    wd = WhileDecorator({'max': 1, 'stop': False})

    context = Context()
    mock = CoroutineMock()
    assert not await wd.exec_iteration(2, context, mock)
    # context endures
    assert context['whileCounter'] == 2
    assert len(context) == 1
    # step_method called once and only once with updated context
    mock.assert_called_once_with({'whileCounter': 2})


async def test_while_exec_iteration_stop_evals_false():
    """exec_iteration False when stop is False."""
    wd = WhileDecorator({'stop': '{stop}'})

    context = Context({'stop': False})
    mock = CoroutineMock()

    assert not await wd.exec_iteration(2, context, mock)
    # context endures
    assert context['whileCounter'] == 2
    assert len(context) == 2
    # step_method called once and only once with updated context
    mock.assert_called_once_with({'stop': False, 'whileCounter': 2})


# ------------------- WhileDecorator: exec_iteration -------------------------#

# ------------------- WhileDecorator: while_loop -----------------------------#


async def test_while_loop_stop_true():
    """Stop True doesn't run loop even once."""
    wd = WhileDecorator({'stop': True})

    mock = CoroutineMock()

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(Context(), mock)

    mock.assert_not_called()

    assert mock_logger_info.mock_calls == [
        call('while decorator will not loop, because the stop condition True '
             'already evaluated to True before 1st iteration.')]


async def test_while_loop_stop_evals_true():
    """Stop evaluates True from formatting expr doesn't run loop even once."""
    wd = WhileDecorator({'stop': '{thisistrue}'})

    mock = CoroutineMock()

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(Context({'thisistrue': True}), mock)

    mock.assert_not_called()

    assert mock_logger_info.mock_calls == [
        call('while decorator will not loop, because the stop condition '
             '{thisistrue} already evaluated to True before 1st iteration.')]


async def test_while_loop_no_stop_no_max():
    """no stop, no max should raise error."""
    wd = WhileDecorator({'stop': True})
    wd.max = None
    wd.stop = None

    mock = CoroutineMock()
    with pytest.raises(PipelineDefinitionError) as err_info:
        await wd.while_loop(Context(), mock)

    mock.assert_not_called()
    assert str(err_info.value) == (
        "the while decorator must have either max or "
        "stop, or both. But not neither.")


@patch('asyncio.sleep')
async def test_while_loop_max_no_stop(mock_time_sleep):
    """while loop runs with max but no stop."""
    wd = WhileDecorator({'max': 3})
    context = Context({'k1': 'v1'})
    mock = CoroutineMock()

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(context, mock)

    assert context['whileCounter'] == 3
    assert mock.call_count == 3
    mock.assert_called_with({'k1': 'v1', 'whileCounter': 3})

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times at 0.0s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]


@patch('asyncio.sleep')
async def test_while_loop_stop_no_max(mock_time_sleep):
    """while loop runs with stop but no max."""
    wd = WhileDecorator({'stop': '{k1}', 'sleep': '{k2}'})
    context = Context({'k1': False, 'k2': 0.3})

    step_count = 0
    step_context = []

    def mock_step(context):
        nonlocal step_count, step_context
        step_count += 1
        step_context.append(deepcopy(context))
        if context['whileCounter'] == 3:
            context['k1'] = True

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(context, mock_step)

    assert context['whileCounter'] == 3
    assert step_count == 3
    assert step_context == [{'k1': False, 'k2': 0.3, 'whileCounter': 1},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 2},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 3}]

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0.3)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop until {k1} evaluates to True at 0.3s '
             'intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]


@patch('asyncio.sleep')
async def test_while_loop_stop_and_max_stop_before_max(mock_time_sleep):
    """while loop runs with stop and max, exit before max."""
    wd = WhileDecorator({'max': 5, 'stop': '{k1}', 'sleep': '{k2}'})
    context = Context({'k1': False, 'k2': 0.3})

    step_count = 0
    step_context = []

    def mock_step(context):
        nonlocal step_count, step_context
        step_count += 1
        step_context.append(deepcopy(context))
        if context['whileCounter'] == 3:
            context['k1'] = True

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(context, mock_step)

    assert context['whileCounter'] == 3
    assert step_count == 3
    assert step_context == [{'k1': False, 'k2': 0.3, 'whileCounter': 1},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 2},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 3}]

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0.3)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 5 times, or until {k1} evaluates to '
             'True at 0.3s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]


@patch('asyncio.sleep')
async def test_while_loop_stop_and_max_exhaust_max(mock_time_sleep):
    """while loop runs with stop and max, exhaust max."""
    wd = WhileDecorator({'max': 3, 'stop': '{k1}', 'sleep': '{k2}'})
    context = Context({'k1': False, 'k2': 0.3})

    step_count = 0
    step_context = []

    def mock_step(context):
        nonlocal step_count, step_context
        step_count += 1
        step_context.append(deepcopy(context))

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(context, mock_step)

    assert context['whileCounter'] == 3
    assert step_count == 3
    assert step_context == [{'k1': False, 'k2': 0.3, 'whileCounter': 1},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 2},
                            {'k1': False, 'k2': 0.3, 'whileCounter': 3}]

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0.3)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times, or until {k1} evaluates to '
             'True at 0.3s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3'),
        call('while decorator looped 3 times, and {k1} never evaluated to '
             'True.')]


@patch('asyncio.sleep')
async def test_while_loop_stop_and_max_exhaust_error(mock_time_sleep):
    """while loop runs with stop and max, exhaust max."""
    wd = WhileDecorator({'max': 3,
                         'stop': '{k1}',
                         'sleep': '{k2}',
                         'errorOnMax': '{k3}'})
    context = Context({'k1': False, 'k2': 0.3, 'k3': True})

    step_count = 0
    step_context = []

    def mock_step(context):
        nonlocal step_count, step_context
        step_count += 1
        step_context.append(deepcopy(context))

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with patch.object(logger, 'error') as mock_logger_error:
            with pytest.raises(LoopMaxExhaustedError) as err_info:
                await wd.while_loop(context, mock_step)

    assert str(err_info.value) == (
        "while loop reached 3 and {k1} never evaluated to True.")

    assert context['whileCounter'] == 3
    assert step_count == 3
    assert step_context == [{'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 1},
                            {'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 2},
                            {'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 3}]

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0.3)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times, or until {k1} evaluates to '
             'True at 0.3s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]

    assert mock_logger_error.mock_calls == [
        call('exhausted 3 iterations of while loop, and errorOnMax is True.')
    ]


@patch('asyncio.sleep')
async def test_while_loop_max_exhaust_error(mock_time_sleep):
    """while loop runs with only max, exhaust max."""
    wd = WhileDecorator({'max': 3,
                         'sleep': '{k2}',
                         'errorOnMax': True})
    context = Context({'k1': False, 'k2': 0.3, 'k3': True})

    step_count = 0
    step_context = []

    def mock_step(context):
        nonlocal step_count, step_context
        step_count += 1
        step_context.append(deepcopy(context))

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        with patch.object(logger, 'error') as mock_logger_error:
            with pytest.raises(LoopMaxExhaustedError) as err_info:
                await wd.while_loop(context, mock_step)

    assert str(err_info.value) == "while loop reached 3."

    assert context['whileCounter'] == 3
    assert step_count == 3
    assert step_context == [{'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 1},
                            {'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 2},
                            {'k1': False,
                             'k2': 0.3,
                             'k3': True,
                             'whileCounter': 3}]

    assert mock_time_sleep.call_count == 2
    mock_time_sleep.assert_called_with(0.3)

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 3 times at 0.3s intervals.'),
        call('while: running step with counter 1'),
        call('while: running step with counter 2'),
        call('while: running step with counter 3')]

    assert mock_logger_error.mock_calls == [
        call('exhausted 3 iterations of while loop, and errorOnMax is True.')
    ]


@patch('asyncio.sleep')
async def test_while_loop_all_substitutions(mock_time_sleep):
    """while loop runs every param substituted."""
    wd = WhileDecorator({'max': '{k3[1][k031]}',
                         'stop': '{k1}',
                         'sleep': '{k2}',
                         'errorOnMax': '{k3[1][k032]}'})
    context = Context({'k1': False,
                       'k2': 0.3,
                       'k3': [
                           0,
                           {'k031': 1, 'k032': False}
                       ]})

    step_count = 0

    def mock_step(context):
        nonlocal step_count
        step_count += 1

    logger = logging.getLogger('pypyr.dsl')
    with patch.object(logger, 'info') as mock_logger_info:
        await wd.while_loop(context, mock_step)

    assert context['whileCounter'] == 1
    assert step_count == 1

    assert mock_time_sleep.call_count == 0

    assert mock_logger_info.mock_calls == [
        call('while decorator will loop 1 times, or until {k1} evaluates to '
             'True at 0.3s intervals.'),
        call('while: running step with counter 1'),
        call('while decorator looped 1 times, and {k1} never evaluated to '
             'True.')]
# ------------------- WhileDecorator: while_loop -----------------------------#
# ------------------- WhileDecorator -----------------------------------------#
