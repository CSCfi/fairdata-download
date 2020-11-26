import click
import pytest

from download.services.mq import get_mq, init_mq

def test_init_command_confirm(runner, mock_init_mq, recorder):
    result = runner.invoke(args=['mq', 'init'], input='y\n')

    assert not result.exception
    assert 'Do you want to continue?' in result.output
    assert 'Initialized' in result.output
    assert recorder.called

def test_init_command_unconfirm(runner, mock_init_mq, recorder):
    result = runner.invoke(args=['mq', 'init'], input='n\n')

    assert not result.exception
    assert 'Do you want to continue?' in result.output
    assert 'Initialized' not in result.output
    assert not recorder.called
