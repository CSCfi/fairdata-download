import click
import pytest

from download.mq import get_mq

def test_generate_command(runner, pending_dataset, not_found_dataset):
    # Pending dataset
    result = runner.invoke(args=['generator', 'generate'])

    assert not result.exception

    # Not found dataset
    result = runner.invoke(args=['generator', 'generate'])

    assert not result.exception

    # Not pending dataset
    result = runner.invoke(args=['generator', 'generate'])

    assert not result.exception

    # Empty queue
    result = runner.invoke(args=['generator', 'generate'])

    assert not result.exception

