import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip(reason="Materialization only supports languages ['sql']; got 'python'")
class TestBasePythonModelTests(BasePythonModelTests):
    pass
