import pytest

from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)


@pytest.mark.skip(reason="Not yet test")
class TestAliasesMaxCompute(BaseAliases):
    pass


@pytest.mark.skip(reason="Not yet test")
class TestAliasErrorsMaxCompute(BaseAliasErrors):
    pass


@pytest.mark.skip(reason="Not yet test")
class TestSameAliasDifferentSchemasMaxCompute(BaseSameAliasDifferentSchemas):
    pass


@pytest.mark.skip(reason="Not yet test")
class TestSameAliasDifferentDatabasesMaxCompute(BaseSameAliasDifferentDatabases):
    pass
