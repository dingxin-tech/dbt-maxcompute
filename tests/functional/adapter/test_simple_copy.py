import pytest
from dbt.tests.adapter.simple_copy.test_simple_copy import SimpleCopyBase, EmptyModelsArentRunBase
from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase
from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal


class TestSimpleCopyBase(SimpleCopyBase):
    pass


@pytest.mark.skip(reason="This test is ok, but we need re-implement get_tables_in_schema method")
class TestEmptyModelsArentRun(EmptyModelsArentRunBase):

    def test_dbt_doesnt_run_empty_models(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7

        tables = self.get_tables_in_schema(project)

        assert "empty" not in tables.keys()
        assert "disabled" not in tables.keys()

    def get_tables_in_schema(self, project):
        odps = project.adapter.get_odps_client()
        tables = odps.list_tables(schema=self.test_schema)
        return {table.name: table.type for table in tables}

    pass
