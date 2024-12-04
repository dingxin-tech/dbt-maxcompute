import pytest
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic


@pytest.mark.skip(
    reason="`get_create_materialized_view_as_sql` has not been implemented for this adapter."
)
class TestMaterializedViewMaxCompute(MaterializedViewBasic):
    pass
