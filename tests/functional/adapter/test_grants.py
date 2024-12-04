import os

import pytest
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.util import get_connection, relation_from_name


class TestSeedGrants(BaseSeedGrants):
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            actual_grants = adapter.run_security_sql(show_grant_sql)
        return actual_grants

    os.environ["DBT_TEST_USER_1"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_2"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_3"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    pass


@pytest.mark.skip(reason="invalid privilege type INSERT for TABLE.")
class TestModelGrants(BaseModelGrants):
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            actual_grants = adapter.run_security_sql(show_grant_sql)
        return actual_grants

    os.environ["DBT_TEST_USER_1"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_2"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_3"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    pass


class TestModelGrantsInvalid(BaseInvalidGrants):

    def grantee_does_not_exist_error(self):
        return "Unhandled error"

    def privilege_does_not_exist_error(self):
        return "Unhandled error"

    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            actual_grants = adapter.run_security_sql(show_grant_sql)
        return actual_grants

    os.environ["DBT_TEST_USER_1"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_2"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_3"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    pass


class TestSnapshotGrants(BaseSnapshotGrants):
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            actual_grants = adapter.run_security_sql(show_grant_sql)
        return actual_grants

    os.environ["DBT_TEST_USER_1"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_2"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_3"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    pass


class TestIncrementalGrants(BaseIncrementalGrants):
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            actual_grants = adapter.run_security_sql(show_grant_sql)
        return actual_grants

    os.environ["DBT_TEST_USER_1"] = "RAM$mc_schema@test.aliyunid.com:test_user_2507"
    os.environ["DBT_TEST_USER_2"] = "RAM$mc_schema@test.aliyunid.com:test_user_2353"
    pass
