from typing import Tuple, Optional

from dbt.adapters.base import BaseRelation
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.tests.util import (
    assert_message_in_logs,
    run_dbt,
    run_dbt_and_capture,
)


class TestMaterializedViewMaxCompute(MaterializedViewBasic):
    def test_materialized_view_create(self, project, my_materialized_view):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_materialized_view_create_idempotent(self, project, my_materialized_view):
        # setup creates it once; verify it's there and run once
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_materialized_view_full_refresh(self, project, my_materialized_view):
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_materialized_view.identifier, "--full-refresh"]
        )
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"
        assert_message_in_logs(f"Applying REPLACE to: {my_materialized_view}", logs)

    def test_materialized_view_replaces_table(self, project, my_table):
        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_materialized_view(project, my_table)

        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "materialized_view"

    def test_materialized_view_replaces_view(self, project, my_view):
        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_materialized_view(project, my_view)

        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "materialized_view"

    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        """
        Because materialized views in mc cannot be renamed,
        backup tables cannot be created when changing to table materialization.
        Therefore the modification cannot be successful.
        """
        pass

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        """
        Because materialized views in mc cannot be renamed,
        backup tables cannot be created when changing to table materialization.
        Therefore the modification cannot be successful.
        """
        pass

    def test_materialized_view_only_updates_after_refresh(
            self, project, my_materialized_view, my_seed
    ):
        # poll database
        table_start = self.query_row_count(project, my_seed)
        view_start = self.query_row_count(project, my_materialized_view)

        # insert new record in table
        self.insert_record(project, my_seed, (4, 400))

        # poll database
        table_mid = self.query_row_count(project, my_seed)
        view_mid = self.query_row_count(project, my_materialized_view)

        # refresh the materialized view
        self.refresh_materialized_view(project, my_materialized_view)

        # poll database
        table_end = self.query_row_count(project, my_seed)
        view_end = self.query_row_count(project, my_materialized_view)

        # new records were inserted in the table but didn't show up in the view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end

    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table.render()} (`id`, `value`) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        sql = f"ALTER MATERIALIZED VIEW {materialized_view.render()} REBUILD;"
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        with project.adapter.connection_named("_test"):
            return project.adapter.get_relation(
                relation.database, relation.schema, relation.identifier
            ).type.lower()
