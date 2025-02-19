import json

import pytest
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.tests.adapter.persist_docs import fixtures
from dbt.tests.util import run_dbt

_MODELS__VIEW = """
{{ config(materialized='table') }}
select 2 as id, 'Bob' as name
"""


class TestPersistDocsToView(BasePersistDocs):
    pass


# Note: Not support persist docs to view.
class TestPersistDocsRedshift(BasePersistDocs):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_docs_model.sql": fixtures._MODELS__NO_DOCS_MODEL,
            "table_model.sql": fixtures._MODELS__TABLE,
        }

    def test_has_comments_pglike(self, project):
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 3
        table_node = catalog_data["nodes"]["model.test.table_model"]
        view_node = self._assert_has_table_comments(table_node)

        no_docs_node = catalog_data["nodes"]["model.test.no_docs_model"]
        self._assert_has_view_comments(no_docs_node, False, False)

    pass


class TestPersistDocsRedshiftColumn(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
