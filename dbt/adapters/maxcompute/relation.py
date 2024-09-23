from dataclasses import dataclass, field
from typing import FrozenSet, Optional, TypeVar

from dbt.adapters.base.relation import BaseRelation, ComponentName, InformationSchema
from dbt.adapters.contracts.relation import RelationType, Path, Policy
from dbt_common.utils.dict import filter_null_values
from odps.models import Table

Self = TypeVar("Self", bound="MaxComputeRelation")


@dataclass(frozen=True, eq=False, repr=False)
class MaxComputeRelation(BaseRelation):
    quote_character: str = "`"
    # subquery alias name is not required in MaxCompute
    require_alias: bool = False

    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )

    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )

    def matches(
            self,
            database: Optional[str] = None,
            schema: Optional[str] = None,
            identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                return False

        return True

    @property
    def project(self):
        return self.database

    @property
    def schema(self):
        return self.schema

    def information_schema(self, identifier: Optional[str] = None) -> "MaxComputeInformationSchema":
        return MaxComputeInformationSchema.from_relation(self, identifier)

    @classmethod
    def from_odps_table(cls, table: Table):
        identifier = table.name
        schema = table.get_schema()
        schema = schema.name if schema else None

        is_view = table.is_virtual_view or table.is_materialized_view

        return cls.create(
            database=table.project.name,
            schema=schema,
            identifier=identifier,
            type=RelationType.View if is_view else RelationType.Table,
        )


@dataclass(frozen=True, eq=False, repr=False)
class MaxComputeInformationSchema(InformationSchema):
    quote_character: str = "`"

    @classmethod
    def get_path(cls, relation: BaseRelation, information_schema_view: Optional[str]) -> Path:
        return Path(
            database="SYSTEM_CATALOG",
            schema="INFORMATION_SCHEMA",
            identifier=information_schema_view,
        )

    @classmethod
    def get_include_policy(cls, relation, information_schema_view):
        return relation.include_policy.replace(
            database=True,
            schema=True,
            identifier=True
        )

    @classmethod
    def get_quote_policy(
            cls,
            relation,
            information_schema_view: Optional[str],
    ) -> Policy:
        return relation.quote_policy.replace(
            database=False,
            schema=False,
            identifier=False
        )
