/* For examples of how to fill out the macros please refer to the postgres adapter and docs
postgres adapter macros: https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql
dbt docs: https://docs.getdbt.com/docs/contributing/building-a-new-adapter
*/

{% macro maxcompute__truncate_relation(relation) -%}
'''Removes all rows from table.'''
{% if relation.schema -%}
    truncate table {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }};
{%- else -%}
    truncate table {{ relation.database }}.{{ relation.identifier }};
{%- endif -%}
{% endmacro %}

{% macro maxcompute__rename_relation(from_relation, to_relation) -%}
'''Renames a relation in the database.'''
{% if from_relation.schema -%}
    alter table {{ from_relation.database }}.{{ from_relation.schema }}.{{ from_relation.identifier }} rename to {{ to_relation.database }}.{{ to_relation.schema }}.{{ to_relation.identifier }};
{%- else -%}
    alter table {{ from_relation.database }}.{{ from_relation.identifier }} rename to {{ to_relation.database }}.{{ to_relation.identifier }}
{%- endif -%}
{% endmacro %}


{% macro maxcompute__alter_column_type(relation,column_name,new_column_type) -%}
'''Changes column name or data type'''
{% if relation.schema -%}
    alter table {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }} change {{ column_name }} {{ column_name }} {{ new_column_type }};
{%- else -%}
    alter table {{ relation.database }}.{{ relation.identifier }} change {{ column_name }} {{ column_name }} {{ new_column_type }};
{%- endif -%}
{% endmacro %}


{% macro maxcompute__copy_grants() -%}
    {{ return(True) }}
{% endmacro %}


{% macro maxcompute__create_table_as(relation, sql) -%}
{% if relation.schema -%}
  create table if not exists {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }} as ({{ sql }});
{%- else -%}
  create table if not exists {{ relation.database }}.default.{{ relation.identifier }} as ({{ sql }});
{%- endif -%}
{%- endmacro %}

{% macro maxcompute__create_view_as(relation, sql) -%}
{% if relation.schema -%}
  create or replace view {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }} as ({{ sql }});
{%- else -%}
  create or replace view {{ relation.database }}.default.{{ relation.identifier }} as ({{ sql }});
{%- endif -%}
{%- endmacro %}

