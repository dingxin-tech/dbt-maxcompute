import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_equals import BaseEquals
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_null_compare import BaseNullCompare, BaseMixedNullCompare
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod


class TestAnyValueMaxCompute(BaseAnyValue):
    pass


class TestArrayAppendMaxCompute(BaseArrayAppend):
    pass


class TestArrayConcatMaxCompute(BaseArrayConcat):
    pass


class TestArrayConstructMaxCompute(BaseArrayConstruct):
    pass


class TestBoolOrMaxCompute(BaseBoolOr):
    pass


class TestCastMaxCompute(BaseCast):
    pass


class TestCastBoolToTextMaxCompute(BaseCastBoolToText):
    pass


class TestConcatMaxCompute(BaseConcat):
    pass


class TestCurrentTimestampMaxCompute(BaseCurrentTimestampNaive):
    # Since maxcompute returns pandas.Timestamp for timestamp type we need to manually convert it to datetime.Datetime
    @pytest.fixture(scope="class")
    def current_timestamp(self, project):
        from dbt.tests.util import relation_from_name, run_dbt

        run_dbt(["build"])
        relation = relation_from_name(project.adapter, "current_ts")
        result = project.run_sql(f"select current_ts_column from {relation}", fetch="one")
        sql_timestamp = result[0] if result is not None else None
        return sql_timestamp.to_pydatetime()


class TestDateMaxCompute(BaseDate):
    pass


class TestDateSpineMaxCompute(BaseDateSpine):
    # In this case, we need to modify the test SQL,
    # because in MaxCompute, 'YYYY-MM-DD' is just a STRING and cannot be automatically converted to DATE.

    models__test_date_spine_sql = """
    with generated_dates as (
        {{ date_spine("day", "DATE'2023-09-01'", "DATE'2023-09-10'") }}
    ), expected_dates as (
            select DATE'2023-09-01' as expected
            union all
            select DATE'2023-09-02' as expected
            union all
            select DATE'2023-09-03' as expected
            union all
            select DATE'2023-09-04' as expected
            union all
            select DATE'2023-09-05' as expected
            union all
            select DATE'2023-09-06' as expected
            union all
            select DATE'2023-09-07' as expected
            union all
            select DATE'2023-09-08' as expected
            union all
            select DATE'2023-09-09' as expected
    ), joined as (
        select
            generated_dates.date_day,
            expected_dates.expected
        from generated_dates
        left join expected_dates on generated_dates.date_day = expected_dates.expected
    )
    SELECT * from joined
    """

    @pytest.fixture(scope="class")
    def models(self):
        from dbt.tests.adapter.utils import fixture_date_spine

        return {
            "test_date_spine.yml": fixture_date_spine.models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                self.models__test_date_spine_sql, "date_spine"
            ),
        }

    pass


class TestDateTruncMaxCompute(BaseDateTrunc):
    pass


class TestDateAddMaxCompute(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                            "interval_length": "bigint",
                        },
                    },
                },
            },
        }

    pass


class TestDateDiffMaxCompute(BaseDateDiff):
    models__test_datediff_sql = """
    with data as (

        select * from {{ ref('data_datediff') }}

    )

    select

        case
            when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
            when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
            when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
            when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
            when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
            when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
            when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
            else null
        end as actual,
        result as expected

    from data

    -- Also test correct casting of literal values.

    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "microsecond") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "millisecond") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "second") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "minute") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "hour") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "day") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-03 00:00:00.000000'", "week") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "month") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "quarter") }} as actual, 1 as expected
    union all select {{ datediff("TIMESTAMP'1999-12-31 23:59:59.999999'", "TIMESTAMP'2000-01-01 00:00:00.000000'", "year") }} as actual, 1 as expected
    """

    @pytest.fixture(scope="class")
    def models(self):
        from dbt.tests.adapter.utils import base_utils, fixture_datediff

        return {
            "test_datediff.yml": fixture_datediff.models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                self.models__test_datediff_sql, "datediff"
            ),
        }

    pass


class TestEqualsMaxCompute(BaseEquals):
    pass


class TestEscapeSingleQuotesMaxCompute(BaseEscapeSingleQuotesBackslash):
    pass


class TestExceptMaxCompute(BaseExcept):
    pass


class TestGenerateSeriesMaxCompute(BaseGenerateSeries):
    pass


class TestGetIntervalsBetweenMaxCompute(BaseGetIntervalsBetween):
    models__test_get_intervals_between_sql = """
    SELECT
      {{ get_intervals_between("DATE'2023-09-01'", "DATE'2023-09-12'", "day") }} as intervals,
      11 as expected
    """

    @pytest.fixture(scope="class")
    def models(self):
        from dbt.tests.adapter.utils import fixture_get_intervals_between

        return {
            "test_get_intervals_between.yml": fixture_get_intervals_between.models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                self.models__test_get_intervals_between_sql,
                "get_intervals_between",
            ),
        }


class TestGetPowersOfTwoMaxCompute(BaseGetPowersOfTwo):
    pass


class TestHashMaxCompute(BaseHash):
    pass


class TestIntersectMaxCompute(BaseIntersect):
    pass


# not support date_part 'quarter'
class TestLastDayMaxCompute(BaseLastDay):
    seeds__data_last_day_csv = """date_day,date_part,result
2018-01-02,month,2018-01-31
2018-01-02,day,2018-01-02
2018-01-02,year,2018-12-31
,month,
"""
    models__test_last_day_sql = """
    with data as (

        select * from {{ ref('data_last_day') }}

    )

    select
        case
            when date_part = 'month' then {{ last_day('date_day', 'month') }}
            when date_part = 'day' then {{ last_day('date_day', 'day') }}
            when date_part = 'year' then {{ last_day('date_day', 'year') }}
            else null
        end as actual,
        result as expected

    from data
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": self.seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        from dbt.tests.adapter.utils import fixture_last_day

        return {
            "test_last_day.yml": fixture_last_day.models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                self.models__test_last_day_sql, "last_day"
            ),
        }


class TestLengthMaxCompute(BaseLength):
    pass


# Note: Not Support Limit and Distinct
class TestListaggMaxCompute(BaseListagg):
    seeds__data_listagg_output_csv = """group_col,expected,version
1,"a_|_b_|_c",bottom_ordered
2,"1_|_a_|_p",bottom_ordered
3,"g_|_g_|_g",bottom_ordered
3,"g, g, g",comma_whitespace_unordered
3,"g,g,g",no_params
"""

    models__test_listagg_sql = """
    with data as (

        select * from {{ ref('data_listagg') }}

    ),

    data_output as (

        select * from {{ ref('data_listagg_output') }}

    ),

    calculate as (

        select
            group_col,
            {{ listagg('string_text', "'_|_'", "order by order_col") }} as actual,
            'bottom_ordered' as version
        from data
        group by group_col

        union all

        select
            group_col,
            {{ listagg('string_text', "', '") }} as actual,
            'comma_whitespace_unordered' as version
        from data
        where group_col = 3
        group by group_col

        union all

        select
            group_col,
            {{ listagg('string_text') }} as actual,
            'no_params' as version
        from data
        where group_col = 3
        group by group_col

    )

    select
        calculate.actual,
        data_output.expected
    from calculate
    left join data_output
    on calculate.group_col = data_output.group_col
    and calculate.version = data_output.version
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        from dbt.tests.adapter.utils import fixture_listagg

        return {
            "data_listagg.csv": fixture_listagg.seeds__data_listagg_csv,
            "data_listagg_output.csv": self.seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        from dbt.tests.adapter.utils import fixture_listagg

        return {
            "test_listagg.yml": fixture_listagg.models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                self.models__test_listagg_sql, "listagg"
            ),
        }


@pytest.mark.skip(reason="Not support create NULL Column yet.")
class TestMixedNullCompareMaxCompute(BaseMixedNullCompare):
    pass


@pytest.mark.skip(reason="Not support create NULL Column yet.")
class TestNullCompareMaxCompute(BaseNullCompare):
    pass


class TestPositionMaxCompute(BasePosition):
    pass


class TestReplaceMaxCompute(BaseReplace):
    pass


class TestRightMaxCompute(BaseRight):
    pass


class TestSafeCastMaxCompute(BaseSafeCast):
    pass


class TestSplitPartMaxCompute(BaseSplitPart):
    pass


class TestStringLiteralMaxCompute(BaseStringLiteral):
    pass


@pytest.mark.skip(reason="Useless test, Timestamp micros have been depercated")
class TestCurrentTimestampsMaxCompute(BaseCurrentTimestamps):
    pass


class TestValidateSqlMethodMaxCompute(BaseValidateSqlMethod):

    @pytest.fixture(scope="class")
    def valid_sql(self) -> str:
        return "select 1;"

    pass
