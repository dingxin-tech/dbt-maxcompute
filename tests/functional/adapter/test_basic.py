import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsMaxCompute(BaseSimpleMaterializations):
    # passed
    pass


class TestSingularTestsMaxCompute(BaseSingularTests):
    # passed
    pass


class TestSingularTestsEphemeralMaxCompute(BaseSingularTestsEphemeral):
    # unsupported operand type(s) for +=: 'NoneType' and 'str'
    pass


class TestEmptyMaxCompute(BaseEmpty):
    # passed
    pass


class TestEphemeralMaxCompute(BaseEphemeral):
    # unsupported operand type(s) for +=: 'NoneType' and 'str'
    pass


class TestIncrementalMaxCompute(BaseIncremental):
    # passed
    pass


class TestGenericTestsMaxCompute(BaseGenericTests):
    # passed
    pass


class TestSnapshotCheckColsMaxCompute(BaseSnapshotCheckCols):
    # merge into target table must be transactional table
    pass


class TestSnapshotTimestampMaxCompute(BaseSnapshotTimestamp):
    # merge into target table must be transactional table
    pass


class TestBaseAdapterMethodMaxCompute(BaseAdapterMethod):
    # passed
    pass
