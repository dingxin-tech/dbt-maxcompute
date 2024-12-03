import pytest
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants


@pytest.mark.skip(
    reason="Please use webconsole/pyodps for permission operations. Dbt will not perform any modification operations."
)
class TestSeedGrants(BaseSeedGrants):
    pass
