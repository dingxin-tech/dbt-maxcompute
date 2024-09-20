from dbt.adapters.sql import SQLAdapter

from dbt.adapters.maxcompute import MaxComputeConnectionManager


class MaxComputeAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = MaxComputeConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

# may require more build out to make more user friendly to confer with team and community.
