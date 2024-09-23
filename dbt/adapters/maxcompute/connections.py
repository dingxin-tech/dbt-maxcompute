from contextlib import contextmanager
from dataclasses import dataclass
from dbt_common.exceptions import (
    DbtConfigError,
    DbtRuntimeError
)
from dbt.adapters.contracts.connection import Credentials, AdapterResponse

from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger

from odps import ODPS
import odps.dbapi

logger = AdapterLogger("MaxCompute")

@dataclass
class MaxComputeCredentials(Credentials):
    project: str
    endpoint: str
    accessId: str
    accessKey: str

    ALIASES = {
        'database': 'project',
        'ak': 'accessId',
        'sk': 'accessKey',
    }

    @property
    def type(self):
        """Return name of adapter."""
        return "maxcompute"

    @property
    def unique_field(self):
        return self.endpoint + '_' + self.project

    def _connection_keys(self):
        return ("project", "endpoint")


class MaxComputeConnectionManager(SQLConnectionManager):
    TYPE = "maxcompute"

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        o = ODPS(
            credentials.accessId,
            credentials.accessKey,
            project=credentials.project,
            endpoint=credentials.endpoint,
        )

        try:
            o.get_project().reload()
        except Exception as e:
            raise DbtConfigError(f"Failed to connect to MaxCompute: {str(e)}") from e

        handle = odps.dbapi.connect(o)
        connection.state = 'open'
        connection.handle = handle
        return connection

    @classmethod
    def get_response(cls, cursor):
        # FIXME：we should get 'code', 'message', 'rows_affected' from cursor
        message = "OK"
        return AdapterResponse(_message=message)

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise
            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise DbtRuntimeError(msg)
            else:
                raise DbtRuntimeError(str(exc))

    def cancel(self, connection):
        connection.handle.cancel()
