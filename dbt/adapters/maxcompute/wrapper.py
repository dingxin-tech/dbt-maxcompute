from datetime import datetime
from decimal import Decimal

from odps.dbapi import Cursor, Connection


class ConnectionWrapper(Connection):

    def cursor(self, *args, **kwargs):
        return CursorWrapper(
            self, *args, use_sqa=self._use_sqa,
            fallback_policy=self._fallback_policy,
            hints=self._hints, **kwargs
        )


class CursorWrapper(Cursor):

    def execute(self, operation, parameters=None, **kwargs):
        def replace_sql_placeholders(sql_template, values):
            if not values:
                return sql_template
            if operation.count('%s') != len(parameters):
                raise ValueError("参数数量与SQL模板中的占位符数量不匹配")
            return operation % tuple(parameters)

        def param_normalization(params):
            if not params:
                return None
            normalized_params = []
            for param in params:
                if isinstance(param, Decimal):
                    normalized_params.append(f"{param}BD")
                elif isinstance(param, datetime):
                    normalized_params.append(f"TIMESTAMP_NTZ'{param.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(param, str):
                    normalized_params.append(f"'{param}'")
                else:
                    normalized_params.append(f"{param}")
            return normalized_params

        parameters = param_normalization(parameters)
        operation = replace_sql_placeholders(operation, parameters)
        super().execute(operation)
