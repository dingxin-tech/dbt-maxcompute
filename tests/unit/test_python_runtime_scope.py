from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.maxcompute.context import GLOBAL_SQL_HINTS
from dbt.adapters.maxcompute.impl import MaxComputeAdapter
from dbt.adapters.maxcompute.wrapper import ConnectionWrapper, MaxQAConfig


@contextmanager
def acquired_connection(*args):
    yield


def make_adapter(hints):
    adapter = object.__new__(MaxComputeAdapter)
    handle = ConnectionWrapper(odps=MagicMock(), hints=hints)
    adapter.connections = MagicMock()
    adapter.connections.get_thread_connection.return_value = SimpleNamespace(handle=handle)
    return adapter, handle


def submitted_hints(handle):
    handle.cursor().execute("select 1")
    return handle.odps.execute_sql.call_args.kwargs["hints"]


@pytest.mark.parametrize("dependencies", [None, [], ["model.project.input"]])
def test_ordinary_sql_keeps_legacy_runtime_and_lazy_connection(dependencies):
    assert "odps.sql.python.version" not in GLOBAL_SQL_HINTS
    hints = {"odps.sql.python.version": "cp27"}
    adapter, handle = make_adapter(hints)
    node = (
        None
        if dependencies is None
        else SimpleNamespace(depends_on=SimpleNamespace(nodes=dependencies))
    )
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        with adapter.connection_named("legacy", node):
            assert handle._hints == hints
    adapter.connections.get_thread_connection.assert_not_called()


@pytest.mark.parametrize("fail", [False, True])
def test_function_call_runtime_is_scoped_even_when_execution_fails(fail):
    hints = {"odps.sql.python.version": "cp37", "other": "unchanged"}
    adapter, handle = make_adapter(hints)
    previous_hints = handle._hints
    node = SimpleNamespace(depends_on=SimpleNamespace(nodes=["function.project.double_value"]))
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        try:
            with adapter.connection_named("function_caller", node):
                assert submitted_hints(handle) == {
                    **hints,
                    "odps.sql.python.version": "cp311",
                }
                if fail:
                    raise RuntimeError("query failed")
        except RuntimeError:
            assert fail
    assert handle._hints is previous_hints
    assert submitted_hints(handle) == hints
    adapter.connections.get_thread_connection.assert_not_called()


def test_nested_runtime_scopes_restore_each_callers_defaults():
    _, handle = make_adapter({"other": "unchanged"})
    with handle.python_runtime("cp311"):
        with handle.python_runtime("cp37"):
            assert submitted_hints(handle)["odps.sql.python.version"] == "cp37"
        assert submitted_hints(handle)["odps.sql.python.version"] == "cp311"
    assert submitted_hints(handle) == {"other": "unchanged"}


@pytest.mark.parametrize("explicit_runtime", [None, "cp27", "cp37"])
def test_explicit_sql_runtime_reaches_pyodps_unchanged(explicit_runtime):
    client = MagicMock()
    handle = ConnectionWrapper(odps=client, hints={"other": "unchanged"})
    statement = "select 1"
    if explicit_runtime:
        statement = f"set odps.sql.python.version={explicit_runtime}; {statement}"
    with handle.python_runtime("cp311"):
        handle.cursor().execute(statement)
    assert client.execute_sql.call_args.kwargs["hints"] == {
        "other": "unchanged",
        "odps.sql.python.version": explicit_runtime or "cp311",
    }


def test_inline_function_references_without_resolved_dependencies():
    adapter, handle = make_adapter({})
    node = SimpleNamespace(functions=[["double_value"]], depends_on=SimpleNamespace(nodes=[]))
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        with adapter.connection_named("inline_query", node):
            assert submitted_hints(handle)["odps.sql.python.version"] == "cp311"


@pytest.mark.parametrize("materialization", ["ephemeral", "view", "table"])
def test_runtime_follows_query_dependencies_but_stops_at_persisted_tables(materialization):
    adapter, handle = make_adapter({})
    parent = SimpleNamespace(
        config=SimpleNamespace(materialized=materialization),
        depends_on=SimpleNamespace(nodes=["function.project.double_value"]),
    )
    adapter._macro_resolver = SimpleNamespace(nodes={"model.project.parent": parent})
    node = SimpleNamespace(depends_on=SimpleNamespace(nodes=["model.project.parent"]))
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        with adapter.connection_named("consumer", node):
            hints = submitted_hints(handle)
            assert hints.get("odps.sql.python.version") == (
                None if materialization == "table" else "cp311"
            )


def test_dependency_cycle_without_a_function_does_not_change_runtime():
    adapter, handle = make_adapter({})
    node = SimpleNamespace(
        config=SimpleNamespace(materialized="ephemeral"),
        depends_on=SimpleNamespace(nodes=["model.project.cycle"]),
    )
    adapter._macro_resolver = SimpleNamespace(nodes={"model.project.cycle": node})
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        with adapter.connection_named("cycle", node):
            assert "odps.sql.python.version" not in submitted_hints(handle)


def test_model_runtime_hint_also_applies_to_inline_show_queries():
    adapter, handle = make_adapter({})
    node = SimpleNamespace(
        config={"sql_hints": {"odps.sql.python.version": "cp37"}},
        functions=[["legacy_function"]],
    )
    with patch.object(SQLAdapter, "connection_named", acquired_connection):
        with adapter.connection_named("show", node):
            assert submitted_hints(handle)["odps.sql.python.version"] == "cp37"


@pytest.mark.parametrize("explicit_runtime", [None, "cp27", "cp37"])
def test_runtime_and_explicit_overrides_reach_maxqa(explicit_runtime):
    client = MagicMock()
    handle = ConnectionWrapper(odps=client, hints={}, maxqa_config=MaxQAConfig())
    statement = "select 1"
    if explicit_runtime:
        statement = f"set odps.sql.python.version={explicit_runtime}; {statement}"
    with handle.python_runtime("cp311"):
        handle.cursor().execute(statement)
    assert client.execute_sql_interactive.call_args.kwargs["hints"] == {
        "odps.sql.python.version": explicit_runtime or "cp311"
    }
