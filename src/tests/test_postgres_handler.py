import pytest

from utils import postgres_handler


def test_build_batch_merge_statements_are_ordered_for_idempotent_load():
    statements = postgres_handler.build_batch_merge_statements("batch-001")

    assert len(statements) >= 7
    assert "INSERT INTO dim_customer" in statements[0][0]
    assert "INSERT INTO fact_product_view" in statements[5][0]
    assert "stg_fact_product_view" in statements[5][0]
    assert all("batch_id = %s" in stmt[0] for stmt in statements if "DELETE FROM" in stmt[0] or "INSERT INTO" in stmt[0])


def test_execute_transaction_rolls_back_on_error(monkeypatch):
    class FakeCursor:
        def __init__(self):
            self.rowcount = 1

        def execute(self, statement, params=None):
            if "SELECT fail" in statement:
                raise RuntimeError("injected failure")

        def close(self):
            pass

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()
            self.rollback_called = 0
            self.commit_called = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commit_called += 1

        def rollback(self):
            self.rollback_called += 1

        def close(self):
            pass

    conn = FakeConnection()
    monkeypatch.setattr(postgres_handler, "get_postgres_connection", lambda: conn)

    with pytest.raises(RuntimeError, match="injected failure"):
        postgres_handler.execute_transaction(
            [
                ("INSERT INTO dim_customer VALUES (%s)", ("sk-1",)),
                ("SELECT fail", ()),
            ]
        )

    assert conn.rollback_called == 1
    assert conn.commit_called == 0
