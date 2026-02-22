import asyncio
import unittest

from app.services import remnawave_adult as adult


class _ScalarsResult:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values


class _Result:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return _ScalarsResult(self._values)


class _FakeSession:
    def __init__(self):
        self.update_stmt = None

    async def execute(self, stmt, params=None):
        # First execute: select rows for recheck.
        if params is None:
            return _Result(["_invalid_domain_"])
        # Second execute: bulk update.
        self.update_stmt = stmt
        return _Result([])

    async def commit(self):
        return None

    async def rollback(self):
        return None


class RecheckBulkUpdateTests(unittest.TestCase):
    def test_bulk_update_disables_session_synchronization(self):
        original_ensure_schema = adult._ensure_adult_schema

        async def _schema_ready():
            return True

        adult._ensure_adult_schema = _schema_ready
        db = _FakeSession()
        try:
            processed = asyncio.run(adult._process_dns_unique_recheck_batch(db, limit=10))
        finally:
            adult._ensure_adult_schema = original_ensure_schema

        self.assertEqual(processed, 1)
        self.assertIsNotNone(db.update_stmt)
        self.assertIs(db.update_stmt.get_execution_options().get("synchronize_session"), False)


if __name__ == "__main__":
    unittest.main()
