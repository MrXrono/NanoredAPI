import asyncio
import uuid
import unittest

from app.api.v1 import admin
from app.services import remnawave_adult_task_queue as tq


class _FakeRedisStatus:
    def __init__(self, summary, consumers):
        self._summary = summary
        self._consumers = consumers

    async def xpending(self, stream, group):
        return self._summary

    async def xinfo_consumers(self, stream, group):
        return self._consumers


class _FakeRedisQueue:
    def __init__(self):
        self.acked = []
        self.deleted = []

    async def xack(self, stream, group, msg_id):
        self.acked.append((stream, group, msg_id))

    async def xdel(self, stream, msg_id):
        self.deleted.append((stream, msg_id))


class WorkerStatusTests(unittest.TestCase):
    def test_xpending_dict_and_matched_consumer(self):
        fake = _FakeRedisStatus(
            summary={"pending": 5},
            consumers=[{"name": "worker-1", "pending": 2, "idle": 1000}],
        )

        original_get_redis = admin.get_redis

        async def _fake_get_redis():
            return fake

        admin.get_redis = _fake_get_redis
        try:
            result = asyncio.run(admin._stream_worker_status("stream", "group", "worker-1"))
        finally:
            admin.get_redis = original_get_redis

        self.assertEqual(result["pending"], 2)
        self.assertEqual(result["state"], "running")

    def test_no_consumers_with_pending_is_pending_not_disconnected(self):
        fake = _FakeRedisStatus(summary={"pending": 3}, consumers=[])

        original_get_redis = admin.get_redis

        async def _fake_get_redis():
            return fake

        admin.get_redis = _fake_get_redis
        try:
            result = asyncio.run(admin._stream_worker_status("stream", "group", "worker-1"))
        finally:
            admin.get_redis = original_get_redis

        self.assertEqual(result["pending"], 3)
        self.assertEqual(result["state"], "pending")


class WorkerCancelTests(unittest.TestCase):
    def test_sync_task_cancelled_marks_final_status(self):
        fake_redis = _FakeRedisQueue()
        updates = []

        original_get_redis = tq.get_redis
        original_create = tq._create_task_run
        original_update = tq._update_task_run
        original_sync = tq.sync_adult_catalog

        async def _fake_get_redis():
            return fake_redis

        async def _fake_create(**kwargs):
            return uuid.uuid4()

        async def _fake_update(run_id, **kwargs):
            updates.append(kwargs)

        async def _fake_sync(progress_cb=None):
            raise asyncio.CancelledError()

        tq.get_redis = _fake_get_redis
        tq._create_task_run = _fake_create
        tq._update_task_run = _fake_update
        tq.sync_adult_catalog = _fake_sync

        try:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(tq._process_sync_message("1-0", {}))
        finally:
            tq.get_redis = original_get_redis
            tq._create_task_run = original_create
            tq._update_task_run = original_update
            tq.sync_adult_catalog = original_sync

        self.assertTrue(any(u.get("status") == "cancelled" and u.get("finished") is True for u in updates))
        self.assertEqual(len(fake_redis.acked), 1)
        self.assertEqual(len(fake_redis.deleted), 1)


if __name__ == "__main__":
    unittest.main()
