import unittest

from app.services import ingest_metrics as im


class _FakeSyncRedis:
    def __init__(self):
        self.data = {}

    def hincrby(self, key, field, amount):
        bucket = self.data.setdefault(key, {})
        bucket[field] = int(bucket.get(field, 0)) + int(amount)

    def hgetall(self, key):
        bucket = self.data.get(key, {})
        return {k: str(v) for k, v in bucket.items()}


class SharedIngestMetricsTests(unittest.TestCase):
    def test_rsyslog_snapshot_uses_shared_counters(self):
        fake = _FakeSyncRedis()
        original_get_sync_redis = im._get_sync_redis

        def _fake_get_sync_redis():
            return fake

        im._get_sync_redis = _fake_get_sync_redis
        try:
            im.record_rsyslog_enqueue(10)
            im.record_rsyslog_result(
                validated_ok=8,
                processed_ok=8,
                inserted_new=3,
                deduplicated=5,
                rejected=2,
                reject_reasons={"invalid_ts": 2},
            )
            im.record_rsyslog_failed(1)
            im.record_rsyslog_retried(4)

            snap = im.get_ingest_metrics_snapshot()["rsyslog"]
        finally:
            im._get_sync_redis = original_get_sync_redis

        self.assertEqual(snap["received"], 10)
        self.assertEqual(snap["processed_ok"], 8)
        self.assertEqual(snap["inserted_new"], 3)
        self.assertEqual(snap["deduplicated"], 5)
        self.assertEqual(snap["rejected"], 2)
        self.assertEqual(snap["failed"], 1)
        self.assertEqual(snap["retried"], 4)
        self.assertTrue(any(x["reason"] == "invalid_ts" and x["count"] == 2 for x in snap["reject_reasons"]))


if __name__ == "__main__":
    unittest.main()
