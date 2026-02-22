import asyncio
import sys
import types
import unittest

sys.modules.setdefault("httpx", types.ModuleType("httpx"))

from app.services import remnawave_adult as svc


class FakeSession:
    def __init__(self):
        self.rollback_calls = 0

    async def rollback(self):
        self.rollback_calls += 1


class RemnawaveTxtPipelineTests(unittest.TestCase):
    def test_normalize_and_domain_safety(self):
        self.assertEqual(svc.normalize_remnawave_domain('https://Sub.Example.COM/path'), 'example.com')
        self.assertIsNone(svc.normalize_remnawave_domain('127.0.0.1'))
        self.assertFalse(svc._is_domain_db_safe('a' * 260 + '.com'))
        self.assertTrue(svc._is_domain_db_safe('example.com'))


    def test_chunk_builder_handles_tail_and_chunk_local_dedup(self):
        lines = [
            'a.one.test',
            'b.two.test',
            'a.one.test',
            'invalid_domain',
            'c.three.test',
        ]
        chunks, skipped = svc._chunk_normalized_domains(lines, chunk_size=2)
        self.assertEqual(chunks, [['one.test', 'two.test'], ['three.test']])
        self.assertEqual(skipped, 1)

    def test_bucket_routing(self):
        self.assertEqual(svc._bucket_file_for_domain('9example.com'), '0-9.txt')
        self.assertEqual(svc._bucket_file_for_domain('alpha.com'), 'a-d.txt')
        self.assertEqual(svc._bucket_file_for_domain('fox.com'), 'e-h.txt')
        self.assertEqual(svc._bucket_file_for_domain('jazz.com'), 'i-l.txt')
        self.assertEqual(svc._bucket_file_for_domain('moon.com'), 'm-p.txt')
        self.assertEqual(svc._bucket_file_for_domain('qqq.com'), 'q-t.txt')
        self.assertEqual(svc._bucket_file_for_domain('vimeo.com'), 'u-x.txt')
        self.assertEqual(svc._bucket_file_for_domain('zeta.com'), 'y-z.txt')

    def test_bucket_table_resolution_for_candidates(self):
        tables = svc._bucket_table_names_for_candidates(['alpha.com', 'vimeo.com'])
        self.assertIn('adult_domain_bucket_a_d', tables)
        self.assertIn('adult_domain_bucket_u_x', tables)
        self.assertIn('adult_domain_bucket_old', tables)

        old_only = svc._bucket_table_names_for_candidates([])
        self.assertEqual(old_only, ['adult_domain_bucket_old'])

    def test_extract_domains_and_invalid_tokens(self):
        sample = '''
        example.com
        127.0.0.1 test.net
        include:bad_token
        !!!wrong
        '''
        domains, invalid = svc._extract_domains_and_invalid_tokens(sample)
        self.assertIn('example.com', domains)
        self.assertIn('test.net', domains)
        self.assertIn('include:bad_token', invalid)

    def test_retryable_db_error_detection(self):
        self.assertTrue(svc._msg_has_retryable_db_error(Exception('deadlock detected')))
        self.assertTrue(svc._msg_has_retryable_db_error(Exception('canceling statement due to statement timeout')))
        self.assertFalse(svc._msg_has_retryable_db_error(Exception('syntax error at or near')))

    def test_run_with_db_retry_success_after_retries(self):
        calls = {'n': 0}
        db = FakeSession()

        async def flaky_op():
            calls['n'] += 1
            if calls['n'] < 3:
                raise Exception('deadlock detected')

        asyncio.run(svc._run_with_db_retry(flaky_op, db=db, op_name='flaky-op', max_attempts=4))
        self.assertEqual(calls['n'], 3)
        self.assertEqual(db.rollback_calls, 2)

    def test_run_with_db_retry_stops_on_non_retryable(self):
        calls = {'n': 0}
        db = FakeSession()

        async def broken_op():
            calls['n'] += 1
            raise Exception('syntax error at or near "foo"')

        with self.assertRaises(Exception):
            asyncio.run(svc._run_with_db_retry(broken_op, db=db, op_name='broken-op', max_attempts=4))
        self.assertEqual(calls['n'], 1)
        self.assertEqual(db.rollback_calls, 1)


if __name__ == '__main__':
    unittest.main()
