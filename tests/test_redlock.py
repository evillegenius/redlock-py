import unittest

from redlock import (Redlock,
                     MultipleRedlockException,
                     QuorumError,
                     OwnerError,
                     MissingError)

class TestRedlock(unittest.TestCase):

    def setUp(self):
        self.redlock = Redlock([{"host": "localhost"}])

    def test_lock(self):
        lock = self.redlock.lock("pants", 100)
        self.assertEqual(lock.resource, b"pants")
        self.redlock.unlock(lock)
        lock = self.redlock.lock("pants", 10)
        self.redlock.unlock(lock)

    def test_blocked(self):
        lock = self.redlock.lock("pants", 1000)
        bad = self.redlock.lock("pants", 10)
        self.assertFalse(bad)
        self.redlock.unlock(lock)

    def test_bad_connection_info(self):
        with self.assertRaises(QuorumError):
            Redlock([{"cat": "hog"}])

    def test_py3_compatible_encoding(self):
        lock = self.redlock.lock("pants", 1000)
        key = self.redlock.servers[0].get(b"pants")
        self.redlock.unlock(lock)
        self.assertEqual(lock.key, key)

    def test_ttl_not_int_trigger_exception_value_error(self):
        with self.assertRaises(TypeError):
            self.redlock.lock("pants", 1000.0)

    def test_multiple_redlock_exception(self):
        ex1 = Exception("Redis connection error")
        ex2 = Exception("Redis command timed out")
        exc = MultipleRedlockException([ex1, ex2])
        exc_str = str(exc)
        self.assertIn('connection error', exc_str)
        self.assertIn('command timed out', exc_str)

    def test_query(self):
        import time
        lock = self.redlock.lock("pants", 100)
        lock2 = self.redlock.query("pants")
        self.assertTrue(lock2.validity <= lock.validity)
        self.assertEqual(lock2.resource, lock.resource)
        self.assertEqual(lock2.key, lock.key)

        time.sleep(0.010)

        lock2 = self.redlock.query("pants")
        self.assertTrue(lock2.validity < lock.validity)
        self.assertEqual(lock2.resource, lock.resource)
        self.assertEqual(lock2.key, lock.key)

        self.redlock.unlock(lock)

    def test_extend(self):
        lock = self.redlock.lock("pants", 100)
        lock2 = self.redlock.extend(lock, 1000)
        self.assertTrue(lock2.validity > lock.validity)
        self.assertEqual(lock2.resource, lock.resource)
        self.assertEqual(lock2.key, lock.key)
        self.redlock.unlock(lock)

if __name__ == '__main__':
    unittest.main()
