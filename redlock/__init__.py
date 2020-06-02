import string
import random
import time
import warnings
from collections import namedtuple

import redis
from redis.exceptions import RedisError

# Python 3 compatibility
string_type = getattr(__builtins__, 'basestring', str)

try:
    basestring
except NameError:
    basestring = str


Lock = namedtuple("Lock", ("validity", "resource", "key"))


class RedlockException(Exception):
    pass

class MultipleRedlockException(RedlockException):
    """
    Wrap possibly multiple exceptions that may have occurred among the quorum of
    servers. Generally the errors will be redis.exception.RedisError objects but
    during Redlock initialization, the errors could include any error returned
    by attempting to create and verify a redis connection.
    """
    def __init__(self, errors, *args, **kwargs):
        super(MultipleRedlockException, self).__init__(*args, **kwargs)
        self.errors = errors

    def __str__(self):
        return ' :: '.join([str(e) for e in self.errors])

    def __repr__(self):
        return self.__str__()

class QuorumError(MultipleRedlockException):
    """Failed to obtain a quorum of Redis servers."""
    pass

class LockError(RedlockException):
    """
    Base class for failures to lock, extend, or unlock a resource that is not
    caused by underlying redis server failures..
    """
    pass

class OwnerError(LockError):
    """Attempt to operate on a lock owned by another key."""
    pass

class MissingError(LockError):
    """Attempt to operate on a missing or expired lock"""
    pass

class Redlock(object):
    default_retry_count = 3
    default_retry_delay = 0.2

    # Return 1 == "unlocked", 0 == "no such key", -1 == "not lock owner"
    unlock_script = """
    local keyval = redis.call("get",KEYS[1])
    if keyval == ARGV[1] then
        return redis.call("del",KEYS[1])
    elseif keyval == false then
        return 0
    else
        return -1
    end"""

    # Return 1 == "extended", 0 == "no such key", -1 == "not lock owner"
    extend_script = """
    local keyval = redis.call("get",KEYS[1])
    if keyval == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    elseif keyval == false then
        return 0
    else
        return -1
    end"""

    # Return a tuple of (ttl, resource, key). If the resource is not locked
    # return (ttl <= 0, resource, None)
    query_script = """
    return {redis.call("pttl",KEYS[1]), KEYS[1], redis.call("get",KEYS[1])}
    """

    def __init__(self, connection_list, retry_count=None, retry_delay=None):
        self.servers = []
        redis_errors = []
        for connection_info in connection_list:
            try:
                if isinstance(connection_info, string_type):
                    server = redis.StrictRedis.from_url(connection_info)
                elif type(connection_info) == dict:
                    server = redis.StrictRedis(**connection_info)
                else:
                    server = connection_info

                # Connecting to a non-existent server still returns a valid
                # server object. We have to test that the server is actually
                # there. If not, ping will raise an RedisError
                if server.ping():
                    self.servers.append(server)
            except Exception as e:
                # Defer reporting of all exceptions until we know if we got a
                # quorum or not.
                redis_errors.append(e)

        self.quorum = (len(connection_list) // 2) + 1

        if len(self.servers) < self.quorum:
            raise QuorumError(
                redis_errors,
                "Failed to connect to a quorum of redis servers")

        # We got a quorum but still want to report failure messages.
        self._warn(redis_errors)

        self.retry_count = retry_count or self.default_retry_count
        self.retry_delay = retry_delay or self.default_retry_delay

    def _warn(self, redis_errors):
        """Convert a list of RedisError objects into warnings."""
        for error in redis_errors:
            warnings.warn('{}: {}'.format(type(error).__name__, error))

    def lock_instance(self, server, resource, key, ttl):
        # Note: returns True or None
        if not isinstance(ttl, int):
            raise TypeError('ttl {!r} is not an integer'.format(ttl))
        return server.set(resource, key, nx=True, px=ttl)

    def unlock_instance(self, server, resource, key):
        # Note: returns 1 == Success, 0 = No such resource, -1 = not lock owner
        return server.eval(self.unlock_script, 1, resource, key)
            
    def extend_instance(self, server, resource, key, ttl):
        # Note: returns 1 == Success, 0 = No such resource, -1 = not lock owner
        return server.eval(self.extend_script, 1, resource, key, ttl)
     
    def query_instance(self, server, resource):
        values = server.eval(self.query_script, 1, resource)
        # clamp the ttl value to 0
        return Lock(max(values[0], 0), values[1], values[2])

    def get_unique_id(self):
        CHARACTERS = string.ascii_letters + string.digits
        return ''.join(random.choice(CHARACTERS) for _ in range(22)).encode()

    def lock(self, resource, ttl, key=None):
        retry = 0
        if key is None:
            key = self.get_unique_id()
        elif hasattr(key, 'encode'):
            key = key.encode()

        # Values in redis are binary
        if hasattr(resource, 'encode'):
            resource = resource.encode()

        redis_errors = []
        while retry < self.retry_count:
            n = 0
            start_time = int(time.time() * 1000)
            del redis_errors[:]
            for server in self.servers:
                try:
                    if self.lock_instance(server, resource, key, ttl):
                        n += 1
                except RedisError as e:
                    redis_errors.append(e)
            elapsed_time = int(time.time() * 1000) - start_time
            validity = int(ttl - elapsed_time)
            if validity > 0 and n >= self.quorum:
                # We got a quorum but *may* have seen some errors. If so, warn
                # the user about them.
                self._warn(redis_errors)
                return Lock(validity, resource, key)
            else:
                for server in self.servers:
                    try:
                        self.unlock_instance(server, resource, key)
                    except:
                        pass
                retry += 1
                time.sleep(self.retry_delay)

        if redis_errors:
            raise MultipleRedlockException(redis_errors)

        return False

    def unlock(self, lock):
        redis_errors = []
        unlocked = missing = notowned = 0
        for server in self.servers:
            try:
                result = self.unlock_instance(server, lock.resource, lock.key)
                if result == 1: # success
                    unlocked += 1
                elif result == 0: # no such resource (or resource expired)
                    missing += 1
                else: # result == -1, not lock owner.
                    notowned += 1
            except RedisError as e:
                redis_errors.append(e)

        if unlocked:
            # If we unlocked any servers then we had a valid lock (or partial
            # lock) and now we do not. Declare victory.
            self._warn(redis_errors)
            return True
        elif redis_errors:
            # Something failed internal to redis.
            raise MultipleRedlockException(redis_errors)
        elif notowned:
            # Some other key has a lock (or partial lock). Raise OwnerError.
            raise OwnerError("The resource {} is locked with a different key."
                             .format(lock.resource))
        else:
            # The lock was not owned by us or by anyone else. Raise MissingError.
            raise MissingError("The resource {} is not locked."
                               .format(lock.resource))

    def extend(self, lock, ttl):
        redis_errors = []
        extended = missint = notowned = 0
        start_time = int(time.time() * 1000)
        for server in self.servers:
            try:
                result = self.extend_instance(server, lock.resource, lock.key, ttl)
                if result == 1:
                    # The lock was extended
                    extended += 1
                elif result == 0:
                    missing += 1
                elif result == -1:
                    notowned += 1
            except RedisError as e:
                redis_errors.append(e)

        if extended >= self.quorum:
            # If we extended a quorum of servers then declare victory.
            self._warn(redis_errors)
            elapsed_time = int(time.time() * 1000) - start_time
            validity = int(ttl - elapsed_time)
            return Lock(validity, lock.resource, lock.key)
        elif redis_errors:
            # Something failed internal to redis.
            raise MultipleRedlockException(redis_errors)
        elif notowned:
            # Some other key has the lock (or partial lock). Raise OwnerError.
            # If we extended the ttl on some servers before noticing that we
            # have lost our quorum, those keys may stick around messing things
            # up. Since we don't own the lock, release it entirely.
            for server in self.servers:
                try:
                    self.unlock_instance(server, lock.resource, lock.key)
                except:
                    pass
            raise OwnerError("The resource {} is locked with a different key."
                             .format(lock.resource))
        else:
            # The lock was not owned by us or by anyone else. Raise MissingError.
            raise MissingError("The resource {} is not locked."
                               .format(lock.resource))
        
    def query(self, resource):
        # Values in redis are binary, convert resource if necessary
        if hasattr(resource, 'encode'):
            resource = resource.encode()

        redis_errors = []
        locks = []
        for server in self.servers:
            try:
                lock = self.query_instance(server, resource)
            except RedisError as e:
                redis_errors.append(e)
                lock = Lock(0, resource, None)
            locks.append(lock)

        if redis_errors:
            raise MultipleRedlockException(redis_errors)

        # We may have different values in the list of locks, see if any of them
        # have a quorum. Gather them by key.
        counts = dict()
        for lock in locks:
            if lock.key in counts:
                counts[lock.key] += 1
            else:
                counts[lock.key] = 1

        maxCount, maxKey = max(((count, key) for key, count in counts.items()))

        if maxCount >= self.quorum:
            # We have a winner. Figure out the ttl time.
            locks = [lock for lock in locks if lock.key == maxKey]
            # Sort by ttl.
            locks.sort()
            # Return the lock whose expiration would cause us to lose our
            # quorum.
            return locks[-self.quorum]

        # No one owns a quorum
        return Lock(0, resource, None)
        
