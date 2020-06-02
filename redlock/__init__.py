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
    """
    Base class for all exceptions raised by the redlock package.
    """
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
    """
    A Redlock class is the manager allowing you to lock, extend, and unlock
    resources using a quorum of redis servers.
    """
    default_retry_count = 3
    default_retry_delay = 0.2

    # Lua script to execute on the redis server to unlock a resource.  Return
    # 1 == "unlocked", 0 == "no such key", -1 == "not lock owner"
    unlock_script = """
    local keyval = redis.call("get",KEYS[1])
    if keyval == ARGV[1] then
        return redis.call("del",KEYS[1])
    elseif keyval == false then
        return 0
    else
        return -1
    end"""

    # Lua script to execute on the redis server to extend the lease on a
    # resource.  Return 1 == "extended", 0 == "no such key", -1 == "not lock
    # owner"
    extend_script = """
    local keyval = redis.call("get",KEYS[1])
    if keyval == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    elseif keyval == false then
        return 0
    else
        return -1
    end"""

    # Lua script to execute on the redis server to query a resource lock.
    # Return a tuple of (ttl, resource, key). If the resource is not locked
    # return (ttl <= 0, resource, None).
    query_script = """
    return {redis.call("pttl",KEYS[1]), KEYS[1], redis.call("get",KEYS[1])}
    """

    def __init__(self, connection_list, retry_count=None, retry_delay=None):
        """
        Initialize the Redlock instance with a list of connections, and optionally
        retry_count and retry_delay values.

        The items in the connection_list must be one of:
          * a string containing a redis connection url, see
            redis.StrictRedis.from_url()
          * a dict containing keyword arguments for redis.StrictRedis()
          * an already connected redis.StrictRedis or redis.Redis instance.

        If Redlock cannot ping a quorum of these connections (a strict majority
        of them) then it will raise a QuorumError, otherwise it will be ready to
        work with the quorum of servers to which it could connect.

        When creating a lock, retry_count is the number of times that the server
        will try (not actually retry) to obtain a lock. If retry_count = 1 then
        only 1 attempt will be made to acquire a lock. If retry_count > 1 then
        retry_delay is the approximate time in seconds to wait between
        attempts. Approximate because the value is randomly perterbed to avoid
        repeated contention should multiple clients attempt to acquire the lock
        at the same time. If retry_count and/or retry_delay are not provided,
        they default to trying 3 times with a delay of 0.2 seconds between
        attempts.
        """
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
        """
        Acquire the lock on an individual server instance.

        Returns True or None
        """
        if not isinstance(ttl, int):
            raise TypeError('ttl {!r} is not an integer'.format(ttl))
        return server.set(resource, key, nx=True, px=ttl)

    def unlock_instance(self, server, resource, key):
        """
        Release the lock on an individual server instance.

        Returns 1 == success, 0 == "no such resource", -1 == not lock owner.
        """
        return server.eval(self.unlock_script, 1, resource, key)

    def extend_instance(self, server, resource, key, ttl):
        """
        Extend the lease on the lock on an individual server instance.

        Returns 1 == success, 0 == "no such resource", -1 == not lock owner.
        """
        return server.eval(self.extend_script, 1, resource, key, ttl)
     
    def query_instance(self, server, resource):
        """
        Query the current state of a lock on an individual server instance.

        Returns Lock(ttl, resource, key). If Lock.ttl == 0 then the resource is
        no longer locked.

        Note: Unlocked resources will typically return a value of None for
        Lock.key, but if the resource has been randomly set on the redis server
        by some other process then it may return the value of the resource as
        the key or it may even raise an exception if the resource is not a
        string.
        """
        values = server.eval(self.query_script, 1, resource)
        # clamp the ttl value to 0
        return Lock(max(values[0], 0), values[1], values[2])

    def get_unique_id(self):
        """
        Provide a random unique string to be used as a key when the user did not
        provide an explicit key to use when locking a resource. The string is
        made up of a random sequence of ASCII letters and digits.
        """
        CHARACTERS = string.ascii_letters + string.digits
        return ''.join(random.choice(CHARACTERS) for _ in range(22)).encode()

    def lock(self, resource, ttl, key=None):
        """
        Attempt to acquire a lock on a resource. The resource is actually the name
        of a string valued key on a quorum of redis servers, so make sure to
        pick a name that will not be used for other purposes on any of the
        servers. If the resource name contains unicode text, it will be encoded
        with utf-8 before being used on the redis server.

        ttl is a "time to live" value as an integer number of milliseconds that
        is the lease time on the lock. You may specify a large value, but there
        are no perpetual locks and the value must be an integer. If the lease
        expires, the redis servers will remove the resource key and the resource
        will silently become unlocked. This is beneficial in the case of
        crashing processes to ensure that the system will not become blocked
        indefinitely, but it can be bad if you specify too short of a lease and
        lose the lock before completing your operation. Choosing a good ttl
        value is a trade off that you will have to consider. See also the
        extend() method which will allow you to (possibly periodically) extend
        the lease on the lock.

        If the key argument is provided, it contains the unique string that will
        be used to assert ownership of the lock. If the key value contains
        unicode text, it will also be encoded to utf-8 before being used. A Lock
        instance with this (encoded) value will be needed to unlock the
        resource.  If the key argument is not provided, a random string of ASCII
        letters and digits will be constructed and used as the key.

        Lock will attempt to lock the resource on each of the servers that were
        connected in the initializer up to retry_count times.. If it manages to
        lock the resource on a majority of them then it considers the lock to be
        obtained. If it cannot acquire the lock then it will delay for
        approximately retry_delay seconds before making another attempt.

        If it ultimately obtains the lock then it returns a Lock instance with
        validity set to the approximate lease time in ms, resource set to the
        encoded version of the resource name, and key set to the unique string.

        If the lock cannot be obtained then False is returned.
        """
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
            start_time = time.time()
            del redis_errors[:]
            for server in self.servers:
                try:
                    if self.lock_instance(server, resource, key, ttl):
                        n += 1
                except RedisError as e:
                    redis_errors.append(e)
            elapsed_ms = int((time.time() - start_time) * 1000)
            validity = ttl - elapsed_ms
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
                if retry < self.retry_count:
                    # Sleep for retry_delay +/- 10%
                    time.sleep(random.uniform(self.retry_delay * 0.9,
                                              self.retry_delay * 1.1))

        if redis_errors:
            raise MultipleRedlockException(redis_errors)

        return False

    def unlock(self, lock):
        """
        Given a lock object returned from lock() or extend(), release the lock on
        the resource. If the resource can be unlocked on any of the server
        instance then True is returned. If the lock has presumably expired and
        been acquired by someone else then an OwnerError is raised. Finally, if
        the lock has expired and not been acquired a MissingError is
        raised. Both OwnerError and MissingError derive from LockError so you
        can catch LockError exceptions to catch both of them at once.
        """
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
        """
        Extend the lease on a resource with a new ttl value. If the lease can be
        extended on a quorum of the redis servers then a new Lock will be
        returned with validity set to the new approximate ttl value.

        If the lease cannot be extended, raise an OwnerError or MissingError
        exception like unlock does.

        Note that if ttl <= 0, the lease on resource will expire immediately and
        the resource will be unlocked.
        """
        redis_errors = []
        extended = missint = notowned = 0
        start_time = time.time()
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
            elapsed_ms = int((time.time() - start_time) * 1000)
            validity = ttl - elapsed_ms
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
        """
        Return a Lock instance representing the current lock state of the resource.
        If the resource is locked on a quorum of the servers, a Lock instance is
        returned with validity set to the ttl value at which the resource will
        lose its quorum, the (encoded) resource name, and (encoded) key. If no
        resource has a quorum, returns Lock(0, resource, None).

        Because of the inherently ephemeral nature of locks, query should not be
        used to determine who "currently" has a lock since that may have changed
        by the time this method has returned. If you have a lock and you want to
        ensure that it has not been lost, use extend() which will either extend
        the lease on the lock or raise an exception.
        """
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
        
