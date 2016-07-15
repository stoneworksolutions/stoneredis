#!/usr/bin/env python
# encoding: utf-8


import inspect
import os
import redis
import redis.sentinel
import redis_lock
import time
import traceback
from redis.exceptions import ConnectionError, RedisError, TimeoutError


class StoneRedis(redis.client.Redis):

    def __init__(self, *args, **kwargs):
        ''' Original method. Called through args kwargs to keep compatibility with future versions
        of redis-py. If we need to pass non exisiting arguments they would have to be treated here:
        self.myparam = kwargs.pop(myparam)
        If new arguments are added to this class they must also be added to pipeline method and be treated in StonePipeline class.
        It will also be necessary to add to StoneSentinel class.
        '''
        # Save them with re connection purposes
        self.args = args
        self.kwargs = kwargs

        # conn_retries is the number of times that reconnect will try to connect
        self.conn_retries = kwargs.pop('conn_retries', 1)

        # conn_retries is the number of times that any command will try to execute
        self.retries = kwargs.pop('retries', 3)

        # max_sleep is the amount of time between reconnection attmpts by safe_reconnect
        self.max_sleep = kwargs.pop('max_sleep', 30)

        self.logger = kwargs.pop('logger', None)

        super(redis.client.Redis, self).__init__(*args, **kwargs)

    def ping(self):
        try:
            super(StoneRedis, self).ping()
        except:
            return False
        return True

    def connect(self, conn_retries=None):
        ''' Connects to Redis with a exponential waiting (3**n) '''
        return self.reconnect(conn_retries=conn_retries)

    def reconnect(self, conn_retries=None):
        ''' Connects to Redis with a exponential waiting (3**n) '''
        if conn_retries is None:
            conn_retries = self.conn_retries

        count = 0
        if self.logger:
            self.logger.info('Connecting to Redis..')
        while count < conn_retries:
            super(redis.client.Redis, self).__init__(*self.args, **self.kwargs)

            if self.ping():
                if self.logger:
                    self.logger.info('Connected to Redis!')
                return True
            else:
                sl = min(3 ** count, self.max_sleep)
                if self.logger:
                    self.logger.info('Connecting failed, retrying in {0} seconds'.format(sl))
                time.sleep(sl)
                count += 1
        raise ConnectionError

    def safe_reconnect(self):
        ''' Connects to Redis with a exponential waiting (3**n), wont return until successfully connected'''
        count = 0
        if self.logger:
            self.logger.info('Connecting to Redis..')
        while True:
            super(redis.client.Redis, self).__init__(*self.args, **self.kwargs)

            if self.ping():
                if self.logger:
                    self.logger.info('Connected to Redis!')
                return True
            else:
                sl = min(3 ** count, self.max_sleep)
                if self.logger:
                    self.logger.info('Connecting failed, retrying in {0} seconds'.format(sl))
                time.sleep(sl)
                count += 1

    def _multi_lpop_pipeline(self, pipe, queue, number):
        ''' Pops multiple elements from a list in a given pipeline'''
        pipe.lrange(queue, 0, number - 1)
        pipe.ltrim(queue, number, -1)

    def multi_lpop(self, queue, number, transaction=True):
        ''' Pops multiple elements from a list
            This operation will be atomic if transaction=True is passed
        '''
        try:
            pipe = self.pipeline(transaction=transaction)
            pipe.multi()
            self._multi_lpop_pipeline(pipe, queue, number)
            return pipe.execute()[0]
        except IndexError:
            return []
        except:
            raise

    def _multi_rpush_pipeline(self, pipe, queue, values, bulk_size=0):
        ''' Pushes multiple elements to a list in a given pipeline
            If bulk_size is set it will execute the pipeline every bulk_size elements
        '''
        cont = 0
        for value in values:
            pipe.rpush(queue, value)
            if bulk_size != 0 and cont % bulk_size == 0:
                pipe.execute()

    def multi_rpush(self, queue, values, bulk_size=0, transaction=True):
        ''' Pushes multiple elements to a list
            If bulk_size is set it will execute the pipeline every bulk_size elements
            This operation will be atomic if transaction=True is passed
        '''
        # Check that what we receive is iterable
        if hasattr(values, '__iter__'):
            pipe = self.pipeline(transaction=transaction)
            pipe.multi()
            self._multi_rpush_pipeline(pipe, queue, values, bulk_size)
            pipe.execute()
        else:
            raise ValueError('Expected an iterable')

    def multi_rpush_limit(self, queue, values, limit=100000):
        ''' Pushes multiple elements to a list in an atomic way until it reaches certain size
            Once limit is reached, the function will lpop the oldest elements
            This operation runs in LUA, so is always atomic
        '''

        lua = '''
        local queue = KEYS[1]
        local max_size = tonumber(KEYS[2])
        local table_len = tonumber(table.getn(ARGV))
        local redis_queue_len = tonumber(redis.call('LLEN', queue))
        local total_size = redis_queue_len + table_len
        local from = 0

        if total_size >= max_size then
            -- Delete the same amount of data we are inserting. Even better, limit the queue to the specified size
            redis.call('PUBLISH', 'DEBUG', 'trim')
            if redis_queue_len - max_size + table_len > 0 then
                from = redis_queue_len - max_size + table_len
            else
                from = 0
            end
            redis.call('LTRIM', queue, from, redis_queue_len)
        end
        for _,key in ipairs(ARGV) do
            redis.call('RPUSH', queue, key)
        end
        return 1

        '''

        # Check that what we receive is iterable
        if hasattr(values, '__iter__'):
            if len(values) > limit:
                raise ValueError('The iterable size is bigger than the allowed limit ({1}): {0}'.format(len(values), limit))
            try:
                self.multi_rpush_limit_script([queue, limit], values)
            except AttributeError:
                if self.logger:
                    self.logger.info('Script not registered... registering')
                # If the script is not registered, register it
                self.multi_rpush_limit_script = self.register_script(lua)
                self.multi_rpush_limit_script([queue, limit], values)
        else:
            raise ValueError('Expected an iterable')

    def rpush_limit(self, queue, value, limit=100000):
        ''' Pushes an element to a list in an atomic way until it reaches certain size
            Once limit is reached, the function will lpop the oldest elements
            This operation runs in LUA, so is always atomic
        '''

        lua = '''
        local queue = KEYS[1]
        local max_size = tonumber(KEYS[2])
        local table_len = 1
        local redis_queue_len = tonumber(redis.call('LLEN', queue))
        local total_size = redis_queue_len + table_len
        local from = 0

        if total_size >= max_size then
            -- Delete the same amount of data we are inserting. Even better, limit the queue to the specified size
            redis.call('PUBLISH', 'DEBUG', 'trim')
            if redis_queue_len - max_size + table_len > 0 then
                from = redis_queue_len - max_size + table_len
            else
                from = 0
            end
            redis.call('LTRIM', queue, from, redis_queue_len)
        end
        redis.call('RPUSH', queue, ARGV[1])
        return 1

        '''

        try:
            self.rpush_limit_script([queue, limit], [value])
        except AttributeError:
            if self.logger:
                self.logger.info('Script not registered... registering')
            # If the script is not registered, register it
            self.rpush_limit_script = self.register_script(lua)
            self.rpush_limit_script([queue, limit], [value])

    def get_lock(self, lockname, locktime=60, auto_renewal=False):
        ''' Gets a lock and returns if it can be stablished. Returns false otherwise '''
        pid = os.getpid()
        caller = inspect.stack()[0][3]
        try:
            # rl = redlock.Redlock([{"host": settings.REDIS_SERVERS['std_redis']['host'], "port": settings.REDIS_SERVERS['std_redis']['port'], "db": settings.REDIS_SERVERS['std_redis']['db']}, ])
            rl = redis_lock.Lock(self, lockname, expire=locktime, auto_renewal=auto_renewal)
        except:
            if self.logger:
                self.logger.error('Process {0} ({1}) could not get lock {2}. Going ahead without locking!!! {3}'.format(pid, caller, lockname, traceback.format_exc()))
            return False
        try:
            lock = rl.acquire(blocking=False)
        except RedisError:
            return False
        if not lock:
            return False
        else:
            return rl

    def wait_for_lock(self, lockname, locktime=60, auto_renewal=False):
        ''' Gets a lock or waits until it is able to get it '''
        pid = os.getpid()
        caller = inspect.stack()[0][3]
        try:
            # rl = redlock.Redlock([{"host": settings.REDIS_SERVERS['std_redis']['host'], "port": settings.REDIS_SERVERS['std_redis']['port'], "db": settings.REDIS_SERVERS['std_redis']['db']}, ])
            rl = redis_lock.Lock(self, lockname, expire=locktime, auto_renewal=auto_renewal)
        except AssertionError:
            if self.logger:
                self.logger.error('Process {0} ({1}) could not get lock {2}. Going ahead without locking!!! {3}'.format(pid, caller, lockname, traceback.format_exc()))
            return False
        cont = 1
        t0 = time.time()
        lock = None
        while not lock:
            time.sleep(.05)
            cont += 1
            if cont % 20 == 0:
                if self.logger:
                    self.logger.debug('Process {0} ({1}) waiting for lock {2}. {3} seconds elapsed.'.format(pid, caller, lockname, time.time() - t0))
            # lock = rl.lock(lockname, locktime_ms)
            try:
                lock = rl.acquire()
            except RedisError:
                pass
        if self.logger:
            self.logger.debug('Process {0} ({1}) got lock {2} for {3} seconds'.format(pid, caller, lockname, locktime))
        return rl

    def release_lock(self, lock, force=False):
        ''' Frees a lock '''
        pid = os.getpid()
        caller = inspect.stack()[0][3]
        # try:
        #   rl = redlock.Redlock([{"host": settings.REDIS_SERVERS['std_redis']['host'], "port": settings.REDIS_SERVERS['std_redis']['port'], "db": settings.REDIS_SERVERS['std_redis']['db']}, ])
        # except:
        #   logger.error('Process {0} ({1}) could not release lock {2}'.format(pid, caller, lock.resource))
        #   return False
        if lock and lock._held:
            lock.release()
        if self.logger:
            self.logger.debug('Process {0} ({1}) released lock'.format(pid, caller))

    def pipeline(self, transaction=True, shard_hint=None):
        ''' Return a pipeline that support StoneRedis custom methods '''
        args_dict = {
            'connection_pool': self.connection_pool,
            'response_callbacks': self.response_callbacks,
            'transaction': transaction,
            'shard_hint': shard_hint,
            'logger': self.logger,
            'retries': self.retries,
            'max_sleep': self.max_sleep,
        }

        return StonePipeline(**args_dict)

    def setx(self, key, value):
        ''' Sets an value in only if his key exists
            This operation runs in LUA, so is always atomic
        '''

        lua = '''
        local key = ARGV[1]
        local value = ARGV[2]

        if redis.call('EXISTS', key) > 0 then
            redis.call('SET', key, value)
            return 1
        end

        return 0

        '''

        try:
            return bool(self.setx_script(['key', 'value'], [key, value]))
        except AttributeError:
            if self.logger:
                self.logger.info('Script not registered... registering')
            # If the script is not registered, register it
            self.setx_script = self.register_script(lua)
            return bool(self.setx_script(['key', 'value'], [key, value]))

    def hsetx(self, key, field, value):
        ''' Sets a value in a field in only if his key exists
            This operation runs in LUA, so is always atomic
        '''

        lua = '''
        local key = ARGV[1]
        local field = ARGV[2]
        local value = ARGV[3]

        if redis.call('EXISTS', key) > 0 then
            redis.call('HSET', key, field, value)
            return 1
        end

        return 0

        '''

        try:
            return bool(self.hsetx_script(['key', 'field', 'value'], [key, field, value]))
        except AttributeError:
            if self.logger:
                self.logger.info('Script not registered... registering')
            # If the script is not registered, register it
            self.hsetx_script = self.register_script(lua)
            return bool(self.hsetx_script(['key', 'field', 'value'], [key, field, value]))

    def execute_command(self, *args, **options):
        ''' Wrapper of Redis.execute_command to warrants n retries of the call on failure '''
        try:
            return super(redis.client.Redis, self).execute_command(*args, **options)
        except (ConnectionError, TimeoutError):
            count = 0
            while count < self.retries:
                try:
                    return super(redis.client.Redis, self).execute_command(*args, **options)
                except (ConnectionError, TimeoutError):
                    sl = min(3 ** count, self.max_sleep)
                    if self.logger:
                        self.logger.info('Connecting failed, retrying in {0} seconds'.format(sl))
                    time.sleep(sl)
                    count += 1
            raise


class StonePipeline(redis.client.Pipeline, StoneRedis):
    ''' Pipeline for the StoneRedis class.
        If we need to pass non exisiting arguments they would have to be removed:
        kwargs.pop(myparam)
    '''

    def __init__(self, *args, **kwargs):
        # conn_retries is the number of times that reconnect will try to connect
        self.logger = kwargs.pop('logger', None)

        # conn_retries is the number of times that any command will try to execute
        self.retries = kwargs.pop('retries', 3)

        # max_sleep is the amount of time between reconnection attmpts by safe_reconnect
        self.max_sleep = kwargs.pop('max_sleep', 30)

        super(StonePipeline, self).__init__(*args, **kwargs)

    def multi_lpop(self, queue, number, transaction=False):
        ''' Pops multiple elements from a list '''
        try:
            self._multi_lpop_pipeline(self, queue, number)
        except:
            raise

    def multi_rpush(self, queue, values, bulk_size=0, transaction=False):
        # TODO: Separar transaccional de no-transaccional
        ''' Pushes multiple elements to a list '''
        # Check that what we receive is iterable
        if hasattr(values, '__iter__'):
            self._multi_rpush_pipeline(self, queue, values, 0)
        else:
            raise ValueError('Expected an iterable')

    def execute(self, raise_on_error=True):
        ''' Wrapper of Pipeline.execute to warrants n retries of the call on failure '''
        stack = self.command_stack
        try:
            return super(StonePipeline, self).execute(raise_on_error)
        except (ConnectionError, TimeoutError):
            # Transactional will retry the call on failure
            if self.transaction:
                count = 0
                while count < self.retries:
                    try:
                        self.command_stack = stack
                        return super(StonePipeline, self).execute(raise_on_error)
                    except (ConnectionError, TimeoutError):
                        sl = min(3 ** count, self.max_sleep)
                        if self.logger:
                            self.logger.info('Connecting failed, retrying in {0} seconds'.format(sl))
                        time.sleep(sl)
                        count += 1
            # Non transactional case has same behaviour.
            else:
                raise  # It's not necessary but this is more specific.
            raise


class StoneSentinel(redis.sentinel.Sentinel):
    ''' Wrapper of original method to allow a good instantiation of StoneRedis class.
    '''
    def master_for(self, service_name, redis_class=StoneRedis, connection_pool_class=redis.sentinel.SentinelConnectionPool, **kwargs):
        if redis_class is StoneRedis:
            stoneredis_kwargs = {}

            # conn_retries is the number of times that reconnect will try to connect
            stoneredis_kwargs['conn_retries'] = kwargs.pop('conn_retries', 1)

            # conn_retries is the number of times that any command will try to execute
            stoneredis_kwargs['retries'] = kwargs.pop('retries', 3)

            # max_sleep is the amount of time between reconnection attmpts by safe_reconnect
            stoneredis_kwargs['max_sleep'] = kwargs.pop('max_sleep', 30)

            stoneredis_kwargs['logger'] = kwargs.pop('logger', None)

        redis_class_instance = super(StoneSentinel, self).master_for(service_name, redis_class=redis_class, connection_pool_class=connection_pool_class, **kwargs)

        if redis_class is StoneRedis:
            for k, v in stoneredis_kwargs.iteritems():
                redis_class_instance.__setattr__(k, v)
                redis_class_instance.__setitem__(k, v)

        return redis_class_instance
