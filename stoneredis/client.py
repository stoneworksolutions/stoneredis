#!/usr/bin/env python
# encoding: utf-8
"""
redis_utils.py
"""

import time
import redis
import redis.sentinel
from redis.exceptions import ConnectionError


class StoneRedis(redis.client.Redis):

    def __init__(self, *args, **kwargs):
        ''' Original method. Called through args kwargs to keep compatibility with future versions
        of redis-py. If we need to pass non exisiting arguments they would have to be treated here:
        self.myparam = kwargs.pop(myparam)
        '''
        # Save them with re connection purposes
        self.args = args
        self.kwargs = kwargs

        # conn_retries is the number of times that reconnect will try to connect
        if 'conn_retries' in kwargs:
            self.conn_retries = kwargs.pop('conn_retries')
        else:
            self.conn_retries = 1

        # max_sleep is the amount of time between reconnection attmpts by safe_reconnect
        if 'max_sleep' in kwargs:
            self.max_sleep = kwargs.pop('max_sleep')
        else:
            self.max_sleep = 30

        if 'logger' in kwargs:
            self.logger = kwargs.pop('logger')
        else:
            self.logger = None

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
                sl = 3 ** count
                if self.logger:
                    self.logger.info('Connecting failed, retrying in {0} seconds'.format(sl))
                time.sleep(sl)
                count += 1
        raise ConnectionError

    def safe_reconnect(self, conn_retries=None):
        ''' Connects to Redis with a exponential waiting (3**n), wont return until successfully connected'''
        if conn_retries is None:
            conn_retries = self.conn_retries

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

    def multi_lpop(self, queue, number, transaction=False):
        ''' Pops multiple elements from a queue in an atomic way :) should be on the specs! '''
        try:
            pipe = self.pipeline(transaction=transaction)
            pipe.multi()
            pipe.lrange(queue, 0, number - 1)
            pipe.ltrim(queue, number, -1)
            return pipe.execute()[0]
        except IndexError:
            return []
        except:
            raise

    def multi_rpush(self, queue, values, bulk_size=0, transaction=False):
        ''' Pushes multiple elements to a queue in an atomic way :) should be on the specs!
            If bulk_size is set it will execute every each bulk_side inserted elements
            The pipeline will be transactional if transaction is True '''

        # Check that what we receive is iterable
        if hasattr(values, '__iter__'):
            pipe = self.pipeline(transaction=transaction)
            pipe.multi()
            cont = 0
            for value in values:
                pipe.rpush(queue, value)
                if bulk_size != 0 and cont % bulk_size == 0:
                    pipe.execute()
            pipe.execute()
        else:
            raise ValueError('Expected an iterable')

    def multi_rpush_limit(self, queue, values, limit):
        ''' Pushes multiple elements to a queue in an atomic way until it reaches certain size :) should be on the specs!
            Once limit is reached, the function will lpop the oldest elements
            If bulk_size is set it will execute every each bulk_side inserted elements
            The pipeline will be transactional if transaction is True '''

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
            print 'Iterable, lets rock!'
            if len(values) > limit:
                raise ValueError('The iterable size is bigger than the allowed limit ({1}): {0}'.format(len(values), limit))
            try:
                self.multi_rpush_limit_script([queue, limit], values)
            except AttributeError:
                print 'Script not registered... registering'
                # If the script is not registered, register it
                self.multi_rpush_limit_script = self.register_script(lua)
                self.multi_rpush_limit_script([queue, limit], values)
        else:
            raise ValueError('Expected an iterable')
