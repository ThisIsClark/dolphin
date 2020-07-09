# Copyright 2020 The SODA Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import uuid

import redis
from oslo_config import cfg
from oslo_log import log

from delfin import cryptor
from delfin import utils

LOG = log.getLogger(__name__)
CONF = cfg.CONF


class RedisClient(metaclass=utils.Singleton):
    def __init__(self):
        password = CONF.redis.password or ''
        password = cryptor.decode(password)
        self.pool = redis.ConnectionPool(host=CONF.redis.redis_ip,
                                         port=CONF.redis.redis_port,
                                         password=password,
                                         socket_connect_timeout=60,
                                         socket_timeout=60)

    def get_redis(self):
        """Get redis client

        :return: The redis client
        """
        try:
            redis_client = redis.StrictRedis(connection_pool=self.pool)
            return redis_client
        except Exception as err:
            if isinstance(err, TimeoutError) \
                    or isinstance(err, ConnectionError):
                LOG.error("connect to redis failed")
                raise err