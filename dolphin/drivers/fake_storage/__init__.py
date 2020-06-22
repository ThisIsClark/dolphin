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

import decorator
import math
import random
import six
import time

from oslo_log import log
from oslo_utils import uuidutils

from dolphin.drivers import driver

LOG = log.getLogger(__name__)


def wait_random(low, high):

    @decorator.decorator
    def _wait(f, *a, **k):
        rd = random.randint(0, 100)
        secs = low + (high - low) * rd / 100
        time.sleep(secs)
        return f(*a, **k)

    return _wait


class FakeStorageDriver(driver.StorageDriver):
    """FakeStorageDriver shows how to implement the StorageDriver,
    it also plays a role as faker to fake data for being tested by clients.
    """

    # System login simulation.
    @wait_random(1, 1.5)
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @wait_random(0.04, 0.12)
    def get_storage(self, context):
        # Do something here
        sn = six.text_type(uuidutils.generate_uuid())
        total, used, free = self._get_random_capacity()
        return {
            'name': 'fake_driver',
            'description': 'fake driver.',
            'vendor': 'fake_vendor',
            'model': 'fake_model',
            'status': 'normal',
            'serial_number': sn,
            'firmware_version': '1.0.0',
            'location': 'HK',
            'total_capacity': total,
            'used_capacity': used,
            'free_capacity': free,
        }

    @wait_random(0.1, 0.15)
    def list_storage_pools(self, ctx):
        rd_pools_count = random.randint(1, 100)
        LOG.info("###########fake_pools number for %s: %d" % (self.storage_id,
                                                              rd_pools_count))
        pool_list = []
        for idx in range(rd_pools_count):
            total, used, free = self._get_random_capacity()
            p = {
                "name": "fake_pool_" + str(idx),
                "storage_id": self.storage_id,
                "original_id": "fake_original_id_" + str(idx),
                "description": "Fake Pool",
                "status": "normal",
                "storage_type": "block",
                "total_capacity": total,
                "used_capacity": used,
                "free_capacity": free,
            }
            pool_list.append(p)
        return pool_list

    def list_volumes(self, ctx):
        # Batch list, limit means the most volumes can get at once.
        limit = 200
        # Get a random number as the volume count.
        rd_volumes_count = random.randint(1, 2000)
        LOG.info("###########fake_volumes number for %s: %d" % (
            self.storage_id, rd_volumes_count))
        loops = math.ceil(rd_volumes_count / limit)
        volume_list = []
        for idx in range(loops):
            start = idx * limit
            end = (idx + 1) * limit
            if idx == (loops - 1):
                end = rd_volumes_count
            vs = self._get_volume_range(start, end)
            volume_list = volume_list + vs
        return volume_list

    def add_trap_config(self, context, trap_config):
        pass

    def remove_trap_config(self, context, trap_config):
        pass

    def parse_alert(self, context, alert):
        pass

    def clear_alert(self, context, alert):
        pass

    @wait_random(0.5, 2.0)
    def _get_volume_range(self, start, end):
        volume_list = []

        for i in range(start, end):
            wwn = six.text_type(uuidutils.generate_uuid())
            total, used, free = self._get_random_capacity()
            v = {
                "name": "fake_vol_" + str(i),
                "storage_id": self.storage_id,
                "description": "Fake Volume",
                "status": "normal",
                "original_id": "fake_original_id_" + str(i),
                "wwn": wwn,
                "total_capacity": total,
                "used_capacity": used,
                "free_capacity": free,
            }
            volume_list.append(v)
        return volume_list

    def _get_random_capacity(self):
        total = random.randint(1000, 2000)
        used = int(random.randint(0, 100) * total / 100)
        free = total - used
        return total, used, free
