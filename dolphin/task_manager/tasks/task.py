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

import inspect
import threading
import psutil
import decorator
from oslo_log import log
import datetime

from dolphin import coordination
from dolphin import db
from dolphin import exception
from dolphin import utils
from dolphin.common import constants
from dolphin.drivers import api as driverapi
from dolphin.i18n import _
from dolphin import redis_utils

LOG = log.getLogger(__name__)


def set_synced_after(resource_type):
    @decorator.decorator
    def _set_synced_after(func, *args, **kwargs):
        call_args = inspect.getcallargs(func, *args, **kwargs)
        self = call_args['self']
        ret = func(*args, **kwargs)
        lock = coordination.Lock(self.storage_id)
        with lock:
            try:
                storage = db.storage_get(self.context, self.storage_id)
            except exception.StorageNotFound:
                LOG.warn('Storage %s not found when set synced'
                         % self.storage_id)
            else:
                storage[constants.DB.DEVICE_SYNC_STATUS] = utils.set_bit(
                    storage[constants.DB.DEVICE_SYNC_STATUS],
                    resource_type,
                    constants.SyncStatus.SYNCED)
                db.storage_update(self.context, self.storage_id, storage)
        return ret

    return _set_synced_after


def check_deleted():
    @decorator.decorator
    def _check_deleted(func, *args, **kwargs):
        call_args = inspect.getcallargs(func, *args, **kwargs)
        self = call_args['self']
        ret = func(*args, **kwargs)
        # When context.read_deleted = 'yes', db.storage_get would
        # only get the storage whose 'deleted' tag is not default value
        self.context.read_deleted = 'yes'
        try:
            db.storage_get(self.context, self.storage_id)
        except exception.StorageNotFound:
            LOG.debug('Storage %s not found when checking deleted'
                      % self.storage_id)
        else:
            self.remove()
        return ret

    return _check_deleted


class StorageResourceTask(object):

    def __init__(self, context, storage_id):
        self.storage_id = storage_id
        self.context = context
        self.driver_api = driverapi.API()

    def _classify_resources(self, storage_resources, db_resources):
        """
        :param storage_resources:
        :param db_resources:
        :return: it will return three list add_list: the items present in
        storage but not in current_db. update_list:the items present in
        storage and in current_db. delete_id_list:the items present not in
        storage but present in current_db.
        """
        original_ids_in_db = [resource['original_id']
                              for resource in db_resources]
        delete_id_list = [resource['id'] for resource in db_resources]
        add_list = []
        update_list = []

        for resource in storage_resources:
            if resource['original_id'] in original_ids_in_db:
                resource['id'] = db_resources[original_ids_in_db.index(
                    resource['original_id'])]['id']
                delete_id_list.remove(resource['id'])
                update_list.append(resource)
            else:
                add_list.append(resource)

        return add_list, update_list, delete_id_list


class StorageDeviceTask(StorageResourceTask):
    def __init__(self, context, storage_id):
        super(StorageDeviceTask, self).__init__(context, storage_id)

    @check_deleted()
    @set_synced_after(constants.ResourceType.STORAGE_DEVICE)
    def sync(self):
        """
        :return:
        """
        LOG.info('Syncing storage device for storage id:{0}'.format(
            self.storage_id))
        try:
            storage = self.driver_api.get_storage(self.context,
                                                  self.storage_id)

            db.storage_update(self.context, self.storage_id, storage)
        except AttributeError as e:
            LOG.error(e)
        except Exception as e:
            msg = _('Failed to update storage entry in DB: {0}'
                    .format(e))
            LOG.error(msg)
        else:
            LOG.info("Syncing storage successful!!!")

    def remove(self):
        LOG.info('Remove storage device for storage id:{0}'
                 .format(self.storage_id))
        try:
            db.storage_delete(self.context, self.storage_id)
            db.access_info_delete(self.context, self.storage_id)
            db.alert_source_delete(self.context, self.storage_id)
        except Exception as e:
            LOG.error('Failed to update storage entry in DB: {0}'.format(e))


class StoragePoolTask(StorageResourceTask):
    def __init__(self, context, storage_id):
        super(StoragePoolTask, self).__init__(context, storage_id)
        self.client = redis_utils.RedisClient().redis_client

    @check_deleted()
    @set_synced_after(constants.ResourceType.STORAGE_POOL)
    def sync(self):
        """
        :return:
        """
        LOG.info('Syncing storage pool for storage id:{0}'.format(
            self.storage_id))
        """
        try:
            # collect the storage pools list from driver and database
            storage_pools = self.driver_api.list_storage_pools(self.context,
                                                               self.storage_id)
            old_set_name = 'storage_pool_' + self.storage_id
            next, cur_list_ids = self.client.sscan(old_set_name, 0)
            while next != 0:
                next, tmp = self.client.sscan(old_set_name, next)
                cur_list_ids.extend(tmp)
            cur_list_ids_str = {x.decode('utf-8') for x in cur_list_ids}
            new_list_ids_str = {pool['original_id'] for pool in storage_pools}
            update_list_ids_str = cur_list_ids_str.intersection(new_list_ids_str)
            add_list_ids_str = new_list_ids_str.difference(update_list_ids_str)
            delete_list_ids_str = cur_list_ids_str.difference(update_list_ids_str)
            add_list = []
            update_list = []

            for pool in storage_pools:
                if pool['original_id'] in add_list_ids_str:
                    add_list.append(pool)
                elif pool['original_id'] in update_list_ids_str:
                    update_list.append(pool)

            LOG.info('###StoragePoolTask for {0}:add={1},delete={2},'
                     'update={3}'.format(self.storage_id,
                                         len(add_list),
                                         len(delete_list_ids_str),
                                         len(update_list)))
            if delete_list_ids_str:
                db.storage_pools_delete(self.context, self.storage_id, delete_list_ids_str)

            if update_list:
                db.storage_pools_update(self.context, update_list)

            if add_list:
                db.storage_pools_create(self.context, add_list)

            for id in add_list_ids_str:
                self.client.sadd(old_set_name, id)
            # 删除的
            for id in delete_list_ids_str:
                self.client.srem(old_set_name, id)

        except AttributeError as e:
            LOG.error(e)
        except Exception as e:
            msg = _('Failed to sync pools entry in DB: {0}'
                    .format(e))
            LOG.error(msg)
        else:
            LOG.info("Syncing storage pools successful!!!")
        """

    def remove(self):
        LOG.info('Remove storage pools for storage id:{0}'.format(
            self.storage_id))
        db.storage_pool_delete_by_storage(self.context, self.storage_id)


class StorageVolumeTask(StorageResourceTask):
    def __init__(self, context, storage_id):
        super(StorageVolumeTask, self).__init__(context, storage_id)
        self.client = redis_utils.RedisClient().redis_client

    @check_deleted()
    @set_synced_after(constants.ResourceType.STORAGE_VOLUME)
    def sync(self):
        """
        :return:
        """
        LOG.info('Syncing volumes for storage id:{0}'.format(self.storage_id))
        try:
            # collect the volumes list from driver and database
            storage_volumes = self.driver_api.list_volumes(self.context,
                                                           self.storage_id)
            # 把当前设备查上来的数据的original_id放入set storage_volume_{storage_id}_new中
            """方案1
            add_list = []
            update_list = []
            part1 = datetime.datetime.now()
            old_set_name = 'storage_volume_' + self.storage_id
            cur_list_ids = self.client.smembers(old_set_name)
            part2 = datetime.datetime.now()
            interval = (part2 - part1).seconds
            if interval > 0:
                LOG.info('ly>>>>>>interval 1 is %s' % interval)
            cur_list_ids_str = {x.decode('utf-8') for x in cur_list_ids}
            new_list_ids_str = {volume['original_id'] for volume in storage_volumes}
            update_list_ids_str = cur_list_ids_str.intersection(new_list_ids_str)
            add_list_ids_str = new_list_ids_str.difference(update_list_ids_str)
            delete_list_ids_str = cur_list_ids_str.difference(update_list_ids_str)

            for volume in storage_volumes:
                if volume['original_id'] in add_list_ids_str:
                    add_list.append(volume)
                elif volume['original_id'] in update_list_ids_str:
                    update_list.append(volume)
            """
            """方案2
            add_list = []
            update_list = []
            set_name = 'storage_volume_' + self.storage_id
            part1 = datetime.datetime.now()
            for storage_volume in storage_volumes:
                if self.client.sismember(set_name, storage_volume['original_id']):
                    update_list.append(storage_volume)
                    self.client.srem(set_name, storage_volume['original_id'])
                else:
                    add_list.append(storage_volume)
            part2 = datetime.datetime.now()
            interval = (part2 - part1).seconds
            if interval > 0:
                LOG.info('ly>>>>>>interval1 is %s' % interval)
            delete_list_ids = self.client.smembers(set_name)
            delete_list_ids_str = {x.decode('utf-8') for x in delete_list_ids}
            part3 = datetime.datetime.now()
            interval = (part3 - part2).seconds
            if interval > 0:
                LOG.info('ly>>>>>>interval2 is %s' % interval)
            """
            old_set_name = 'storage_volume_' + self.storage_id
            new_set_name = 'storage_volume_' + self.storage_id + "_new"
            part1 = datetime.datetime.now()
            for storage_volume in storage_volumes:
                self.client.sadd(new_set_name, storage_volume['original_id'])
            add_ids = self.client.sdiff(new_set_name, old_set_name)
            add_ids_str = [x.decode('utf-8') for x in add_ids]
            del_ids = self.client.sdiff(old_set_name, new_set_name)
            del_ids_str = [x.decode('utf-8') for x in del_ids]
            upd_ids = self.client.sinter(new_set_name, old_set_name)
            upd_ids_str = [x.decode('utf-8') for x in upd_ids]
            part2 = datetime.datetime.now()
            interval = (part2 - part1).seconds
            if interval > 0:
                LOG.info('ly>>>>>>interval1 is %s' % interval)
            add_list = []
            update_list = []
            for volume in storage_volumes:
                if volume['original_id'] in add_ids_str:
                    add_list.append(volume)
                elif volume['original_id'] in upd_ids_str:
                    update_list.append(volume)
            LOG.info('###StorageVolumeTask for {0}:add={1},delete={2},'
                     'update={3}'.format(self.storage_id,
                                         len(add_list),
                                         len(del_ids_str),
                                         len(update_list)))
            #LOG.info('ly>>>>>[task]memory is %s' % psutil.Process().memory_info().peak_wset)
            if del_ids_str:
                db.volumes_delete(self.context, self.storage_id, del_ids_str)

            if update_list:
                db.volumes_update(self.context, update_list)

            if add_list:
                db.volumes_create(self.context, add_list)

            # 更新redis
            # 新增的
            for id in add_ids_str:
                self.client.sadd(old_set_name, id)
            # 删除的
            for id in del_ids_str:
                self.client.srem(old_set_name, id)
            self.client.delete(new_set_name)

        except AttributeError as e:
            LOG.error(e)
        except Exception as e:
            msg = _('Failed to sync volumes entry in DB: {0}'
                    .format(e))
            LOG.error(msg)
        else:
            LOG.info("Syncing volumes successful!!!")

    def remove(self):
        LOG.info('Remove volumes for storage id:{0}'.format(self.storage_id))
        db.volume_delete_by_storage(self.context, self.storage_id)
