# Copyright 2020 The SODA Authors.
# Copyright 2016 Red Hat, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from enum import IntEnum

# The maximum value a signed INT type may have
DB_MAX_INT = 0x7FFFFFFF


# Custom fields for Dolphin objects
class StorageStatus(object):
    NORMAL = 'normal'
    OFFLINE = 'offline'
    ABNORMAL = 'abnormal'

    ALL = (NORMAL, OFFLINE, ABNORMAL)


class StoragePoolStatus(object):
    NORMAL = 'normal'
    OFFLINE = 'offline'
    ABNORMAL = 'abnormal'

    ALL = (NORMAL, OFFLINE, ABNORMAL)


class VolumeStatus(object):
    AVAILABLE = 'available'
    ERROR = 'error'

    ALL = (AVAILABLE, ERROR)


class StorageType(object):
    BLOCK = 'block'
    FILE = 'file'

    ALL = (BLOCK, FILE)


class ResourceType(IntEnum):
    STORAGE_DEVICE = 0
    STORAGE_POOL = 1
    STORAGE_VOLUME = 2


class SyncStatus(IntEnum):
    SYNCED = 0
    SYNCING = 1


class DB(object):
    DEVICE_SYNC_STATUS = 'sync_status'


class ProvisioningPolicy(object):
    THICK = 'thick'
    THIN = 'thin'

    ALL = (THICK, THIN)


# Constants for redis client
SOCKET_CONNECT_TIMEOUT = 10
SOCKET_TIMEOUT = 20
REDIS_TIMEOUT = 100
REDIS_TASK_TIMEOUT = 100
REDIS_SLEEP_TIME = 0.001
REDIS_DEFAULT_IP = '127.0.0.1'
REDIS_DEFAULT_PORT = 6379
TRAP_RECEIVER_CLASS = 'dolphin.alert_manager.trap_receiver.TrapReceiver'
