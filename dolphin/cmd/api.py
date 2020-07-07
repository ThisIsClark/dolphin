#!/usr/bin/env python

# Copyright 2020 The SODA Authors.
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Starter script for dolphin OS API."""

import eventlet
eventlet.monkey_patch()

import sys

from oslo_config import cfg
from oslo_log import log

from dolphin.common import config  # noqa
from dolphin import service
from dolphin import utils
from dolphin import version
from dolphin import db
from dolphin import redis_utils
from dolphin import context
CONF = cfg.CONF


def init_redis():
    client = redis_utils.RedisClient().redis_client
    ctxt = context.RequestContext()
    storages = db.storage_get_all(ctxt)
    for storage in storages:
        id = storage.id
        # 从数据库读取pool数据
        db_pools = db.storage_pool_get_all(ctxt,
                                           filters={"storage_id": id})
        # 将pool的original_id写入redis的storage_pool_{storage_id}集合中
        pool_set_name = "storage_pool_" + id
        client.delete(pool_set_name)
        for pool in db_pools:
            client.sadd(pool_set_name, pool['original_id'])
        # 从数据库读取volume数据
        db_volumes = db.volume_get_all(ctxt,
                                       filters={"storage_id": id})
        # 将volume的original_id写入redis的volume_{storage_id}集合中
        volume_set_name = "storage_volume_" + id
        client.delete(volume_set_name)
        for volume in db_volumes:
            client.sadd(volume_set_name, volume['original_id'])


def main():
    log.register_options(CONF)
    CONF(sys.argv[1:], project='dolphin',
         version=version.version_string())
    log.setup(CONF, "dolphin")
    utils.monkey_patch()

    launcher = service.process_launcher()
    api_server = service.WSGIService('dolphin')
    task_server = service.Service.create(binary='dolphin-task',
                                         coordination=True)
    launcher.launch_service(api_server, workers=api_server.workers or 1)
    launcher.launch_service(task_server)
    db.register_db()
    # Launch alert manager service
    alert_manager = service.AlertMngrService()
    launcher.launch_service(alert_manager)
    init_redis()

    launcher.wait()


if __name__ == '__main__':
    main()
