#!/usr/bin/env python

import os, sys, time, copy, re
import threading, argparse
import asyncio

from pyraft.common import RaftException
from pyraft import raft
from kazoo.client import KazooClient

class ArcusOrbitor(raft.RaftNode):
    def __init__(self, nid, addr, ensemble, zk_addr, overwrite_peer):
        super().__init__(nid, addr, ensemble, overwrite_peer=overwrite_peer)

        self.zk_addr = zk_addr
        self.check_list_map = {}

        self.failover_count = 0

        self.cooldown_time = 0
        self.cooldown_start = 0

    def health_checker_thread(self):
        self.log_info('health check thread start')

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.check_all())

        self.log_info('health check thread is terminated')

    def watch_children(self, event):
        self.reload_check_list()

    def reload_check_list(self):
        cloud_list = []
        service_codes = self.zk.get_children('/arcus/cache_list/')
        for service_code in service_codes:
            if self.re_service_code.match(service_code):
                cloud_list.append(service_code)

        tmp_check_list_map = {}
        for cloud in cloud_list:
            children = self.zk.get_children('/arcus/cache_list/%s' % cloud, watch=self.watch_children)
            self.log_info('reload %s cache_list: %s' % (cloud, str(children)))
            for child in children:
                tmp_check_list_map[child] = cloud

            self.check_list_map = tmp_check_list_map

    def on_start(self):
        self.zk = KazooClient(hosts=self.zk_addr)
        self.zk.start()

        self.hc_flag = 'start'
        self.reload_check_list()

    def on_shutdown(self):
        self.zk.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-zk', dest='zk_addr', required=True, help='zookeeper address')

    args = raft.parse_default_args(parser)

    node = ArcusOrbitor(args.nid, args.addr, args.ensemble_map, args.zk_addr, args.overwrite_peer)

    node.start()
    node.join()
