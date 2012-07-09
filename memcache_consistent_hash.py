#!/usr/bin
# -*- encoding:utf-8 -*-

import zlib
from memcache import Client
from operator import itemgetter, attrgetter
mm_server = [{'host':'192.168.201.109', 'port':11211}, {'host':'192.168.1.96', 'port':11211}]
MMC_CONSISTENT_BUCKETS = 1024
MMC_CONSISTENT_POINTS = 160


class mmc_consistent:

    state = {
            'num_server':0,
            'num_points':0,
            'points':[],
            'buckets_populated':0,
            'buckets':[]
            }

    def mmc_consistent_find(self, point):
        #print point
        lo = 0
        hi = self.state['num_points'] - 1
        mid = 0
        while(1):
            if point <= self.state['points'][lo]['point'] or point > self.state['points'][hi]['point']:
                #print 1;
                return self.state['points'][lo]['server']

            #二分法查找
            mid = lo + (hi - lo) / 2;

            if not mid:
                mid = 0

            if point <= self.state['points'][mid]['point'] and point > self.state['points'][mid-1]['point']:
                return self.state['points'][mid]['server']

            if self.state['points'][mid]['point'] < point:
                lo = mid + 1;
            else:
                hi = mid - 1;


    def mmc_consistent_populate_buckets(self):
        step = 0xffffffff / MMC_CONSISTENT_BUCKETS
        self.state['points'] = sorted(self.state['points'], key=lambda point:point['point'])
        #self.state['points'] = sorted(self.state['points'], key = itemgetter('point'))

        #print self.state['points']
        #return
        for i in range(MMC_CONSISTENT_BUCKETS):
            self.state['buckets'].insert(i, self.mmc_consistent_find(step * i))
        self.state['buckets_populated'] = 1

    def mmc_hash(self,key):
        return zlib.crc32(key) & 0xffffffff

    #添加服务器1台服务器虚拟成多个虚拟节点
    def mmc_consistent_add_server(self, server, weight):
        #print state
        points = weight * MMC_CONSISTENT_POINTS

        for i in range(points):
            key = '%s:%d-%d' % (server['host'], server['port'], i)
            hash_result = self.mmc_hash(key)
            self.state['points'].insert(self.state['num_points']+i,{'server':server, 'point':hash_result})

        self.state['num_points'] += points
        self.state['num_server'] += 1
        self.state['buckets_populated'] = 0

    def mmc_consistent_compare(self, point_a, point_b):
        if point_a['point'] <= point_b['point']:
            return -1
        if point_a['point'] > point_b['point']:
            return 1
        return 0
        #return state

    def mmc_consistent_find_server(self, key):
        #print self.state
        if self.state['num_server'] > 1:
            if not self.state['buckets_populated']:
                self.mmc_consistent_populate_buckets()
            hash_result = self.mmc_hash(key)
            return self.state['buckets'][hash_result % MMC_CONSISTENT_BUCKETS]
        return self.state['points'][0]['server']

if __name__ == '__main__':

    def mm_find_key_host(mm_server, key):
        mmc = mmc_consistent()
        for item in mm_server:
            mmc.mmc_consistent_add_server(server=item, weight=1)
        server = mmc.mmc_consistent_find_server(key)
        return server

    '''mmc = mmc_consistent()
    for item in mm_server:
        mmc.mmc_consistent_add_server(server=item, weight=1)'''
    #print mmc.state['points']
    #根据计算得出对应服务器获取对应key数据 验证
    key = ['test1','test2','test3','test4','test5','test6','test7','test8','test9']
    for item in key:
        server = mm_find_key_host(mm_server, item)
        print server
        mconfig = ['%s:%d' % (server['host'], server['port'])]
        mm = Client(mconfig)
        print mm.get(item)

        #直接使用python_memcached 获取数据验证
        mm_server_list = ['192.168.1.96:11211', '192.168.201.109:11211']
        mm = Client(mm_server_list)
        print mm.get(item)

