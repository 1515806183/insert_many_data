#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:hua
# coding=utf-8
import time
import pymysql
from DBUtils.PooledDB import PooledDB
from multiprocessing.dummy import Pool as ThreadPool


class startJob(object):

    def __init__(self, maxconnections=20, mincached=10, maxcached=5, maxshared=3,
                 host=None, port=3306, user='root', password=None, db=None, pool_nums=15):
        POOL = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=maxconnections,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=mincached,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=maxcached,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=maxshared,
            # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。

            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."] 比如设置数据库的开始时间 set firstday=3

            ping=0,
            # ping MySQL服务端，检查是否服务可用。
            #  如：0 = None = never,
            # 1 = default = whenever it is requested,
            # 2 = when a cursor is created,
            # 4 = when a query is executed,
            # 7 = always
            host=host,
            port=port,
            user=user,
            password=password,
            charset="utf8",
            db=db
        )
        # 建立mysql连接
        self.conn = POOL.connection()
        self.cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.async_pool = ThreadPool(pool_nums)  # 线程池

    # 拼接执行命令
    def run(self):

        # 执行线程
        results = []
        for i in range(20):
            result = self.async_pool.apply_async(self.deal_snmp, args=(i,))
            results.append(result)

        # 执行线程
        for i in results:
            i.wait()  # 等待线程函数执行完毕

        # 关闭数据库
        self.cur.close()
        self.conn.close()

    # 执行命令
    def deal_snmp(self, i):
        try:
            sql = "INSERT INTO test_table (hua) VALUES ('%s')" % i
            # 执行sql, 并保存
            self.cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            pass


start_time = time.time()
job = startJob(host='120.79.254.223', password='123456', db='info', port=3307)
job.run()
print('耗时：', time.time() - start_time)
