# -*- coding:utf-8 -*-
import time
from pymysql import *


# 装饰器，计算插入10000条数据需要的时间
def timer(func):
    def decor(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        d_time = end_time - start_time
        print("这次插入10000条数据耗时 : ", d_time)

    return decor


@timer
def add_test_users():
    # 将待插入的数据先进行遍历，处理放到一个元祖的列表里面
    usersvalues = []
    for num in range(1, 10000):
        usersvalues.append((str(num),))

    conn = connect(host='120.79.254.223', user='root', password='xxxx', database='info', charset='utf8', port=3307)
    cs = conn.cursor()  # 获取光标
    cs.executemany('insert into test_table (hua) values(%s)', usersvalues)
    conn.commit()
    cs.close()
    conn.close()
    print('OK')


if __name__ == '__main__':
    add_test_users()
