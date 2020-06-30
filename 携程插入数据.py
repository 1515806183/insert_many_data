import pymysql
import requests, time
from concurrent.futures import ProcessPoolExecutor


def data_handler(urls):
    conn = pymysql.connect(host='120.79.254.223', user='root', password='xxxx', database='info', charset='utf8', port=3306)
    cursor = conn.cursor()
    for i in range(urls[0], urls[1]):
        sql = 'insert into test_table(hua) values(%s);'
        res = cursor.execute(sql, [i, ])
        conn.commit()
    cursor.close()
    conn.close()


def run():
    urls = [(1, 2000), (2001, 5000), (5001, 8000), (8001, 10000)]
    with ProcessPoolExecutor() as excute:
        ##ProcessPoolExecutor 提供的map函数，可以直接接受可迭代的参数，并且结果可以直接for循环取出
        excute.map(data_handler, urls)


if __name__ == '__main__':
    start_time = time.time()
    run()
    stop_time = time.time()
    print('插入1万条数据耗时 %s' % (stop_time - start_time))
