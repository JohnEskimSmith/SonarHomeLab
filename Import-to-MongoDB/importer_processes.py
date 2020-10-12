#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"
import argparse
import os
import multiprocessing as mp
from ujson import loads as ujson_loads
from pymongo import MongoClient
import time
from bson.int64 import Int64
from ipaddress import IPv4Address
from typing import (List)


def parse_record(row: str) -> dict:
    """
    reparse str record to dict with keys(fields) for MongoDB
    :param row:
    :return: dict record for insert to MongoDB
    Example return record:
    First schema
    {
    "timestamp" : NumberLong(1601597605),
    "type" : "a",                         - without changes from file
    "value" : NumberLong(416915262),      - Int represent of IPv4
    "tld" : "com",
    "domain" : "spectrum",
    "subdomain" : "024-217-159-062.res"
    }
    Second schema
    {
    "timestamp" : NumberLong(1601652544),
    "type" : "cname",
    "tld" : "be",
    "domain" : "stylingcorner",
    "subdomain" : "mail",
    "value_str" : "stylingcorner.be"     - no IP in record of file and save as is
}
    """

    def convert_timestamp(value: str) -> Int64:
        try:
            return Int64(value)
        except:
            pass

    def convert_ip(value: str) -> Int64:
        try:
            ip = IPv4Address(value)
            return Int64(ip)
        except:
            pass

    def convert_domain(value: str):
        try:
            domain_parts = value.split('.')
            tld = domain_parts[-1]
            domain = domain_parts[-2]
            subdomain = value[:-len(tld) - len(domain) - 1].strip('.')
            return tld, domain, subdomain
        except:
            pass

    record = {}
    try:
        _row: dict = ujson_loads(row)
        keys = {'timestamp': convert_timestamp,
                'name': convert_domain,
                'type': None,
                'value':convert_ip,
                'domain':convert_domain}
        for k in keys:
            if k in _row:
                if keys[k]:
                    record[k] = keys[k](_row[k])
                else:
                    record[k] = _row[k]
        if 'name' in record:
            if record['name']:
                record['tld'] = record['name'][0]
                record['domain'] = record['name'][1]
                record['subdomain'] = record['name'][2]
                record.pop('name')
        if not record['value']:
            record['value_str'] = _row['value']
        for k in list(record.keys()):
            if not record[k]:
                record.pop(k)
    except:
        pass
    return record


def process(lines: List[str]) -> List[dict]:
    _records = [parse_record(line) for line in lines if line]
    records = list(filter(lambda z:z,_records))
    return records


def process_wrapper(file_name_raw: str,
                    conn_string: str,
                    database: str,
                    collection_name: str,
                    chunk_start: int,
                    chunk_size: int) -> None:
    """

    :param file_name_raw:
    :param conn_string:
    :param database:
    :param collection_name:
    :param chunk_start:
    :param chunk_size:
    :return:
    """

    """ Read a particular chunk """
    client = init_connect_to_mongodb(conn_string)
    db = client[database]
    collection = db[collection_name]
    print(chunk_start)
    with open(file_name_raw, newline='\n') as file:
        file.seek(chunk_start)
        lines = file.read(chunk_size).splitlines()
        block = process(lines)
        try:
            collection.insert_many(block)
        except:
            pass


def chunkify(file_name_raw, size=1024*1024*10):
    """ Return a new chunk """
    with open(file_name_raw, 'rb') as file:
        chunk_end = file.tell()
        while True:
            chunk_start = chunk_end
            file.seek(size, 1)
            file.readline()
            chunk_end = file.tell()
            if chunk_end > file_end:
                chunk_end = file_end
                yield chunk_start, chunk_end - chunk_start
                break
            else:
                yield chunk_start, chunk_end - chunk_start


def init_connect_to_mongodb(connect_string: str) -> MongoClient:

    check = False
    count_repeat = 4
    sleep_sec = 1
    check_i = 0

    while not check and check_i < count_repeat:
        try:
            client = MongoClient(connect_string, serverSelectionTimeoutMS=60)
            client.server_info()
            check = True
        except Exception as ex:
            print(f"try {check_i}, error:'{str(ex)}' connecting - error, sleep - 1 sec.")
            time.sleep(sleep_sec)
            check_i += 1
        else:
            return client


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Importer Sonar dataset to MongoDB (multiprocessing version)')

    parser.add_argument(
        "--conection",
        dest='conection_string',
        type=str,
        default='mongodb://localhost:27017',
        help="conection string MongoDB (ex.: mongodb://localhost:27017), default: mongodb://localhost:27017")

    parser.add_argument(
        "--collection",
        dest='collection',
        type=str,
        help="MongoDB collection name")

    parser.add_argument(
        "--dbname",
        dest='dbname',
        type=str,
        default="sonar",
        help="MongoDB database name, default: sonar")

    parser.add_argument(
        "-f",
        "--input-file",
        dest='input_file',
        type=str,
        help="full path to unpacked file with dataset (ex.: 2020-10-02-1601597234-fdns_any.json)")

    parser.add_argument(
        "--processes",
        dest='processes',
        type=int,
        help="processes, default: 4")

    args = parser.parse_args()
    if not args.collection:
        print('collection name? Exit.')
        exit(1)

    if not args.processes:
        count_cpu = mp.cpu_count()
    else:
        count_cpu = args.processes

    dbname = args.dbname
    file_name_raw = args.input_file
    file_end = os.path.getsize(file_name_raw)
    connection_string = args.conection_string

    pool = mp.Pool(count_cpu)

    jobs = []
    print('Run json reading...')
    collection_name = args.collection
    for chunk_start, chunk_size in chunkify(file_name_raw):
        jobs.append(pool.apply_async(process_wrapper,
                                     (file_name_raw,  connection_string, dbname, collection_name, chunk_start, chunk_size)))

    # wait for all jobs to finish
    for job in jobs:
        job.get()

    # clean up
    pool.close()
    pool.join()
