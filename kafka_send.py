#!/usr/local/bin/python3

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer.future import RecordMetadata

import sys
import ssl
import json
import time
import os
import gzip

from random import randint

import datetime
import time

from collections import OrderedDict

import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

print('Starting kafka_send.py')

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
# context = ssl.create_default_context()
# context.options &= ssl.OP_NO_TLSv1
# context.options &= ssl.OP_NO_TLSv1_1
def get_offset_s(store_num):
    offset = 0
    if store_num != 0:
        offset = randint(-60, 60)
    return offset
def load_records(store_num):

    runon = [] # store the dates yyyymmdd that the load has already run

    # this loop will cause the load to run once every day
    while True:

        # # if we've already run today, keep skipping until tomorrow
        rundate = datetime.datetime.now()
        rundate_yyyymmdd = rundate.strftime("%Y%m%d")
        # if rundate_yyyymmdd in runon:
        #     print('runon{} contains {} - waiting until tomorrow before processing again.'.format(runon, rundate_yyyymmdd))
        #     time.sleep(600) # sleep 10 minutes
        #     # skip loading the data - let's try again
        #     continue
        # else:
        #     print("runon{} doesn't contains {} - processing file.".format(runon, rundate_yyyymmdd))

        print('Starting new loop')

        # start with a fresh producer
        producer = KafkaProducer(bootstrap_servers=['ec2-34-214-236-207.us-west-2.compute.amazonaws.com:9092'],api_version=(0,10))

        with gzip.open('OnlineRetail.json.gz', 'rt') as tx_f:

            lines_processed = 0
#             write_lines_processed(lines_processed)
#             write_tps(0)

            start_time = datetime.datetime.today().timestamp()

            for line in tx_f:

                # introduce some variability to the transactions
                if randint(0, 9) <= 3:
                    continue

                # load the record - use an OrderedDict to keep the fields in order
                j = json.loads(line, object_pairs_hook=OrderedDict)
                tx_dt = datetime.datetime.fromtimestamp(int(j['InvoiceDate'])/1000)
                tx_dt = tx_dt.replace(year=rundate.year, month=rundate.month, day=rundate.day)
                # add some randomness to the invoicedate
                tx_dt = tx_dt + datetime.timedelta(seconds=get_offset_s(store_num))

                tx_time = tx_dt.strftime('%H:%M:%S')
                j['InvoiceTime'] = tx_time
                j['InvoiceDate'] = int(tx_dt.strftime('%S')) * 1000

                j['StoreID'] = int(store_num)

                # TODO - use 3 digits for store, 4 for line num
                j['TransactionID'] = str(j['InvoiceNo']) + str(j['LineNo']) + str(store_num) + tx_dt.strftime('%y%m%d')

                if lines_processed == 0:
                    print('Processing first record', json.dumps(j))

                producer.send("retail_demo",json.dumps(j).encode('utf-8'))
                lines_processed += 1

                # print status every 10000 records processed
                if lines_processed < 10 or lines_processed % 10000 == 0:
        #             write_lines_processed(lines_processed)
                      print("{} lines processed".format(lines_processed))

        #         if lines_processed % 1000 == 0:
        #             time_diff = datetime.datetime.today().timestamp() - start_time
        #             write_tps(1000 / time_diff)
        #             start_time = datetime.datetime.today().timestamp()

            producer.flush()
            producer.close()
            print('Finished processing records. Flushed and Closed producer ...')

if __name__ == "__main__":

    store_num = 45
    load_records(store_num)
