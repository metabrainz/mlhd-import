#!/usr/bin/env python3

import os
import json
import sys
import tarfile
import gzip
import io
from time import sleep, time
from threading import Thread, Lock

import click
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

NUM_FILES = 576
TOTAL_COUNT = 27000000000
SUBMIT_THRESHOLD = 10000
ERROR_RETRY_DELAY = 5

BIGQUERY_PROJECT_ID = 'listenbrainz'
BIGQUERY_DATASET_ID = 'mlhd'
BIGQUERY_TABLE_ID = 'import'
APP_CREDENTIALS_FILE = 'bigquery-credentials.json'

class NoCredentialsFileException(Exception):
    pass
    
class NoCredentialsVariableException(Exception):
    pass

class FileImporter(Thread):

    def __init__(self, dir, file_index):
        super(FileImporter, self).__init__()
        self.done = False
        self.dir = dir
        self.file_index = file_index

    def is_done(self):
        return self.done

    def handle_file(self, member, contents, bq_data):
        comp = io.BytesIO()
        comp.write(contents)
        comp.seek(0)

        user = member.split(".")[0]
        with gzip.GzipFile(fileobj=comp, mode='rb') as f:
            for line in f.readlines():
                line = line.strip()
                cols = line.decode('ascii').split('\t')
                if len(cols) == 4 and cols[3]:
                    data = {
                        'listened_at' : cols[0],
                        'user_name' : user,
                        'artist_mbid' : cols[1],
                        'release_mbid' : cols[2],
                        'recording_mbid' : cols[3]
                    }
                    bq_data.append({
                        'json': data,
                        'insertId': "%s-%s-%s" % (data['user_name'], data['listened_at'], data['recording_mbid'])
                    })

    def submit(self, bigquery, bq_data):
        body = { 'rows' : bq_data }
        while True:
            try:
                ret = bigquery.tabledata().insertAll(
                    projectId=BIGQUERY_PROJECT_ID,
                    datasetId=BIGQUERY_DATASET_ID,
                    tableId=BIGQUERY_TABLE_ID,
                    body=body).execute(num_retries=5)
                return len(bq_data)

            except HttpError as e:
                print("Submit to BigQuery failed: %s. Retrying in 3 seconds." % str(e))

            except Exception as e:
                print("Unknown exception on submit to BigQuery failed: %s. Retrying in 3 seconds." % str(e))

            sleep(ERROR_RETRY_DELAY)


    def run(self):

        if not os.path.exists(APP_CREDENTIALS_FILE):
            logger.error("The BigQuery credentials file does not exist, cannot connect to BigQuery")
            raise NoCredentialsFileException

        try:
            credentials = GoogleCredentials.get_application_default()
            bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
        except (NoCredentialsFileException, NoCredentialsVariableException):
            self.log.error("Credential File not present or invalid! Sleeping...")
            sleep(1000)

        bq_data = []
        path = os.path.join(self.dir, "MLHD_%03d.tar" % self.file_index)
        tar = tarfile.open(path)
        for i, member in enumerate(tar.getnames()):
            self.handle_file(member, tar.extractfile(member).read(), bq_data)
            while len(bq_data) > SUBMIT_THRESHOLD:
                count = self.submit(bigquery, bq_data[:SUBMIT_THRESHOLD])
                update_total(count)
                bq_data = bq_data[SUBMIT_THRESHOLD:]

        tar.close()

        self.submit(bigquery, bq_data)
        self.done = True
        print("file %s complete." % self.file_index)


NUM_THREADS = 25
total = 0
lock = Lock()

def update_total(count):
    global total
    global lock
    lock.acquire()
    total += count
    lock.release()

def get_total():
    global total
    global lock
    lock.acquire()
    t = total
    lock.release()
    return t
 
@click.command()
@click.argument('dir')
def import_data(dir):
    global total

    threads = []
    last_update_total = 0
    last_update = time()
    for file_index in range(NUM_FILES):
        if len(threads) < NUM_THREADS:
            print("Start thread for file %d" % file_index)
            thread = FileImporter(dir, file_index)
            thread.start()
            threads.append(thread)
            continue

        thread_ended = False
        while not thread_ended:
            for i, thread in enumerate(threads):
                if thread.is_done():
                    threads[i].join()
                    del threads[i]
                    thread_ended = True
                    break

            if time() - last_update > 10.0:
                total = get_total()
                print("total %s, %s row/sec, %.2f%% complete" % ("{:,}".format(total), "{:,}".format(int((total - last_update_total) / (time() - last_update))), total / TOTAL_COUNT))
                last_update = time()
                last_update_total = total

            sleep(1)

if __name__ == '__main__':
    import_data()
