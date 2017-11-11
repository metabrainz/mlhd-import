#!/usr/bin/env python3

import os
import json
import sys
import tarfile
import gzip
import io
from time import sleep, time

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

def handle_file(member, contents, bq_data):
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

def submit(bigquery, bq_data):
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


@click.command()
@click.argument('dir')
def import_tars(dir):

    total = 0
    last_update_count = 0
    last_update = time()

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
    for index in range(NUM_FILES):
        path = os.path.join(dir, "MLHD_%03d.tar" % index)
        tar = tarfile.open(path)
        for member in tar.getnames():
            handle_file(member, tar.extractfile(member).read(), bq_data)

            while len(bq_data) > SUBMIT_THRESHOLD:
                count = submit(bigquery, bq_data[:SUBMIT_THRESHOLD])
                total += count
                last_update_count += count
                bq_data = bq_data[SUBMIT_THRESHOLD:]

            if time() - last_update > 10.0:
                print("file %d of %d: total %s, %s row/sec, %.2f%% complete" % (index, NUM_FILES, "{:,}".format(total), "{:,}".format(int(last_update_count / (time() - last_update))), total / TOTAL_COUNT))
                last_update_count = 0
                last_update = time()

        tar.close()

    submit(bigquery, bq_data)
 
if __name__ == '__main__':
    import_tars()
