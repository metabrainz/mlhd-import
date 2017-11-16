#!/usr/bin/env python3

import os
import json
import sys
import tarfile
import gzip
import io
import subprocess
from time import sleep, time
from threading import Thread, Lock

import click

NUM_FILES = 576
NUM_THREADS = 32

class FileConverter(Thread):

    def __init__(self, src, dest, file_index):
        super(FileConverter, self).__init__()
        self.done = False
        self.src = src
        self.dest = dest
        self.file_index = file_index

    def is_done(self):
        return self.done

    def run(self):

        cmd = ['./convert-to-avro.py', "schema.json", self.src, self.dest, "%s" % self.file_index, "1"]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as err:
            print("Cannot call converted: %s" % err)
            self.done = True
            return

        self.done = True
        print("file %s complete." % self.file_index)


 
@click.command()
@click.argument('src')
@click.argument('dest')
def import_data(src, dest):

    threads = {}
    last_update_total = 0
    last_update = time()
    file_index = 0
    while file_index < NUM_FILES:
        if len(threads) < NUM_THREADS:
            print("Start thread for file %d" % file_index)
            thread = FileConverter(src, dest, file_index)
            thread.start()
            threads[file_index] = thread
            file_index += 1
            continue

        thread_ended = False
        while not thread_ended:
            for thread in threads:
                if threads[thread].is_done():
                    threads[thread].join()
                    del threads[thread]
                    thread_ended = True
                    break

            sleep(1)

if __name__ == '__main__':
    import_data()
