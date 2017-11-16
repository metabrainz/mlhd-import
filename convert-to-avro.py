#!/usr/bin/env python3

import os
import ujson
import sys
import tarfile
import gzip
import io
import lzma
from time import time

import click
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


UPDATE_INTERVAL = 3
verbose = 1
MAX_SIZE = 100000

def handle_file(member, contents):
    global next_update
    global verbose

    comp = io.BytesIO()
    comp.write(contents)
    comp.seek(0)

    output = []
    count = 0
    user = member.split(".")[0]
    with gzip.GzipFile(fileobj=comp, mode='rb') as f:
        for line in f.readlines():
            line = line.strip()
            cols = line.decode('ascii').split('\t')
            if len(cols) == 4 and cols[3]:
                output.append({
                    'listened_at' : int(cols[0]),
                    'user_name' : user,
                    'artist_mbid' : cols[1],
                    'release_mbid' : cols[2],
                    'recording_mbid' : cols[3]
                })
                count += 1


    return count, output

@click.command()
@click.argument('schema')
@click.argument('src')
@click.argument('dest')
@click.argument('index')
@click.argument('debug')
def import_data(schema, src, dest, index, debug):
    global next_update
    global verbose

    index = int(index)
    verbose = int(debug)
    in_file = os.path.join(src, "MLHD_%03d.tar" % index)
    out_file = os.path.join(dest, "MLHD_%03d.avro" % index)
    count = 0
    next_update = time() + UPDATE_INTERVAL

    schema = avro.schema.Parse(open(schema, "rb").read().decode('ascii'))

    with DataFileWriter(open(out_file, "wb"), DatumWriter(), schema, codec='deflate') as writer:
        tar = tarfile.open(in_file)
        total = 0
        chunks = []
        size = 0
        for i, member in enumerate(tar.getnames()):
            count, data = handle_file(member, tar.extractfile(member).read())
            chunks.append(data)
            total += count
            size += len(data)
            if verbose:
                print("%03d: %d rows processed, %s total rows, %d bytes of output." % (index, count, total, size))
                sys.stdout.flush()

            if size > MAX_SIZE:
                for chunk in chunks:
                    try:
                        for js in chunk:
                            writer.append(js)
                    except Exception as err:
                        print("%03d: err writing file: %s" % (index, err))
                        sys.exit(-1)
                chunks = []
                size = 0



        tar.close()

        if verbose:
            print("%03d: finish writing output file." % index)
            sys.stdout.flush()
        for chunk in chunks:
            try:
                for js in chunk:
                    writer.append(js)
            except Exception as err:
                print("%03d: err writing file: %s" % (index, err))
                sys.exit(-1)


if __name__ == '__main__':
    import_data()
