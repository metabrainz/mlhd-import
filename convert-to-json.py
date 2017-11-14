#!/usr/bin/env python3

import os
import ujson
import sys
import tarfile
import gzip
import io
from time import time

import click

UPDATE_INTERVAL = 3
verbose = 1
MAX_SIZE = 10000000

def handle_file(member, contents):
    global next_update
    global verbose

    comp = io.BytesIO()
    comp.write(contents)
    comp.seek(0)

    output = ""
    count = 0
    user = member.split(".")[0]
    with gzip.GzipFile(fileobj=comp, mode='rb') as f:
        for line in f.readlines():
            line = line.strip()
            cols = line.decode('ascii').split('\t')
            if len(cols) == 4 and cols[3]:
                output += ujson.dumps({
                    'listened_at' : cols[0],
                    'user_name' : user,
                    'artist_mbid' : cols[1],
                    'release_mbid' : cols[2],
                    'recording_mbid' : cols[3]
                })
                output += "\n"
                count += 1


    return count, output

@click.command()
@click.argument('src')
@click.argument('dest')
@click.argument('index')
@click.argument('debug')
def import_data(src, dest, index, debug):
    global next_update
    global verbose

    index = int(index)
    verbose = int(debug)
    in_file = os.path.join(src, "MLHD_%03d.tar" % index)
    out_file = os.path.join(dest, "MLHD_%03d.json" % index)
    count = 0
    next_update = time() + UPDATE_INTERVAL

    with open(out_file, "w") as f:
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
                print("%d rows processed, %s total rows, %d bytes of output." % (count, total, size))
                sys.stdout.flush()

            if size > MAX_SIZE:
                if verbose:
                    print("writing to output file.")
                    sys.stdout.flush()
                for chunk in chunks:
                    try:
                        f.write(chunk)
                    except IOError as err:
                        print("err writing file: %s" % err)
                        sys.exit(-1)
                chunks = []
                size = 0

        tar.close()

        if verbose:
            print("Finish writing output file.")
            sys.stdout.flush()
        for chunk in chunks:
            try:
                f.write(chunk)
            except IOError as err:
                print("err writing file: %s" % err)
                sys.exit(-1)


if __name__ == '__main__':
    import_data()
