#!/usr/bin/env python3

import argparse
import boto3
import botocore
import hashlib
import json
import os
import re
import subprocess
from subprocess import Popen, PIPE
import sys
import urllib.request
#import time

from datetime import datetime


# if there are no command line ars run from env vars
# argv[1] = INSTALLPATH
#

def get_db_info():
    confrb = open(os.environ['ASPACE_INSTALL_DIR'] + "/config/config.rb", "r")

    for line in confrb:

        if re.match("^AppConfig(.*)db_url\S \=(.*)UTF-8", line):
            _, _, dbh, nup = line.split('/')
            host = dbh[0:-5]
            dbport = dbh[-4:]
            #print("dbport: " + dbport)
            dbname, userpass = nup.split('?')
            u, p, _, _ = userpass.split('&')
            _, user = u.split('=')
            _, password = p.split('=')
    os.environ['ASDBHOST'] = host
    #print("ASDBHOSTNAME: ", hostname)
    os.environ['ASDBNAME'] = dbname
    #print("ASDBNAME: ", dbname)
    os.environ['ASDBPORT'] = dbport
    #print("ASDBPORT: ", dbport)
    os.environ['ASDBUSER'] = user
    #print("ASDBUSER: ", user)
    os.environ['ASDBPASSWORD'] = password
    #print("ASDBPASSWORD: ", password)

def dump_db():

    now = datetime.now().strftime("%Y%m%d-%H%M%S")

    dumpfile = now + "-" + os.environ['ASDBNAME'] + ".sql.gz"
    opath = "./" + dumpfile

    out = open(opath, 'w+')

    p1 = Popen(["/usr/bin/mysqldump", "-u" + os.environ['ASDBUSER'], "-p" + os.environ['ASDBPASSWORD'],  "-h", os.environ['ASDBHOST'], "-P", os.environ['ASDBPORT'], "-e", "--opt", os.environ['ASDBNAME']],  stdout=subprocess.PIPE)
    p2 = Popen(["gzip", "-c"], stdin=p1.stdout, stdout=out)
    #p1.wait()
    p1.stdout.close()

    output = p2.communicate()[0]

    return dumpfile

def hash_it(file):
    # read from a buffer so we don't use all the system memory
    BUF_SIZE = 65536  # read 64k chunks

    s = hashlib.sha256()

    with open(file, 'rb+') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            s.update(data)

    #print("SHA256: {0}".format(s.hexdigest()))
    d = format(s.hexdigest())

    return d


def put_file(f, h):
    s3 = boto3.resource('s3')

    object_prefix = os.environ['ASDB_OBJ_PREFIX']
    object_path = object_prefix + "/weekly/" + f

    bucket = os.environ['ASDB_BUCKET']

    # verify bucket exists
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
        print("Uploading ", f, "to ", bucket)
        # Add the checksum as metadata
        s3.Object(bucket, object_path).put(Body=open(f, 'rb'), Metadata={'sha256': h})
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print("Private bucket. Forbidden!")
            return True
        if error_code == 404:
            print("Bucket does not exist!")

    if "ASDB_SECONDARY_BUCKET" in os.environ:
        secondary_path = "asdb/" + f

        secondary_bucket = os.environ['ASDB_SECONDARY_BUCKET']

        # verify bucket exists
        try:
            s3.meta.client.head_bucket(Bucket=secondary_bucket)
            print("Uploading ", f, "to ", secondary_bucket)
            # Add the checksum as metadata
            s3.Object(secondary_bucket, secondary_path).put(Body=open(f, 'rb'), Metadata={'sha256': h})
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                print("Private bucket. Forbidden!")
                return True
            if error_code == 404:
                print("Secondary Bucket does not exist!")


def rm_file(file):
    # delete file
    # see https://stackoverflow.com/a/10840586/3447107
    try:
        os.remove(file)
    except OSError as e:
        pass

#def rotate_key(s3, bucket, period):
def rotate_key(bucket, period, object_prefix):
    if period == "weekly":
        next = "monthly"
        total = 5
    elif period == "monthly":
        next = "yearly"
        total = 12

    t = datetime.today().weekday()
    m = datetime.today().strftime("%m")
    d = datetime.today().strftime("%d")

    if next == "monthly":
        odb = {}
        for o in bucket.objects.filter(Prefix=object_prefix + "/"  + period):
            odb[o.key] = o.last_modified

        oldest = min(odb, key=odb.get)

        print("m: ", m)
        print("d: ", d)
        print("oldest: ", oldest)
        ## take the filename off the object key
        _, _, _, oname = oldest.split('/')

        # copy and delete the oldest dump file
        copy_source = {
                'Bucket': os.environ["ASDB_BUCKET"],
                #'Bucket': "dlts-s3-karms",
                'Key': oldest
                }
        k = object_prefix + "/" + next + "/" + oname
        #print("k:      ",k)
        print("copying ",  oldest, " to monthly")
        new = bucket.Object(k)
        new.copy(copy_source)

        if len(odb) <= total:
            print("Less than minimum number of " + period + " backups.  Nothing to delete")
            #return
        else:
            print("deleting: ", oldest)
            response = bucket.Object(oldest).delete()

    # Keeping two full years of backups so 3 dump files
    # to cover the overlap
    if next == "yearly":
        # Prune yearly, keep 3
        ydb = {}
        for y in bucket.objects.filter(Prefix=object_prefix + "/yearly"):
            ydb[y.key] = y.last_modified

        while len(ydb) > 3:
            yoldest = min(ydb, key=ydb.get)
            _, _, _, yname = oldest.split('/')
            response = bucket.Object(yoldest).delete()
            ydb.clear()
            for y in bucket.objects.filter(Prefix=object_prefix + "/yearly/"):
                ydb[y.key] = y.last_modified


def rotate(bucket):
    # list items in the bucket
    # get their timestamps
    # If today is less than 7 move oldest to monthlys
    #   If today is less than 7 and its January move the oldest to yearly
    # If there are more than 4 weeklys delete the oldest
    # If there are more than 12 monthlys delete the oldest
    # If there are more than 2 yearlys delete the oldest
    print("rotate: ", bucket)
    #print(os.environ['ASDB_OBJ_PREFIX'])
    object_prefix = os.environ['ASDB_OBJ_PREFIX']


    ### resource
    s3 = boto3.resource('s3')
    b = s3.Bucket(bucket)

    t = datetime.today().weekday()
    #print("today: ", t) # the numberical day of the week
    m = datetime.today().strftime("%m")
    #print("month: ", m)

    #if t == 0:
    if t == 6:
        print("Rotate weekly")
        rotate_key(b, "weekly", object_prefix)
        #print(object_prefix)

    ##if t == 0 and m == "01":
    if t == 1 and m == "04":
        print("Rotate monthly")
        rotate_key(b, "monthly", object_prefix)
        #print(object_prefix)



def msg(name=None):
    return '''program.py
        asdb2s3 has two functions, backing up the databasedump to
        an s3 bucket, and rotating weekly, monthly and yearly backups.
        asdbs3 is intended for managing archiveval backups of an RDS
        database, hence there are no dailies, which are taken care of
        inside of RDS.

        - To dump the arhcivesspace database,

        `./asdb2s3.py -i <Archivesspace install path> -b <bucket name>

        - Upload an exisitng dumpfile with a checksum

        `./asdb2s3.py -b <bucket name> -f <file name>

        If no arguments are provided asdb2s3 will dump and rotate
        the archivesspace database based on environment variables.
        The available environment vars are as follows,

          ASPACE_INSTALL_DIR
          ASDB_BUCKET
          ASDB_SECONDARY_BUCKET

        '''

def findval(v, k):
    if type(v) == type({}):
        for k1 in v:
            if k1 == k:
                #print(v[k1])
                return v[k1]

            findval(v[k1], k)

def getidoc():
    with urllib.request.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document/') as response:
        r = response.read()
    return r

def gettags(iid):
    ec2 = boto3.resource('ec2',region_name='us-east-1')
    ec2instance =ec2.Instance(iid)
    instancename = ''
    for tags in ec2instance.tags:
        if tags["Key"] == 'Environment':
            env = tags["Value"]
    return env


def noargs():
    print("Run from environment variables")

def main():

    ap = argparse.ArgumentParser(description="Dump and backup archivesspace database asdb", usage=msg() )
    ap.add_argument("-e", "--env", action='store_true', help="run using local environment variables")
    ap.add_argument("-i", "--installdir", nargs=1,
                    help='Archivesspace installation directory')
    ap.add_argument("-b", "--bucket", nargs=1,
                    help="S3 bucked to send the dump to")
    ap.add_argument("-f", "--file", nargs=1, help="name of the dump file")
    ap.add_argument("-r", "--rotate", action='store_true', help="rotate flag")
    ap.add_argument("-t", "--test", action='store_true', help="test")

    #if len(sys.argv)==1:
    #        ap.print_help(sys.stderr)
    #        sys.exit(1)
    args = vars(ap.parse_args())


    d = getidoc()
    iid = findval(json.loads(d), 'instanceId')
    env = gettags(iid)

    if env == "production":
        # Dump asdb and upload to <bucket>/backups/weekly/
        if args["installdir"] and args["bucket"]:
            os.environ['ASDB_OBJ_PREFIX'] = "archivesspace/backups"
            os.environ['ASPACE_INSTALL_DIR'] = args["installdir"][0]
            os.environ["ASDB_BUCKET"] = args["bucket"][0]
            os.environ["ASDB_SECONDARY_BUCKET"] = "dlts-s3-karms"
            get_db_info()
            f = dump_db()  # this works
            h = hash_it(f)
            put_file(f, h)
            rm_file(f)

        # hash and upload file
        if args["bucket"] and args["file"]:
            os.environ["ASDB_BUCKET"] = args["bucket"][0]
            f = args["file"][0]
            h = hash_it(f)
            put_file(f, h)

        # Rotate backups
        if args["rotate"] and args["bucket"]:
            os.environ['ASDB_OBJ_PREFIX'] = "archivesspace/backups"
            print(os.environ['ASDB_OBJ_PREFIX'])
            print("starting rotation")
            os.environ["ASDB_BUCKET"] = args["bucket"][0]
            bucket = os.environ["ASDB_BUCKET"]
            print("rotate_bucket: main")
            rotate(bucket)

        if args["env"] and args["installdir"] and args["bucket"]:
            get_db_info()
            f = dump_db()  # this works
            h = hash_it(f)
            put_file(f, h)
            rm_file(f)
    else:
        print("Backups only run in production")

    # Test will run in all environments
    if args["test"]:
        #d = getidoc()
        #iid = findval(json.loads(d), 'instanceId')
        #print(iid)
        #env = gettags(iid)
        print(iid, " is running in ", env)


if __name__ == "__main__":
    main()
