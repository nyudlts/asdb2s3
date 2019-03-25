#!/usr/bin/env python36

import argparse
import boto3
import botocore
import hashlib
import os
import re
import subprocess
from subprocess import Popen, PIPE
import sys
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
    #s3 = boto3.client('s3')
    s3 = boto3.resource('s3')

    object_path = "backups/weekly/" + f

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



def rm_file(file):
    # delete file
    # see https://stackoverflow.com/a/10840586/3447107
    try: 
        os.remove(file)
    except OSError as e:
        pass

#def rotate_key(s3, bucket, period):
def rotate_key(bucket, period):
    s3 = boto3.resource('s3')
    #b = s3.Bucket(bucket)
    b = bucket
    odb = {}
    for o in b.objects.filter(Prefix="backups/" + period):
        odb[o.key] = o.last_modified
        

    t = datetime.today().weekday()
    print("today: ", t) # the numberical day of the week
    m = datetime.today().strftime("%m")
    print("month: ", m)

    print("type of: ", type(odb))
    oldest = min(odb, key=odb.get)
    #oldest = min(w, key=o.get)
    print('oldest: ', oldest)

    # take the filename off the object key
    _, _, oname = oldest.split('/')
    print('oname: ', oname)

    print("oldest: ", oldest)
    print("copy oldest to next higher period: ", oldest)
    print('b: ', b)
    print('bucket: ', bucket)
    print('env bucket: ', os.environ['ASDB_BUCKET'])
    copy_source = {
            'Bucket': os.environ["ASDB_BUCKET"],
            #'Bucket': "dlts-s3-karms",
            'Key': oldest
            }
    #_, oname = oldest.split('/')
    #new = b.Object("backups/" + period + "/" + oname)
    new = b.Object("backups/monthly/" + oname)
    print("backups/" + period + "/" + oname)
    print("new: ", new)

    print("copy course: ", copy_source)
    new.copy(copy_source)

    obj = s3.Object(bucket, oldest)
### HERE
    #response = obj.delete()
    #print("response: ", response)



def rotate(bucket):
    # list itmes in the bucket 
    # get their timestamps
    # If today is less than 7 move oldest to monthlys
    #   If today is less than 7 and its January move the oldest to yearly
    # If there are more than 4 weeklys delete the oldest
    # If there are more than 12 monthlys delete the oldest
    # If there are more than 2 yearlys delete the oldest
    print("rotate: ", bucket)

    ### resource
    s3 = boto3.resource('s3')
    b = s3.Bucket(bucket)



    t = datetime.today().weekday()
    print("today: ", t) # the numberical day of the week
    m = datetime.today().strftime("%m")
    print("month: ", m)


    print("t: ", t)
    if t == 0:
        print("Rotate weekly")
        rotate_key(b, "weekly")
#
#
### end weekly
    # rotate all objects in the weekly folder
    #rotate_key(s3, b, "weekly")

#    print("------copy oldest to yearly: ", oldest)
#    #print("len(odb): ", len(odb))
#    # if month = 01 (03 for testing) move object to monthly
#    if m == '03' and t <= 6:
#        #print("move the oldest daily to the yearly: ", m)
#        copy_source = {
#                'Bucket': bucket,
#                'Key': oldest
#                }
#        new = b.Object("backups/yearly/" + oname)
#        new.copy(copy_source)
#

    # Prune
#    while len(odb) > 7:
#        # delete oldest 
#        oldest = min(odb, key=odb.get)
#        print("---delete oldest: ", oldest)
#        print("oname: ", oname)
#        obj = s3.Object(bucket, oldest)
#        response = obj.delete()
#        odb.clear()
#        for o in b.objects.filter(Prefix="backups/"):
#            odb[o.key] = o.last_modified
#        print("len(odb): ", len(odb))

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

        '''

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
    #if len(sys.argv)==1:
    #        ap.print_help(sys.stderr)
    #        sys.exit(1)
    args = vars(ap.parse_args())

    # Dump asdb and upload to <bucket>/backups/weekly/
    if args["installdir"] and args["bucket"]:
        os.environ['ASPACE_INSTALL_DIR'] = args["installdir"][0]
        os.environ["ASDB_BUCKET"] = args["bucket"][0]
        get_db_info()
        f = dump_db()  # this works
        h = hash_it(f)
        put_file(f, h)
        rm_file(f)

    if args["bucket"] and args["file"]:
        os.environ["ASDB_BUCKET"] = args["bucket"][0]
        f = args["file"][0]
        h = hash_it(f)
        put_file(f, h)

    if args["rotate"] and args["bucket"]:
        print("starting rotation")
        os.environ["ASDB_BUCKET"] = args["bucket"][0]
        bucket = os.environ["ASDB_BUCKET"] 
        rotate(bucket)

    if args["env"] and args["installdir"] and args["bucket"]:
        get_db_info()
        f = dump_db()  # this works
        h = hash_it(f)
        put_file(f, h)
        rm_file(f)


if __name__ == "__main__":
    main()
