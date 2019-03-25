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

    object_path = "tmp/" + f
    #object_path = "./" + f

    bucket = os.environ['ASDB_BUCKET']

    # verify bucket exists
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
        #s3.meta.client.head_bucket(Bucket="dlts-3-karm")
        print("Bucket exists!", bucket)
        #s3.upload_file(dumpfile,bucket,object_path)
        s3.Object(bucket, object_path).put(Body=open(f, 'rb'), Metadata={'sha256': h})
        #s3.Object(bucket, object_path).put(body=open(f, 'rb'))
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print("Private bucket. Forbidden!")
            return True
        if error_code == 404:
            print("Bucket does not exist!")


    # delete file

    # this gets the file there, now I need to arrange weekly, 
    # monthly and yearly backups

#def archive_dumps():
    #   Arrange bucket objects into weekly, monthly and yearly

def noargs():
    print("Run from environment variables")

def main():

    ap = argparse.ArgumentParser(description="Dump and archive db")
    ap.add_argument("-i", "--installdir", nargs=1,
                    help='Archivesspace installdir')
    ap.add_argument("-b", "--bucket", nargs=1,
                    help="S3 bucked to send the dump to")
    #ap.add_argument("-p", "--put", nargs=1, help="expects bucketname")
    ap.add_argument("-f", "--file", nargs=1, help="expects filename")
    args = vars(ap.parse_args())

    #if args["installdir"]:
#
#        os.environ['ASPACE_INSTALL_DIR'] = args["installdir"][0]
#        #print("ASPACE_INSTALL_DIR: " , os.environ['ASPACE_INSTALL_DIR'])
#                    
#    if args["bucket"]:
#        os.environ["ASDB_BUCKET"] = args["bucket"]

    if args["installdir"] and args["bucket"]:
        os.environ['ASPACE_INSTALL_DIR'] = args["installdir"][0]
        os.environ["ASDB_BUCKET"] = args["bucket"][0]
        get_db_info()
        fname = dump_db()  # this works
        sha256_digest = hash_it(fname)
        #put_file(dumpfile)
    if args["bucket"] and args["file"]:
        os.environ["ASDB_BUCKET"] = args["bucket"][0]
        b = os.environ["ASDB_BUCKET"] 
        f = args["file"][0]
        h = hash_it(f)
        put_file(f, h)





if __name__ == "__main__":
    main()
