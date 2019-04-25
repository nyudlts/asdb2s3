#!/opt/rh/rh-python36/root/bin/python

import boto3
import json
import urllib.request

#with urllib.request.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document/') as response:
#    r = response.read()
#
#print(r)


def findall(v, k):
    if type(v) == type({}):
        for k1 in v:
            if k1 == k:
                #print(v[k1])
                return v[k1]

            findall(v[k1], k)

def gettags(iid):
    ec2 = boto3.resource('ec2',region_name='us-east-1')
    ec2instance =ec2.Instance(iid)
    instancename = ''
    for tags in ec2instance.tags:
        if tags["Key"] == 'Environment':
            env = tags["Value"]
    return env

def main():

    with urllib.request.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document/') as response:
        r = response.read()

    #print(r)

    iid = findall(json.loads(r), 'instanceId')
    print(iid)
    env = gettags(iid)
    print(env)


if __name__ ==  "__main__":
    main()
