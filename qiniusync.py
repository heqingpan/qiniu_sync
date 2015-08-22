#!/usr/bin/env python
#-*- coding:utf-8 -*-
# 
# AUTHOR = "heqingpan"
# AUTHOR_EMAIL = "heqingpan@126.com"
# URL = "http://git.oschina.net/hqp/qiniu_sync"

import qiniu
from qiniu import Auth
from qiniu import BucketManager
import os
import re

access_key = ''
secret_key = ''
bucket_name = ''
bucket_domain = ''

q = Auth(access_key, secret_key)
bucket = BucketManager(q)
basedir=os.path.realpath(os.path.dirname(__file__))
filename=__file__
ignore_paths=[filename,"{0}c".format(filename)]
ignore_names=[".DS_Store",".git",".gitignore"]
charset="utf8"
diff_time=2*60


def list_all(bucket_name, bucket=None, prefix="", limit=100):
    rlist=[]
    if bucket is None:
        bucket = BucketManager(q)
    marker = None
    eof = False
    while eof is False:
        ret, eof, info = bucket.list(bucket_name, prefix=prefix, marker=marker, limit=limit)
        marker = ret.get('marker', None)
        for item in ret['items']:
            rlist.append(item["key"])
    if eof is not True:
        # 错误处理
        #print "error"
        pass
    return rlist

def get_files(basedir="",fix="",rlist=None,ignore_paths=[],ignore_names=[]):
    if rlist is None:
        rlist=[]
    for subfile in os.listdir(basedir):
        temp_path=os.path.join(basedir,subfile)
        if temp_path in ignore_names:
            continue
        tp=os.path.join(fix,subfile)
        if tp in ignore_paths:
            continue
        if os.path.isfile(temp_path):
            rlist.append(tp)
        elif os.path.isdir(temp_path):
            get_files(temp_path,tp,rlist,ignore_paths,ignore_names)
    return rlist

def get_valid_key_files(subdir=""):
    basedir=subdir or basedir
    files = get_files(basedir=basedir,ignore_paths=ignore_paths,ignore_names=ignore_names)
    return map(lambda f:(f.replace("\\","/"),f),files)


def sync():
    qn_keys=list_all(bucket_name,bucket)
    qn_set=set(qn_keys)
    l_key_files=get_valid_key_files(basedir)
    k2f={}
    update_keys=[]
    u_count=500
    u_index=0
    for k,f in l_key_files:
        k2f[k]=f
        str_k=k
        if isinstance(k,str):
            k=k.decode(charset)
        if k in qn_set:
            update_keys.append(str_k)
            u_index+=1
            if u_index > u_count:
                u_index-=u_count
                update_file(k2f,update_keys)
                update_keys=[]
        else:
            # upload
            upload_file(k,os.path.join(basedir,f))
    if update_keys:
        update_file(k2f,update_keys)
    print "sync end"

def update_file(k2f,ulist):
    ops=qiniu.build_batch_stat(bucket_name,ulist)
    rets,infos = bucket.batch(ops)
    for i in xrange(len(ulist)):
        k=ulist[i]
        f=k2f.get(k)
        ret=rets[i]["data"]
        size=ret.get("fsize",None)
        put_time = int(ret.get("putTime")/10000000)
        local_size=os.path.getsize(f)
        local_time=int(os.path.getatime(f))
        if local_size==size:
            continue
        if put_time >= local_time - diff_time:
            # is new
            continue
        # update
        upload_file(k,os.path.join(basedir,f))

def upload_file(key,localfile):
    print "upload_file:"
    print key
    token = q.upload_token(bucket_name, key)
    mime_type = get_mime_type(localfile)
    params = {'x:a': 'a'}
    progress_handler = lambda progress, total: progress
    ret, info = qiniu.put_file(token, key, localfile, params, mime_type, progress_handler=progress_handler)

def get_mime_type(path):
    mime_type = "text/plain"
    return mime_type

def main():
    sync()

if __name__=="__main__":
    main()
