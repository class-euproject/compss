#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import time
from pycompss.api.api import compss_open
from pycompss.api.task import task
from pycompss.api.parameter import * 

@task(out_coll=COLLECTION_FILE_OUT)
def gen_collection(out_coll):
    for out_file in out_coll:
       text_file = open(out_file, "w")
       text_file.write(str(1))
       text_file.close()

@task(inout_coll=COLLECTION_FILE_INOUT)
def update_collection(inout_coll):
    for inout_file in inout_coll:
        print("Writing one in file " + inout_file)
        text_file = open(inout_file, "a")
        text_file.write(str(1))
        text_file.close()

@task(in_coll=COLLECTION_FILE_IN)
def read_collection(in_coll):
    for in_file in in_coll:
       text_file = open(in_file, "r")
       val = text_file.readline()
       text_file.close()
       #if int(val) == 11:
       #    print("Value correct")
       #else:
       #    raise Exception("Incorrect value read " + val)


if __name__ == '__main__':
    file_collection = ['file_1', 'file_2', 'file_3']
    gen_collection(file_collection)
    update_collection(file_collection)
    read_collection(file_collection)
    read_collection(file_collection)
    for in_file in file_collection:
       text_file = compss_open(in_file, "r")
       val = text_file.readline()
       text_file.close()
       if int(val) == 11:
           print("Value correct for " + in_file)
       else:
           raise Exception("Incorrect value for " + in_file + "Value was " + val + " (Expecting 11)" ) 
