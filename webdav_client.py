# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import webdav3.client as wc
import os
import sys
import pickle
import numpy as np
from webdav3.exceptions import LocalResourceNotFound, ResponseErrorCode


with open('list_of_red_jpg_paths', 'rb') as f:
    red_jpgs = pickle.load(f)

options = {
       'webdav_hostname': "http://192.168.1.12/files", 
        }
client = wc.Client(options)
client.clean('screensaver')
client.mkdir('screensaver')

targets = np.random.choice(red_jpgs,100,False)

for i, target in enumerate(targets):
    loaded = False
    while loaded == False:
        try:
            client.upload_file(remote_path= f'screensaver/file{i}.jpg', local_path=target)
            loaded = True
        except ResponseErrorCode:
            pass
        except LocalResourceNotFound:
            break
        


#client.check("_DSC0005.JPG")
#client.check("files")
#
#client.list('screensaver')
#client.list()
#
#client.clean('_DSC0003.JPG')
#client.mkdir('screensaver')
#client.clean('/screensaver/*')
#client.upload_sync(remote_path="file1.jpg", local_path='D:/My Pictures/D800/2019-Q1/_DSC0001.JPG')
#client.upload('screensaver/file1.jpg','D:/My Pictures/D800/2019-Q1/_DSC0001.JPG')
#
#client.download_sync(remote_path="_DSC0001.JPG", local_path="~/Downloads/copy.jpg")
#
#client.download_sync(remote_path="screensaver", local_path="~/Downloads/screensaver")
#
##this curl command works
#curl -T "D:/My Pictures/D800/2019-Q1/_DSC0001.JPG" "http://192.168.1.12/files/blah2.jpg"
#
#client.upload_sync(remote_path="file3.jpg", local_path='D:/My Pictures/D800/2019-Q1/_DSC0001.JPG')
#
#client.upload_file(remote_path="file3.jpg", local_path='D:/My Pictures/D800/2019-Q1/_DSC0001.JPG')
#client.upload_file(remote_path="file4.jpg", local_path='D:/My Pictures/D800/2014-1H\\Asia 2014\\Kalaw to Inle Lake\\_DSC5034.JPG')
#
#client.upload_file(remote_path="file20.jpg", local_path='D:/My Pictures/D700/Originals\\2010\\Jpegs\\DSC_4404.JPG')
#
