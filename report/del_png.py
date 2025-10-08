#!/usr/local/python-3.9.2/bin/python3.9 
# -*- coding: utf-8 -*-
import subprocess
#import pexpect
#from pexpect import popen_spawn
import os
import re
import time

### del images/*.png

def del_images() :
    """
    mypath ='images'
    pattern = r'\.png$'

    # Deleting files matching the regex pattern
    for root, dirs, files in os.walk(mypath):
        for file in filter(lambda x: re.search(pattern, x), files):
            os.remove(os.path.join(root, file))
    """
    subprocess.run("rm -rf images/*.png" ,shell=True)
    print('del_images is done')

def del_files() :

    """ 
    mypath ='Dividend_file'
    pattern = r'\.html$'

    # Deleting files matching the regex pattern
    for root, dirs, files in os.walk(mypath):
        for file in filter(lambda x: re.search(pattern, x), files):
            os.remove(os.path.join(root, file))
    """
    subprocess.run("rm -rf Dividend_file/*.html" ,shell=True)
    print('del_files is done')




if __name__ == '__main__':
  del_images()

  time.sleep(1)

  #del_files()
  
