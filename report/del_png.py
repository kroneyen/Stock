#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import subprocess


### del images/*.png

def del_images() :
 
    #subprocess.run(["rm", "-rf" ,"images/*.png"])
    subprocess.run("rm -rf images/*.png" ,shell=True)
