# -*- coding: utf-8 -*-
"""
Created on Sat Nov 20 09:03:02 2021

@author: shash
"""

from CrossValidation import *
from multiprocessing import Process,Queue,Pipe


def cross_validation(child_conn):
    msg = "Hello"
    child_conn.send(msg)
    child_conn.close()