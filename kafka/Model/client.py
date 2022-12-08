# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 23:17:38 2021

@author: shash
"""

# Import socket module
import socket			
import json
# Create a socket object
s = socket.socket()		

# Define the port on which you want to connect
port = 12345			

# connect to the server on local computer
s.connect(('127.0.0.1', port))

# receive data from the server and decoding to get the string.
print (s.recv(1024).decode())

dir_path = "C:\\Users\\shash\\Documents\\Sensor Ananlysis\\UCI HAR Dataset"
s.send(dir_path.encode())
# close the connection
data = s.recv(1048576)
print(json.loads(data))
s.close()	
	
