#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Launch a distributed bulletin board system server.
"""

from dbbs.server import BulletinBoardServer
import argparse

# Parse arguments from the command line
parser = argparse.ArgumentParser()
parser.add_argument("uid", help="the unique identifier of this server instance")
args = parser.parse_args()
uid = int(args.uid)

# Load the hosts array to see every other server
hosts = []
with open("./hosts.txt","r") as f:
    while (line := f.readline().rstrip()):
        line = line.strip("\n").split(" ")
        hosts.append([line[0],line[1]])

if __name__ == "__main__":
    #print(uid,":",hosts)
    # Launch a BulletinBoardServer instance
    server_instance = BulletinBoardServer(uid, hosts)