#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Client driver for the bulletin board system.

Connects to a server and provides an API for sending requests and receiving replies.
"""

from __future__ import annotations
from threading import Thread, Lock
import argparse
import pickle
import json
import time
from socket import *

BUFFER_SIZE = 4096

# Parse arguments from the command line
parser = argparse.ArgumentParser()
parser.add_argument("consistency", help="the consistency method requested by this client: sequential, quorum, or ryw")
args = parser.parse_args()
CONSISTENCY = args.consistency
# TODO - check consistency type, check other args in other places, have tests for these

primary_server_id = 0 # For sequential consistency primary-backup protocol

# Load the hosts array to see every server
hosts = []
with open("./hosts.txt","r") as f:
    while (line := f.readline().rstrip()):
        line = line.strip("\n").split(" ")
        hosts.append([line[0],line[1]])


class BulletinBoardClient():
    def __init__(self, consistency: str, hosts: List[List[str]]):
        print("Client started with consistency mode:",consistency)
        self.server_hostname = ''
        self.server_port = ''
        self.choose_server(hosts)
        self.consistency = consistency
        print("Choosing server", self.server_hostname, self.server_port)
        initial_message = self.udp_send("CONN", "", self.server_hostname, self.server_port)
        print(initial_message)
        
    

    def choose_server(self, hosts: List[List[str]]):
        """
        Apply a choice function to the list of possible servers; update hostname and port that this client sends to.
        """
        server_choice = hosts[0]
        self.server_hostname = server_choice[0]
        self.server_port = server_choice[1]
        # TODO



    def request_data(self):
        # Get the JSON of the bulletin board data, parse it into a Python dictionary, then send for front-end viewing
        if self.consistency == "sequential":
            query = pickle.loads(self.udp_send("PRIMARY-READ", "", self.server_hostname, self.server_port))
            data = json.loads(query)
            if 'articles' in data: # The primary server has sent its data
                self.view_data(data)
            elif 'primary' in data: # The correct primary has been sent back, so reflect this and resend request
                print("New primary:",data)
                self.primary_server_id = data['primary']
                self.server_hostname = hosts[self.primary_server_id][0]
                self.server_port = hosts[self.primary_server_id][1]
                self.request_data()
            else:
                raise Exception("Corrupt data from server:",data)
    
    def send_data(self, postOrReply, content):
        # Send a POST or REPLY
        # Post format: post title%post content
        # Reply format: original post ID%reply content
        # @param postOrReply (str): "post" to post and "reply" for reply
        if self.consistency == "sequential":
            print("Sending post",content,"to server")
            if postOrReply == "post":
                query = self.udp_send("PRIMARY-POST", content, self.server_hostname, self.server_port)
            elif postOrReply == "reply":
                query = self.udp_send("PRIMARY-REPLY", content, self.server_hostname, self.server_port)
            data = pickle.loads(query)
            data = json.loads(data)
            if 'ACK' in data:
                print("ack rec post")
            elif 'primary' in data:
                print("New primary:",data)
                self.primary_server_id = data['primary']
                self.server_hostname = hosts[self.primary_server_id][0]
                self.server_port = hosts[self.primary_server_id][1]
                self.send_data(postOrReply, content)
            else:
                raise Exception("Corrupt data from server:",data)
   



    def view_data(self, data, depth = -1):
        if depth == -1:
            self.view_data(data['articles'], depth+1)
        else:
            for article in data:
                print('\t'*depth, end='')
                if 'title' in article: # Needed because of testing purposes
                    print(article['id'],'  ',article['title'])
                else:
                    print(article)
                if 'replies' in article:
                    self.view_data(article['replies'], depth+1)



    def udp_send(self, header: str, message, recipient: str, port: int) -> None:
        """
        Send a message over UDP
        Parameters:
            header (str): The message header string
            message: The message to send. Note that this can be any Python object that can be put into a dictionary
            recipient (str): The hostname of the recipient (localhost if same device)
            port (int): The port of the recipient
        
        After sending a message, wait for a reply (with timeout)
        """

        # Serialize the message to byte form before sending
        message =   {
                        'HEADER': header,
                        'MESSAGE': message,
                        'RECIPIENT': recipient,
                        'PORT': int(port),
                        'SENDERID': 'CLIENT'
                    }
        message = pickle.dumps(message)

        # Send over UDP
        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
            try:
                udp_socket.sendto(message, (recipient, int(port)))
                
                # Now wait (with timeout) for a reply
                # TODO timeout
                reply, server_address = udp_socket.recvfrom(BUFFER_SIZE)
                #print("Client received reply:",reply)
                return reply
            
            finally:
                udp_socket.close()

            




if __name__ == "__main__":
    # NOTE - Some time wait is needed here for the servers to boot up
    client_instance = BulletinBoardClient(CONSISTENCY, hosts)
    print("Hosts are:",hosts)
    #client_instance.request_data()
    client_instance.send_data("post","First post%Hello world")
    time.sleep(1)
    client_instance.request_data()
    client_instance.send_data("reply","1%Scoop")
    client_instance.request_data()