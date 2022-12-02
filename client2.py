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
parser.add_argument("consistency", help="the consistency method requested by this client: sequential, quorum, or rw")
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
        self.choose_server(0)
        self.consistency = consistency
        print("Choosing server", self.server_hostname, self.server_port)
        initial_message = self.udp_send("CONN", "", self.server_hostname, self.server_port)
        print(initial_message)
        
    

    def choose_server(self, server_id: int):
        """
        Apply a choice function to the list of possible servers; update hostname and port that this client sends to.
        # TODO FIX SERVER
        """
        server_choice = hosts[server_id]
        self.server_hostname = server_choice[0]
        self.server_port = server_choice[1]
        # TODO randomize



    def request_data(self, readOrChoose, id = 0):
        # Get the JSON of the bulletin board data, parse it into a Python dictionary, then send for front-end viewing
        # readOrChoose is "choose" for a specific article (must supply ID) or "read" for a list of all the articles
        # id is the ID of the post that should be read if "choose" is selected rather than "read"
        if self.consistency == "sequential":
            query = pickle.loads(self.udp_send("PRIMARY-READ", "", self.server_hostname, self.server_port))
            data = json.loads(query)
            if 'articles' in data: # The primary server has sent its data
                if readOrChoose == "read":
                    self.view_data(data)
                elif readOrChoose == "choose":
                    self.view_article(data, id)
            elif 'primary' in data: # The correct primary has been sent back, so reflect this and resend request
                print("New primary:",data)
                self.primary_server_id = data['primary']
                self.server_hostname = hosts[self.primary_server_id][0]
                self.server_port = hosts[self.primary_server_id][1]
                self.request_data(readOrChoose, id)
            else:
                raise Exception("Corrupt data from server:",data)

        # Quorum read requests
        if self.consistency == "quorum" or self.consistency == "rw":
            if self.consistency == "quorum":
                query = pickle.loads(self.udp_send("QUORUM-READ", "", self.server_hostname, self.server_port))
            elif self.consistency == "rw":
                query = pickle.loads(self.udp_send("RW-READ", "", self.server_hostname, self.server_port))

            print(query)
            data = json.loads(query)
            if 'articles' in data: # The quorum has sent its data
                if readOrChoose == "read":
                    self.view_data(data)
                elif readOrChoose == "choose":
                    self.view_article(data, id)
            else:
                raise Exception("Corrupt data from server:",data)
    


    def send_data(self, postOrReply, content):
        # Send a POST or REPLY
        # Post format: post title%post content
        # Reply format: original post ID%reply content
        # @param postOrReply (str): "post" to post and "reply" for reply
        #if self.consistency == "sequential":
        print("Sending post",content,"to server")
        if postOrReply == "post":
            if self.consistency == "sequential":
                query = self.udp_send("PRIMARY-POST", content, self.server_hostname, self.server_port)
            elif self.consistency == "quorum":
                query = self.udp_send("QUORUM-POST", content, self.server_hostname, self.server_port)
            elif self.consistency == "rw":
                query = self.udp_send("RW-POST", content, self.server_hostname, self.server_port)
        elif postOrReply == "reply":
            if self.consistency == "sequential":
                query = self.udp_send("PRIMARY-REPLY", content, self.server_hostname, self.server_port)
            elif self.consistency == "quorum":
                query = self.udp_send("QUORUM-REPLY", content, self.server_hostname, self.server_port)
            elif self.consistency == "rw":
                query = self.udp_send("RW-REPLY", content, self.server_hostname, self.server_port)
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
   


    def view_article(self, data, id, depth = -1):
        # Recursively descend the data, print out article if ID is found
        if depth == -1:
            self.view_article(data['articles'], id, depth+1)
        else:
            for article in data:
                if str(article['id']) == str(id):
                    print(f"ARTICLE:\n\n{article['title']}\n{'-'*len(article['title'])}\n\n{article['contents']}")
                    return
                self.view_article(article['replies'], id, depth+1)



    def view_data(self, data, depth = -1):
        if depth == -1:
            self.view_data(data['articles'], depth+1)
        else:
            for article in data:
                print('\t'*depth, end='')
                if 'title' in article: # Needed because of testing purposes
                    print(article['id'],'  ',article['title'])
                else:
                    print("Foreign format:",article)
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
    # TODO clean up total order multicast
    # NOTE - Some time wait is needed here for the servers to boot up
    client_instance = BulletinBoardClient(CONSISTENCY, hosts)
    print("Hosts are:",hosts)
    #client_instance.request_data()
    client_instance.choose_server(3)
    #client_instance.send_data("post","Test2%Hello Test")
    #client_instance.send_data("reply","1%Hello Reply NEW!")
    #client_instance.send_data("reply","3%Hello world")
    #time.sleep(1)
    #client_instance.choose_server(1)
    client_instance.request_data("read") # TODO - RW READING
    client_instance.request_data("choose", 4)               # TODO - if choose goes out of bounds, is there an error?
    #client_instance.send_data("reply","16%This is working")
    #client_instance.request_data("choose", 22)
    #client_instance.send_data("reply","10%Scoop")
    #client_instance.request_data()
    # TODO - does switching consistency modes break behavior?

    