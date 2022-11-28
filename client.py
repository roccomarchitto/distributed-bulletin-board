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
from socket import *

BUFFER_SIZE = 4096

# Parse arguments from the command line
parser = argparse.ArgumentParser()
parser.add_argument("consistency", help="the consistency method requested by this client: sequential, quorum, or ryw")
args = parser.parse_args()
consistency = args.consistency
# TODO - check consistency type, check other args in other places, have tests for these

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
        print("Choosing server", self.server_hostname, self.server_port)
        self.udp_send("LOL", "hi", self.server_hostname, self.server_port)
        
    
    def choose_server(self, hosts: List[List[str]]):
        """
        Apply a choice function to the list of possible servers; update hostname and port that this client sends to.
        """
        server_choice = hosts[0]
        self.server_hostname = server_choice[0]
        self.server_port = server_choice[1]
        # TODO


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
                print("Client received reply:",reply)
            
            finally:
                udp_socket.close()

            




if __name__ == "__main__":
    client_instance = BulletinBoardClient(consistency, hosts)
    print("Hosts are:",hosts)