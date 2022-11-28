#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Implements an API for server-server communication, request management, etc.
"""

from __future__ import annotations
from threading import Thread, Lock
from socket import *

BUFFER_SIZE = 4096 # Max bytes in the message buffer
queue_mutex = Lock()

class BulletinBoardServer():
    def __init__(self, uid: int, hosts: List[List[str]]) -> None:
        """
        Object initialization (occurs before node initialization).
        Sets the UID and loads the hosts array.
        Parameters:
            hosts: A list of lists [hostname, port]
            uid (int): The index of hosts that has this object's [hostname, port] entry
        Example: If uid is 5, then hosts[5] is a list [hostname, port] with this object's hostname and port, and
        hosts[6] and hosts[4] are its neighbors in a ring topology
        """
        self.uid = uid
        self.hosts = hosts

        self.hostname = self.hosts[self.uid][0]
        self.port = self.hosts[self.uid][1]
        
        self.message_queue = []

        self.node_initiate()


    def node_initiate(self) -> None:
        # Start the queue listener
        self.queue_listener = Thread(target=self.queue_listener, name=f"queue_listener{self.uid}:{self.port}")
        self.queue_listener.start()
        
        # Start the UDP listener
        self.udp_listener = Thread(target=self.udp_listener, name=f"udp_listener{self.uid}:{self.port}")
        self.udp_listener.start()

        print(f"Node {self.uid} is listening on port {self.port}")


    def queue_listener(self) -> None:
        """
        Main logic for handling incoming messages goes here.
        """
        pass

    """
    Network interface methods
    """

    def udp_send(self, header: str, message, recipient: str, port: int) -> None:
        """
        Send a message over UDP
        Parameters:
            header (str): The message header string
            message: The message to send. Note that this can be any Python object that can be put into a dictionary
            recipient (str): The hostname of the recipient (localhost if same device)
            port (int): The port of the recipient
        """

        # Serialize the message to byte form before sending
        message =   {
                        'HEADER': header,
                        'MESSAGE': message,
                        'RECIPIENT': recipient,
                        'PORT': int(port),
                        'SENDERID': int(self.uid)
                    }
        message = pickle.dumps(message)

        # Send over UDP
        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
            try:
                udp_socket.sendto(message, (recipient, int(port)))
            finally:
                udp_socket.close()


    def udp_broadcast(self, header: str, message) -> None:
        for node in self.hosts:
            self.udp_send(header, message, node[0], node[1])



    def udp_listener(self) -> None:
        # Listen for UDP messages and append to a global receive queue
        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
            try:
                udp_socket.bind(("", int(self.port)))
                while True:
                    # Receive and deserialize messages
                    # Message is a dictionary with keys
                    # HEADER, MESSAGE, RECIPIENT, PORT, SENDERID
                    message, client_address = udp_socket.recvfrom(BUFFER_SIZE)
                    message = pickle.loads(message)
                    # Lock the message queue and append the message
                    queue_mutex.acquire()
                    try:
                        self.message_queue.append(message)
                    finally:
                        queue_mutex.release()
            finally:
                udp_socket.close()