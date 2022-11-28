#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Implements an API for server-server communication, request management, etc.
"""

from __future__ import annotations
from threading import Thread, Lock
import pickle
import time
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

        # Primary node variables
        # Note Chang-Roberts is used for leader election
        self.color = 'red'  # Red or black for Chang-Roberts
        self.init_process = True # Wants to be leader
        self.primary = -1
        self.max_timeout = 3 # Maximum time between heartbeats before timeout occurs
        self.last_heartbeat = time.time()

        # Start the listeners
        self.node_initiate()

        


    def node_initiate(self) -> None:
        # Start the queue listener
        self.queue_listener = Thread(target=self.queue_listener, name=f"queue_listener{self.uid}:{self.port}")
        self.queue_listener.start()
        
        # Start the UDP listener
        self.udp_listener = Thread(target=self.udp_listener, name=f"udp_listener{self.uid}:{self.port}")
        self.udp_listener.start()

        # Start the heartbeat tracker
        self.heartbeat_tracker = Thread(target=self.heartbeat_tracker, name=f"heartbeat_tracker{self.uid}:{self.port}")
        self.heartbeat_tracker.start() 

        print(f"Node {self.uid} is listening on port {self.port}")

    def heartbeat_tracker(self) -> None:
        """
        Keep track of heartbeats; if no heartbeat received within timeout, start a leader election
        """
        while True:
            if (self.primary == self.uid):
                print(f"{uid} IS LEADER SENDING HEARTBEATS!")
            else:
                while not self.primary == self.uid and (abs(time.time() - self.last_heartbeat) < self.max_timeout):
                    continue
                print("TIMEOUT DETECTED")
                return



    def queue_listener(self) -> None:
        """
        Main logic for handling incoming messages goes here.
        """
        neighbor_id = (self.uid+1)%len(self.hosts)
        neighbor_hostname = self.hosts[neighbor_id][0]
        neighbor_port = self.hosts[neighbor_id][1]
        if(self.init_process):
            time.sleep(0.5)
            #send a token w/ self ID to the next neighbor if an initiator process
            print(f"sending first token for process {self.uid} to {neighbor_id}")
            self.udp_send("TOKEN",self.uid,neighbor_hostname,neighbor_port)

        while True:
            if len(self.message_queue) > 0:
                # First get any messages
                queue_mutex.acquire()
                try:
                    message = self.message_queue.pop(0)
                finally:
                    queue_mutex.release()

                #print(message)

                # Leader election messages
                if message["HEADER"] == "TOKEN":
                    recv_id = message["MESSAGE"] # The received ID
                    print(f"Node {self.uid} received token {recv_id} from {message['SENDERID']}")

                    if(self.color == "black"):
                        #When message recieved, just act as router
                        #Send token to next neightbor
                        print("Black router sending")
                        self.udp_send("TOKEN",recv_id,neighbor_hostname,neighbor_port)
                        
                        #If it involves the final leader message, save it
                    else:
                        #If the received ID is less than mine, skip (remove the token received)
                        if(recv_id < self.uid):
                            print(f"Discarding token {recv_id}")
                        #If came back to this process, therefore, I am leader
                        elif(recv_id == self.uid):
                            self.primary = self.uid
                            print(f"Node {self.uid} has received a token from itself (it set {self.primary} to the leader)")
                            self.udp_send("LEADER",self.primary,neighbor_hostname,neighbor_port)
                            self.color = "red" # TODO should this be here?

                        #If the received ID is greater than mine, turn black and forward the token
                        elif(recv_id > self.uid):
                            self.color = 'black'
                            print(f"{recv_id} > {self.uid} (me) -> TURN BLACK, forward {recv_id} to {neighbor_port}")
                            #Then just pass the message onwards because this process quits
                            self.udp_send("TOKEN",recv_id,neighbor_hostname,neighbor_port)

                if message["HEADER"] == "LEADER":
                    if self.primary == -1:    
                        print("Node",self.uid,"has been notified that", message["MESSAGE"], "is the leader")
                        self.primary = message["MESSAGE"]
                        self.udp_send("LEADER",self.primary,neighbor_hostname,neighbor_port)
                        self.color = "red" # TODO should this be here?
                



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


    def client_handler(self, client_address: List[str,int]) -> None:
        print("Client handler invoked",client_address)
        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
            try:
                udp_socket.sendto(b"ACK", client_address)
            finally:
                udp_socket.close()


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

                    if message["SENDERID"] == "CLIENT": # If a message from a client is received, handle it immediately and separately from server requests
                        # Spawn a client handler thread
                        client_handler = Thread(target=self.client_handler, args=(client_address,), name=f"client_handler{client_address}")
                        client_handler.start()
                    
                    elif message["HEADER"] == "HEARTBEAT":
                        # Update the heartbeat immediately, don't send to message queue
                        self.last_heartbeat = time.time()  

                    else: # Add to the queue when a message from another server is received
                        # Lock the message queue and append the message
                        queue_mutex.acquire()
                        try:
                            self.message_queue.append(message)
                        finally:
                            queue_mutex.release()
            finally:
                udp_socket.close()