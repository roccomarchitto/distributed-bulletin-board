#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Implements an API for server-server communication, request management, etc.
"""

from __future__ import annotations
from threading import Thread, Lock
import pickle
import time
import os
import random
import json
from socket import *


BUFFER_SIZE = 4096 # Max bytes in the message buffer
DEBUG = True
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
        self.max_timeout = 10 # Maximum time between heartbeats before timeout occurs
        self.last_heartbeat = time.time()

        # Bulletin board content variables
        self.bulletin_board = ''
        self.latest_id = 0
        
        sample_json = '{"articles": [{"id": 5,"title": "x","contents": "y","replies": [{"id": 6,"title": "xx","contents": "yy","replies": []}]}] }'
        self.bulletin_board = json.loads(sample_json)
        print(self.bulletin_board)
        for i in self.bulletin_board['articles']:
            print(i,'\n')
        # TODO 
        # len(bulletin_board['articles'][0]['replies']) is 1

        # Quorum consistency
        self.read_quorum = [0, 2] # TODO choose a subset
        self.write_quorum = [0, 2] # TODO choose a subset

        # Start the listeners
        self.node_initiate()

        


    def node_initiate(self) -> None:
        # Start the queue listener
        self.queue_listener = Thread(target=self.queue_listener, name=f"queue_listener{self.uid}:{self.port}")
        self.queue_listener.start()
        
        # Start the UDP listener
        self.udp_listener = Thread(target=self.udp_listener, name=f"udp_listener{self.uid}:{self.port}")
        self.udp_listener.start()

        # Start the heartbeat tracker and sender
        self.heartbeat_tracker = Thread(target=self.heartbeat_tracker, name=f"heartbeat_tracker{self.uid}:{self.port}")
        self.heartbeat_tracker.start() 
        self.heartbeat_sender = Thread(target=self.heartbeat_sender, name=f"heartbeat_sender{self.uid}:{self.port}")
        self.heartbeat_sender.start()

        print(f"Node {self.uid} is listening on port {self.port}")

    def heartbeat_tracker(self) -> None:
        """
        Keep track of heartbeats; if no heartbeat received within timeout, start a leader election
        """
        while True:
            while (abs(time.time() - self.last_heartbeat) < self.max_timeout):
                continue
            print("TIMEOUT DETECTED")
            self.primary = -1 # To be updated when new leader is elected
            self.last_heartbeat = time.time() # Reset the timer
            time.sleep(0.2) # TODO FIX THIS WHOLE SECTION
            self.hosts = self.hosts[:-1] # Remove current leader
            self.neighbor_id = (self.uid+1)%len(self.hosts)
            self.neighbor_hostname = self.hosts[self.neighbor_id][0]
            self.neighbor_port = self.hosts[self.neighbor_id][1]
            #print(f"{self.uid}: {self.hosts} sending to {self.neighbor_id}, {len(self.hosts)}")
            self.udp_send("TOKEN",self.uid,self.neighbor_hostname,self.neighbor_port)
            if len(self.hosts) < 2:
                print("Too few hosts to continue - exiting.")
                os._exit(1)
            # TODO update neighbors

    def heartbeat_sender(self) -> None:
        """
        Periodically send heartbeats
        """
        count = 0 # For testing purposes
        while True:
            if (self.uid == self.primary): #and count < 3: # TODO
                time.sleep(0.5)
                self.udp_broadcast("HEARTBEAT","")
                #print("HEARTBEAT SENT BY",self.uid)
                count += 1
            

    def queue_listener(self) -> None:
        """
        Main logic for handling queue-based server-server messages.
        """
        self.neighbor_id = (self.uid+1)%len(self.hosts)
        self.neighbor_hostname = self.hosts[self.neighbor_id][0]
        self.neighbor_port = self.hosts[self.neighbor_id][1]
        if(self.init_process):
            time.sleep(0.5)
            #send a token w/ self ID to the next neighbor if an initiator process
            print(f"sending first token for process {self.uid} to {self.neighbor_id}")
            self.udp_send("TOKEN",self.uid,self.neighbor_hostname,self.neighbor_port)

        while True:
            if len(self.message_queue) > 0:
                # First get any messages
                queue_mutex.acquire()
                try:
                    message = self.message_queue.pop(0)
                finally:
                    queue_mutex.release()

                if message["HEADER"] == "QUORUM-READ-PRIMARY":
                    if (self.primary == self.uid):
                        if DEBUG: print("Primary received quorum read request")
                        client_address = message["MESSAGE"]
                        # Multicast request to quorum, find most up to date (by ID) bulletin, then send back to the client
                        bulletins = []
                        for server_id in self.read_quorum:
                            hostname = self.hosts[server_id][0]
                            port = self.hosts[server_id][1]
                            msg =   {
                                            'HEADER': 'QUORUM-READ-REQ',
                                            'MESSAGE': '',
                                            'RECIPIENT': hostname,
                                            'PORT': int(port),
                                            'SENDERID': self.uid
                                        }
                            msg = pickle.dumps(msg)
                            # Manually send over UDP (since sending requirements differ here from the udp_send function)
                            with socket(AF_INET, SOCK_DGRAM) as udp_socket:
                                try:
                                    udp_socket.sendto(msg, (hostname, int(port))) # TODO timeout here?
                                    reply, server_address = udp_socket.recvfrom(BUFFER_SIZE)
                                    quorum_reply = pickle.loads(reply)
                                    bulletins.append(quorum_reply)
                                finally:
                                    udp_socket.close()

                        # Now make another socket and send to the client
                        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
                            data = pickle.dumps(bulletins[0]) # TODO choose bulletin w/ highest ID
                            curr_max_id = 0
                            curr_max_bulletin = bulletins[0]
                            for bulletin in bulletins:
                                highest_id = self.find_highest_id(json.loads(bulletin))
                                if highest_id > curr_max_id:
                                    print("OVERRIDE",bulletin,curr_max_bulletin)
                                    curr_max_bulletin = bulletin
                                    curr_max_id = highest_id
                            udp_socket.sendto(pickle.dumps(curr_max_bulletin), client_address)
                        
                    else:
                        raise Exception("Inconsistent primaries")


                # Leader election messages
                if message["HEADER"] == "TOKEN":
                    recv_id = message["MESSAGE"] # The received ID
                    print(f"Node {self.uid} received token {recv_id} from {message['SENDERID']}")

                    if(self.color == "black"):
                        #When message recieved, just act as router
                        #Send token to next neightbor
                        print("Black router sending")
                        self.udp_send("TOKEN",recv_id,self.neighbor_hostname,self.neighbor_port)
                        
                        #If it involves the final leader message, save it
                    else:
                        #If the received ID is less than mine, skip (remove the token received)
                        if(recv_id < self.uid):
                            print(f"Discarding token {recv_id}")
                        #If came back to this process, therefore, I am leader
                        elif(recv_id == self.uid):
                            self.primary = self.uid
                            print(f"Node {self.uid} has received a token from itself (it set {self.primary} to the leader)")
                            #time.sleep(.5) # For debug purposes, delay choice of new leader
                            self.udp_send("LEADER",self.primary,self.neighbor_hostname,self.neighbor_port)
                            self.color = "red" # TODO should this be here?

                        #If the received ID is greater than mine, turn black and forward the token
                        elif(recv_id > self.uid):
                            self.color = 'black'
                            print(f"{recv_id} > {self.uid} (me) -> TURN BLACK, forward {recv_id} to {self.neighbor_port}")
                            #Then just pass the message onwards because this process quits
                            self.udp_send("TOKEN",recv_id,self.neighbor_hostname,self.neighbor_port)

                if message["HEADER"] == "LEADER":
                    if self.primary == -1:    
                        print("Node",self.uid,"has been notified that", message["MESSAGE"], "is the leader")
                        self.primary = message["MESSAGE"]
                        self.udp_send("LEADER",self.primary,self.neighbor_hostname,self.neighbor_port)
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

        latency = random.random()*0.1
        time.sleep(latency) # TODO client-sending latencies
        # TODO BUG - when latency is set to 4, two leaders may be elected at the same time due to atomicity concerns (possible assumption?)

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


    def client_handler(self, client_address: List[str,int], message: dict) -> None:
        """
        Handle requests from clients rather than servers here.
        """
        print("Client handler invoked",client_address)
        with socket(AF_INET, SOCK_DGRAM) as udp_socket:
            try:
                print(message)
                if message["HEADER"] == "CONN": # Client notified server that it connected
                    udp_socket.sendto(b"ACK", client_address)
                elif message["HEADER"] == "REQUEST": # Client is requesting the bulletin board TODO FIX
                    self.bulletin_board['articles'].append("{'test':'OK'}")
                    data = json.dumps(self.bulletin_board) # Convert dict to JSON for parsing
                    data = pickle.dumps(data) # Encode JSON in byte form
                    udp_socket.sendto(data, client_address)
                elif message["HEADER"] == "PRIMARY-READ":
                    """
                    These are requests made in sequential consistency mode. Send info back if leader, else send back leader, or spin wait until a leader is chosen.
                    """
                    # TODO is there a problem when a leader is being waited on too long?
                    if (self.primary == self.uid):
                        data = json.dumps(self.bulletin_board) # Convert dict to JSON for parsing
                        data = pickle.dumps(data) # Encode JSON in byte form
                        udp_socket.sendto(data, client_address)
                    else:
                        while self.primary == -1:
                            continue
                        # A leader is now chosen at this point; send its ID back to the client
                        data = dict()
                        data['primary'] = self.primary
                        data = json.dumps(data)
                        data = pickle.dumps(data)
                        udp_socket.sendto(data, client_address)
                elif message["HEADER"] == "PRIMARY-POST":
                    """
                    These are posts made in sequential mode. Send ack if leader, else send back leader, or spin wait until a leader is chosen.
                    """
                    if (self.primary == self.uid):
                        title = message['MESSAGE'].split('%')[0]
                        body = message['MESSAGE'].split('%')[1]
                        print("Appending post",title,":",body)
                        #'{"articles": [{"id ": 5,"title": "x","contents": "y","replies": [{"id ": 6,"title": "xx","contents": "yy","replies": []}]}, {"hi":"ok"} ] }'
                        new_article = dict()
                        self.latest_id += 1
                        new_article['id'] = self.latest_id
                        new_article['title'] = title
                        new_article['contents'] = body
                        new_article['replies'] = []
                        self.bulletin_board['articles'].append(new_article)
                        data = dict()
                        data['ACK'] = 1
                        data = json.dumps(data)
                        data = pickle.dumps(data)
                        udp_socket.sendto(data, client_address)
                    else:
                        while self.primary == -1:
                            continue
                        # A leader is now chosen at this point; send its ID back to the client
                        data = dict()
                        data['primary'] = self.primary
                        data = json.dumps(data)
                        data = pickle.dumps(data)
                        udp_socket.sendto(data, client_address)

                elif message["HEADER"] == "PRIMARY-REPLY":
                    """
                    These are replies made in sequential mode. Send ack if leader, else send back leader, or spin wait until a leader is chosen.
                    """
                    if (self.primary == self.uid):
                        original = message['MESSAGE'].split('%')[0] # The ID of the original post; we will reply to this ID
                        body = message['MESSAGE'].split('%')[1] 
                        print("Replying to post ID",original,":",body)
                        #'{"articles": [{"id ": 5,"title": "x","contents": "y","replies": [{"id ": 6,"title": "xx","contents": "yy","replies": []}]}, {"hi":"ok"} ] }'
                        new_article = dict()
                        self.latest_id += 1
                        new_article['id'] = self.latest_id
                        new_article['title'] = "Reply to Article " + original
                        new_article['contents'] = body
                        new_article['replies'] = []

                        # Need to find the 'replies' section of the post that has the desired ID, and append there
                        self.append_reply(original, new_article, self.bulletin_board)

                        #self.bulletin_board['articles'].append(new_article)

                        data = dict()
                        data['ACK'] = 1
                        data = json.dumps(data)
                        data = pickle.dumps(data)
                        udp_socket.sendto(data, client_address)
                    else:
                        while self.primary == -1:
                            continue
                        # A leader is now chosen at this point; send its ID back to the client
                        data = dict()
                        data['primary'] = self.primary
                        data = json.dumps(data)
                        data = pickle.dumps(data)
                        udp_socket.sendto(data, client_address) 


                ### QUORUM CONSISTENCY CLIENT REQUESTS

                elif message["HEADER"] == "QUORUM-READ":
                    print("Quorum read on server requested")
                    # Send a message to the primary -> primary sends a message back with data
                    self.udp_send("QUORUM-READ-PRIMARY", client_address, self.hosts[self.primary][0], self.hosts[self.primary][1])



            finally:
                udp_socket.close()


    # Bulletin methods TODO: Reorganize

    def append_reply(self, id, reply, current_section, depth = -1) -> None:
        # Recursively append a reply to article ID id, to the self bulletin
        if depth == -1:
            self.append_reply(id, reply, self.bulletin_board['articles'], depth+1)
        else:
            for article in current_section:
                if str(article['id']) == str(id):
                    print("FOUND ARTICLE",article)
                    article['replies'].append(reply)
                    return
                self.append_reply(id, reply, article['replies'], depth+1)
              


    def find_highest_id(self, current_section, depth = -1) -> None:
        # Recursively find the highest ID in the bulletin board
        if depth == -1:
            return self.find_highest_id(current_section['articles'], depth+1)
        else:
            id_list = []
            for article in current_section:
                if not article['replies']:
                    return current_section[0]['id']
                id_list.append(int(self.find_highest_id(article['replies'], depth+1)))
                id_list.append(int(current_section[0]['id']))
            print("ID List:",id_list)
            return max(id_list)
                


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
                        client_handler = Thread(target=self.client_handler, args=(client_address,message), name=f"client_handler{client_address}") # TODO does this allow for multiple threads? Are the non unique names a problem?
                        client_handler.start()
                    
                    elif message["HEADER"] == "HEARTBEAT":
                        # Update the heartbeat immediately, don't send to message queue
                        self.last_heartbeat = time.time()
                        #print("H @",self.uid,time.time())
                    
                    # Quorum requests are handled here rather than in the queue (this is due to the queue not saving socket addresses in the original framework)
                    elif message["HEADER"] == "QUORUM-READ-REQ":
                        data = pickle.dumps(json.dumps(self.bulletin_board)) # Convert dict to JSON for parsing
                        udp_socket.sendto(data, client_address) # TODO change naming of client_address to avoid confusion

                    else: # Add to the queue when a message from another server is received
                        # Lock the message queue and append the message
                        queue_mutex.acquire()
                        try:
                            self.message_queue.append(message)
                        finally:
                            queue_mutex.release()
            finally:
                udp_socket.close()