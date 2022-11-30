#!/bin/bash

# Exit any existing Python processes
pkill -f python 2>/dev/null

# Populate hosts.txt first
rm hosts.txt
echo "localhost 10000" >> hosts.txt
echo "localhost 10001" >> hosts.txt
echo "localhost 10002" >> hosts.txt
echo "localhost 10003" >> hosts.txt
echo "localhost 10004" >> hosts.txt

# Run a driver file corresponding to each hosts.txt entry
python3 launch_server.py 0 &
python3 launch_server.py 1 &
python3 launch_server.py 2 &
python3 launch_server.py 3 &
python3 launch_server.py 4 &