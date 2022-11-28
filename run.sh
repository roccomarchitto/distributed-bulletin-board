#!/bin/bash

# Exit any existing Python processes
pkill -f python 2>/dev/null

# Populate hosts.txt first
rm hosts.txt
echo "localhost 10000" >> hosts.txt
echo "localhost 10001" >> hosts.txt

# Run a driver file corresponding to each hosts.txt entry
python3 launch_server.py 0 &
python3 launch_server.py 1 &
