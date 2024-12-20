#!/bin/bash

# Variables
REMOTE_USER="your_username"
REMOTE_HOST="remote.server.com"
REMOTE_DIR="/path/on/remote"
LOCAL_DIR="/path/on/local"

# Example of using a here-document to run SFTP commands non-interactively
sftp -oBatchMode=yes "${REMOTE_USER}@${REMOTE_HOST}" <<EOF
cd ${REMOTE_DIR}
lcd ${LOCAL_DIR}
mget *.txt    # Get all .txt files from the remote directory
bye
EOF
