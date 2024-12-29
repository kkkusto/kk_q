#!/bin/bash

# Define the script file containing the commands
SCRIPT_FILE="move_files.sh"

# Read and execute each line from the script
while IFS= read -r line; do
    # Ignore empty lines and comments
    if [[ -n "$line" && ! "$line" =~ ^# ]]; then
        echo "Executing: $line"
        eval "$line"
    fi
done < "$SCRIPT_FILE"

echo "All commands executed successfully."
