#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <json_file> <pprof_output_file>"
    exit 1
fi

# Extract the hex-encoded string from the JSON file
hex_string=$(jq -r '.pprof' $1)

# Remove the escaped backslashes
hex_string=$(echo $hex_string | sed 's/\\x//g')

# Convert the hex string to binary and save to a file
echo $hex_string | xxd -r -p > $2
