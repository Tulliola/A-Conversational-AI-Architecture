#!/bin/bash

# This script gets a conversation_id as parameter and reconstructs the entire logging flow in function of it.

if [ $# -ne 1 ]; then
    echo "[ERROR] You must pass exactly one parameter."
    exit 1
fi

cat ./logging/*/*.log | sort | grep ".*[CONV_ID: $1].*"