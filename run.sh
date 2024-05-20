#!/bin/bash
if [ -z "$1" ]; then
  echo "No argument provided. Please provide an argument."
  exit 1
fi
echo "DB File: $1"
./target/release/line-server $1
