#!/bin/sh

echo "Starting VCI Service...."
python vci.py >> logs/vci.logs 2>&1 &



