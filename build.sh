#!/bin/bash -e

docker build --pull -f Dockerfile \
        -t plugin-sonic:latest .
