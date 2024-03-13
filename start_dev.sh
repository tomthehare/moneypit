#!/bin/bash


FLASK_APP=server.py FLASK_ENV=prod flask run --host $(hostname -I | awk '{print $1}') --debug 
