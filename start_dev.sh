#!/bin/bash

source venv/bin/activate
FLASK_APP=server.py FLASK_ENV=prod flask run --host $(hostname -I | awk '{print $1}') --debug 
