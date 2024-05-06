#!/bin/bash

source venv/bin/activate
FLASK_APP=server.py FLASK_ENV=prod flask run --host 0.0.0.0 --port 5180 --debug 
