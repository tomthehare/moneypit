#!/bin/bash

git pull

gpg --batch --output database/tx.db.gpg --symmetric database/tx.db