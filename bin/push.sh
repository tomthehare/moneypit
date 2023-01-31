#!/bin/bash

rm database/tx.db.gpg

gpg --batch --output database/tx.db.gpg --symmetric database/tx.db

git add . && git commit -m "Adding new database"

git push