#!/bin/bash

git pull

gpg --batch --output database/tx.db --decrypt database/tx.db.gpg