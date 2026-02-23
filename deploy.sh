#!/bin/bash
set -e

cd /home/thare/Apps/moneypit

echo "Pulling latest code..."
git pull origin main

echo "Installing dependencies..."
venv/bin/pip install -r requirements.txt

echo "Restarting service..."
sudo systemctl restart moneypit-app

echo "Done."
