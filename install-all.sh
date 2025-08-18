#!/bin/bash
# install-all.sh
# Installs dependencies for both backend and frontend

set -e  # exit on first error

echo "Installing backend dependencies..."
cd backend
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
cd ..

echo "Installing frontend dependencies..."
cd frontend
npm install
cd ..

echo "All dependencies installed successfully!"
