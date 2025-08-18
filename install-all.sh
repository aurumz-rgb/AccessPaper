#!/bin/bash

# ----------------------
# Install backend (Python)
# ----------------------
echo "Installing backend (Python) packages..."
backend_packages=$(sed -n '/# Backend/,/# Frontend/p' requirements.txt | grep -v '#' | grep -v '^$')
for pkg in $backend_packages; do
    python3 -m pip install $pkg
done

# ----------------------
# Install frontend (Node.js)
# ----------------------
echo "Installing frontend (Node.js) packages..."
frontend_packages=$(sed -n '/# Frontend/,$p' requirements.txt | grep -v '#' | grep -v '^$')
for pkg in $frontend_packages; do
    npm install $pkg
done

echo "✅ All dependencies installed!"
