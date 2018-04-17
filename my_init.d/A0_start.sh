#!/bin/sh
echo "Starting wd-entities..."

/root/.env/bin/pip install --upgrade pip
/root/.env/bin/pip install --upgrade git+https://github.com/fserena/wd-entities.git
/root/.env/bin/wd-entities &
