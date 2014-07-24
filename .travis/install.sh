#!/bin/bash
set -e
set -x

git config --global user.email "alice+travis@gothcandy.com"
git config --global user.name "Travis: Marrow"

# We need a recent version (2.6+) or our tests just hang!
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update
sudo apt-get install mongodb-org-server

pip install --upgrade setuptools pytest
pip install tox
pip install python-coveralls
pip install pytest-cov
