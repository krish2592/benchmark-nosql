#!/bin/bash

set -ex 

cd "$(dirname ${BASH_SOURCE[0]})/../../../data/ShopingCart"

if [[ ! -f project_files.zip ]]; then
  curl 'https://www.comp.nus.edu.sg/~cs4224/project_files.zip' -L -o project_files.zip
else
  echo 'project_files.zip already exists'
fi

if [[ ! -d project_files ]]; then
  unzip project_files.zip -d .
else
  echo 'project_files.zip already extracted'
fi



# mkdir tests/ShopingCart