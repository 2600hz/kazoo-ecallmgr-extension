#!/bin/bash

KAZOO_ROOT=~/kazoo
ECALLMGR_EXTENSION_PATH=applications/ecallmgr_extension

cd ~/

if [ ! -d $KAZOO_ROOT ]; then
    echo Cloning kazoo into $KAZOO_ROOT
    git clone https://github.com/2600hz/kazoo $KAZOO_ROOT
fi

cd $KAZOO_ROOT

echo resetting kazoo to origin/master
git fetch --prune
git rebase origin/master

if [ ! -d $ECALLMGR_EXTENSION_PATH ]; then
    echo adding submodule to $KAZOO_ROOT
    git submodule add https://github.com/2600hz/kazoo-ecallmgr-extension $ECALLMGR_EXTENSION_PATH
fi

cd $ECALLMGR_EXTENSION_PATH

echo checking out our commit $CIRCLE_BRANCH
git fetch --prune
git checkout -B $CIRCLE_BRANCH
git reset --hard $CIRCLE_SHA1

cd $KAZOO_ROOT

# wanted when committing
echo setup git config
git config user.email 'circleci@dev.null'
git config user.name 'CircleCI'

echo committing kazoo changes to avoid false positives later
git add .gitmodules $ECALLMGR_EXTENSION_PATH
git commit -m "add submodule"

echo cleaning up kazoo
make -k clean

cd $ECALLMGR_EXTENSION_PATH
