#!/usr/bin/bash
rsync -ravp --delete-during --progress ~/go/deDuplicator-Go/ rpi4:~/go/deDuplicator-Go/
rsync -ravp --delete-during --progress ~/go/deDuplicator-Go/ brain:~/go/deDuplicator-Go/
rsync -ravp --delete-during --progress ~/go/deDuplicator-Go/ pinky:~/go/deDuplicator-Go/
