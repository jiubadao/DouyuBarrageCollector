#!/bin/bash

if [ $# -lt 4 ]; then
    echo "program need 4 params: file headers output sql."
    echo "example: ./exp.sh bullet-20170324.db chatmsg-txt-20170324-58428.csv \"select txt from chatmsg where rid='58428'\" off"
    exit 0
fi

file=$1
output=$2
sql=$3
headers=$4

sqlite3 ${file} <<EOF
.headers ${headers}
.mode csv
.output ${output}
${sql};
EOF
