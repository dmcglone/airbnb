#!/bin/bash
# Backup the airbnb database
"$SQLANY16/bin64/dbunload" -n -c "uid=dba;pwd=sql;eng=dbnb;dbf=/home/tom/src/airbnb/dbnb.db" .
