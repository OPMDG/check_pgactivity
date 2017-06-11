#!/bin/bash

# $1 is required 
# Should be a major postgresql version
# i.e. : 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, 9.0, 9.1, 9.2, 9.3, 9.4 , 9.5, 9.6, 10

HOST=localhost
STATUS_FILE=/tmp/check_pgactivity.data
C=`dirname $0`/check_pgactivity 

#
# check_pgactivity returns 0 for OK, 1 for CRITICAL, 2 for WARNING
# These are considered as "correct" exit codes 
# Any higher exit code is considered as an error
#
check(){
[[ $1 -gt 2 ]] && exit $1 
}

#
# Put your all-version services below 
#

# archive_folder
#$C -s archive_folder --path /var/lib/postgresql/$1/main/pg_xlog || check $?

# backends
$C -s backends -h $HOST -w '10%' -c '30%' || check $?

# btree_bloat
$C -s btree_bloat -h $HOST -w '10%' -c '30%' || check $?

# commit_ratio
$C -s commit_ratio --status-file $STATUS_FILE -h $HOST || check $?
$C -s commit_ratio --status-file $STATUS_FILE -h $HOST -w 'rollbacks=10' -c 'rollbacks=100' || check $?

[[ "$1" == "7.4" ]] &&  exit 0

#
# Put your 8.0+ servicess below 
# 

# configuration
$C -s configuration --work_mem 1024 -h $HOST || check $?
$C -s configuration --maintenance_work_mem 1024 -h $HOST || check $? 
$C -s configuration --shared_buffers 10 -h $HOST || check $?
$C -s configuration --shared_buffers 10 -h $HOST || check $?
$C -s configuration --wal_buffers 10 -h $HOST || check $?
$C -s configuration --checkpoint_segments 10 -h $HOST || check $?
$C -s configuration --effective_cache_size 1024 -h $HOST || check $?
$C -s configuration --no_check_autovacuum  -h $HOST || check $?
$C -s configuration --no_check_fsync -h $HOST || check $?
$C -s configuration --no_check_enable -h $HOST || check $?
$C -s configuration --no_check_track_counts -h $HOST || check $?

[[ "$1" == "8.0" ]] &&  exit 0

#
# Put your 8.1+ servicess below 
#

# autovacuum:
$C -s autovacuum -h $HOST || check $?

# backup_label_age
$C -s backup_label_age -h $HOST -w 1h30m25s -c 28h30m || check $?

[[ "$1" == "8.1" ]] &&  exit 0
# Put your 8.2+ servicess below 

# last_analyze
$C -s last_analyze --status-file $STATUS_FILE -h $HOST -w 30m -c 1h30m || check $?

# last_vacuum
$C -s last_vacuum --status-file $STATUS_FILE -h $HOST -w 30m -c 1h30m || check $?

# backend_status
$C -s backends_status -h $HOST -w 'waiting=5,idle_xact=10' -c 'waiting=20,idle_xact=30' || check $?


[[ "$1" == "8.2" ]] &&  exit 0

#
# Put your 8.3+ servicess below 
#

# bgwriter
$C -s bgwriter --status-file $STATUS_FILE -h $HOST || check $?
# /!\ Illegal division by zero at /tmp/check_pgactivity line 2271.
#$C -s bgwriter --status-file $STATUS_FILE -h $HOST -w 15% -c 33% || check $?

