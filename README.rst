.. highlight:: perl


****************
check_pgactivity
****************


check_pgactivity - PostgreSQL plugins for Nagios

SYNOPSIS
========



.. code-block:: perl

   check_pgactivity {-w|--warning THRESHOLD} {-c|--critical THRESHOLD} [-s|--service SERVICE ] [-h|--host HOST] [-U|--username ROLE] [-p|--port PORT] [-d|--dbname DATABASE] [-S|--dbservice SERVICE_NAME] [-P|--psql PATH] [--debug] [--status-file FILE] [--path PATH] [-t|--timemout TIMEOUT]
   check_pgactivity [--list]
   check_pgactivity [--help]



DESCRIPTION
===========


check_pgactivity is dedicated to monitoring PostgreSQL cluster from Nagios. It
offers many different services and returns various usefull perfdata for
metrology.


\ **-s**\ , \ **--service**\  SERVICE
 
 The nagios service to run. See section SERVICES for a description of available
 services or option \ ``--list``\  for a short service and description list.
 


\ **-h**\ , \ **--host**\  HOST
 
 Database server host or socket directory (default: "localhost").
 


\ **-U**\ , \ **--username**\  ROLE
 
 Database user name (default: "postgres").
 


\ **-p**\ , \ **--port**\  PORT
 
 Database server port (default: "5432").
 


\ **-d**\ , \ **--dbname**\  DATABASE
 
 Database name to connect to (default: "postgres").
 


\ **-S**\ , \ **--dbservice**\  SERVICE_NAME
 
 The service name to use from pg_service.conf to connect.
 


\ **-w**\ , \ **--warning**\  THRESHOLD
 
 The warning threshold.
 


\ **-c**\ , \ **--critical**\  THRESHOLD
 
 The critical threshold.
 


\ **--tmpdir**\  DIRECTORY
 
 Path to a directory where the script can create temporary files. The
 script rely on system default temporary directory if possible.
 


\ **-P**\ , \ **--psql**\  FILE
 
 Path to the \ ``psql``\  executable (default: "psql").
 


\ **--status-file**\  PATH
 
 PATH to the file where service status information will be kept between two
 call. Default to check_pgactivity.data in the same directory of the script.
 


\ **-t**\ , \ **--timeout**\  TIMEOUT
 
 Timeout to use (default: "30s"). It can be specified as raw (in second) or as
 an interval. This timeout will be used as statement_timeout for psql and URL
 timeout for minor_version service.
 


\ **--list**\ 
 
 List available services.
 


\ **-V**\ , \ **--version**\ 
 
 Print version and exit.
 


\ **--debug**\ 
 
 Print some debug messages.
 


\ **-?**\ , \ **--help**\ 
 
 Show this help page.
 



THRESHOLDS
==========


THRESHOLD given as warning and critical values can either be a raw number, a
percentage, an interval or a size. Each available service supports one or more
form (eg. a size and a percentage).


\ **Percentage**\ 
 
 If threshold is a percentage, the value should finish with a '%' without space
 with the actual value. Eg.: 95%.
 


\ **Interval**\ 
 
 If THRESHOLD is an interval, the following units are accepted (not case
 sensitive): s (second), m (minute), h (hour), d (day). You can use more than
 one unit per give value. If not set, the last unit is in seconds. Eg.: "1h 55m
 6" = "1h55m6s".
 


\ **Size**\  If THRESHOLD is a size, the following units are accepted (not case sensitive):
b (Byte), k (KB), m (MB), g (GB), t (TB), p (PB), e (EB) or Z (ZB). Only
integers are accepted. Eg. \ ``1.5MB``\  will be refused, use \ ``1500kB``\ .
 
 The factor between units is 1024 Bytes. Eg. \ ``1g = 1G = 1024\*1024\*1024.``\ 
 



CONNEXIONS
==========


check_pgactivity allows two different of connexion specification: by service or
by specifying values for host, user, port and database. Moreover, some services
can run on multiple host or needs to connect to multiple ones.

You must specify one of the parameters bellow if the service need to connect
to your PostgreSQL instance. In other words, check_pgactivity will NOT look for
the libpq environment variables.

The rules with connexions parameters are:


\ **Parameter**\  \ ``--dbservice SERVICE_NAME``\ 
 
 Define a new host using the given service. Multiple hosts can be defined by
 giving multiple services seperated by a comma. Eg.
 
 
 .. code-block:: perl
 
    --dbservice service1,service2
 
 


\ **Parameters**\  \ ``--host HOST``\ , \ ``--port PORT``\ , \ ``--user ROLE``\  or \ ``--dbname DATABASE``\ 
 
 One of these parameters is enough to defines a new host. If some other
 parameters are missing, default values are used.
 
 If multiple values are given, define as many host as maximum given values.
 
 Values are associated by position. Eg.:
 
 
 .. code-block:: perl
 
    --host h1,h2 --port 5432,5433
 
 
 Means "host=h1 port=5432" and "host=h2 port=5433".
 
 If the number of values is different between parameters, any host that miss a
 parameter will use the first given value for this parameter. Eg.:
 
 
 .. code-block:: perl
 
    --host h1,h2 --port 5433
 
 
 Means: "host=h1 port=5433" and "host=h2 port=5433".
 


\ **Services are define first**\ 
 
 As instance, giving:
 
 
 .. code-block:: perl
 
    --dbservice s1 --host h1 --port 5433
 
 
 Means "service=s1" and "host=h1 port=5433" in this order. If the service
 supports only one host, the second is ignored
 


\ **Mutual exclusion between both methods**\ 
 
 You can not overwrite services connexions variables with parameters \ ``--host HOST``\ , \ ``--port PORT``\ , \ ``--user ROLE``\  or \ ``--dbname DATABASE``\ 
 



SERVICES
========


Here is the list, descriptions and parameters of available services.


\ **autovacuum**\  (8.1+)
 
 Check the autovacuum activity on the cluster.
 
 Perfdata contains the age of oldest autovacuum and the number of worker by kind
 (VACUUM, VACUUM ANALYZE, ANALYZE, VACUUM FREEZE).
 
 Thresholds, if any, are ignored.
 


\ **backends**\  (all)
 
 Check the total number of connexions on the cluster.
 
 Perfdata contains the number of connexions per database.
 
 Critical and Warning thresholds accept either a raw number or a percentage (eg.
 80%). When a threshold is in percent, it is compared to the cluster parameter
 \ ``max_connections``\ .
 


\ **backends_status**\  (8.2+)
 
 Check and report the status of the backends. Depending to your PostgreSQL
 version, status are: idle, idle in transaction, idle in transaction (aborted)
 (>=9.0 only), fastpath function call, active, waiting for lock, undefined,
 disabled and insufficient privilege. The last one appears when you are not
 allowed to see the status of other connexions.
 
 This service supports argument \ ``--exclude REGEX``\  to exclude queries
 matching the given regexp from the check. You can give multiple
 \ ``--exclude REGEX``\ .
 
 Critical and Warning thresholds are optionnal. They accept a list of
 'status_label=value' separated by comma. Available labels are idle, idle_xact,
 aborted_xact, fastpath, active and waiting. Values are raw numbers and empty
 list are forbidden. Here is an example:
 
 
 .. code-block:: perl
 
      -w 'waiting=5,idle_xact=10' -c 'waiting=20,idle_xact=30'
 
 
 Perfdata contains the number of backends for each status and the oldest one for
 each of them for 8.2+.
 
 Note that the number of backend reported as Nagios message \ **includes**\ 
 excluded backend.
 


\ **database_size**\  (8.1+)
 
 Check the variation of database sizes.
 
 This service uses the status file (see \ ``--status-file``\  parameter).
 
 Perfdata contains the size difference for each database since last call.
 
 Critical and Warning thresholds accept either a raw number, a percentage or a
 size (eg. 2.5G).
 


\ **wal_files**\  (8.1+)
 
 Check the number of wal files.
 
 Perfdata returns the total number of wal files, current number of written wal,
 the current number of recycled wal and the rate of wal written to disk since
 last execution on master clusters.
 
 Critical and Warning thresholds accept either a raw number of file or a
 percentage. In case of percentage, the limit is computed based on:
 
 
 .. code-block:: perl
 
    100% = 1 + checkpoint_segments * (2 + checkpoint_completion_target)
 
 
 For PostgreSQL 8.1 and 8.2:
 
 
 .. code-block:: perl
 
    100% = 1 + checkpoint_segments * 2
 
 
 If \ ``wal_keep_segments``\  is set for 9.0 and above, the limit is the greatest
 between the following formulas :
 
 
 .. code-block:: perl
 
    100% = 1 + checkpoint_segments * (2 + checkpoint_completion_target)
    100% = 1 + wal_keep_segments + 2 * checkpoint_segments
 
 


\ **ready_archives**\  (8.1+)
 
 Check the number of wal files ready to archive.
 
 Perfdata returns the number of wal files waiting to be archived.
 
 Critical and Warning thresholds only accept a raw number of file
 


\ **last_analyze**\  (8.2+)
 
 Check on each databases that the oldest analyze (from autovacuum or not) is not
 older than the given threshold.
 
 This service uses the status file (see \ ``--status-file``\  parameter) with
 PostgreSQL 9.1+.
 
 Perfdata returns oldest analyze per database in seconds. With PostgreSQL
 9.1+, it returns the number of [auto]analyses per database since last
 call as well.
 
 Critical and Warning thresholds only accept an interval (eg. 1h30m25s)
 and apply on the oldest analyse maintenance.
 


\ **last_vacuum**\  (8.2+)
 
 Check on each databases that the oldest vacuum (from autovacuum or not) is not
 older than the given threshold.
 
 This service uses the status file (see \ ``--status-file``\  parameter) with
 PostgreSQL 9.1+.
 
 Perfdata returns oldest vacuum per database in seconds. With PostgreSQL
 9.1+, it returns the number of [auto]vacuums per database since last
 call as well.
 
 Critical and Warning thresholds only accept an interval (eg. 1h30m25s)
 and apply on the oldest vacuum.
 


\ **locks**\  (all)
 
 Check the number of locks on the hosts.
 
 Perfdata returns the number of lock for kind of lock.
 
 Critical and Warning thresholds accept either a raw number of lock or a
 percentage. In case of percentage, it is computed against the following limits
 for 7.4 to 8.1:
 
 
 .. code-block:: perl
 
    max_locks_per_transaction * max_connections
 
 
 for 8.2+:
 
 
 .. code-block:: perl
 
    max_locks_per_transaction * (max_connections + max_prepared_transactions)
 
 
 for 9.1+, regarding lockmode :
 
 
 .. code-block:: perl
 
    max_locks_per_transaction * (max_connections + max_prepared_transactions)
  or max_pred_locks_per_transaction * (max_connections + max_prepared_transactions)
 
 


\ **bgwriter**\  (8.3+)
 
 Check the percentage of pages written by backends since last check.
 
 This service uses the status file (see \ ``--status-file``\  parameter).
 
 Perfdata contains differentials of pg_stat_bgwriter counters since last
 call.
 
 Critical and Warning thresholds are optionnal. If set, they only accept a
 percentage.
 


\ **archive_folder**\ 
 
 Check if all archived WAL exist between the oldest and the latest WAL in the
 archive folder and make sure they are 16MB. The given folder must have archived
 files from ONE cluster. The version of PostgreSQL that created the archives is
 only checked on the last one for speed consideration.
 
 This service requires the argument \ ``--path``\  on the command line to specify the
 archive folder path to check.
 
 Optionnal argument \ ``--ignore-wal-size``\  allows to NOT check the WAL size. Usefull if your
 archived WALs are compressed. Default behaviour is to check the WALs size.
 
 Optionnal argument \ ``--suffix``\  allows you define the prefix of your archived
 WALs. Usefull if they are compressed with an extension (eg. .gz, .bz2, ...).
 Default is no suffix.
 
 Perfdata contains the number of WAL archived and the age of the latest one.
 
 Critical and Warning define the max age of the latest archived WAL as an
 interval (eg. 5m or 300s ).
 


\ **minor_version**\  (all)
 
 Check if the cluster is running the latest minor version of PostgreSQL.
 
 Latest version of PostgreSQL can be fetch from PostgreSQL official
 website if check_pgactivity can access it or given as a parameter.
 
 Without \ ``--critical``\  or \ ``--warning``\  parameters, this service attempt
 to fetch the latest version online. You can optionnaly set the path to
 your prefered program using the parameter \ ``--path``\  (eg.
 \ ``--path '/usr/bin/wget'``\ ). Supported programs are: GET, wget, curl,
 fetch, lynx, links, links2.
 
 The online version, rise a critical alert if the minor version is not
 the latest.
 
 If you do not want to (or can not) query the PostgreSQL website, you
 must provide the expected version using either \ ``--warning``\  OR
 \ ``--critical``\ . The given format must be one or more MINOR version
 seperated by anything but a '.'. Eg. the following parameters are all
 equivalent:
 
 
 .. code-block:: perl
 
    --critical "9.3.2 9.2.6 9.1.11 9.0.15 8.4.19"
    --critical "9.3.2, 9.2.6, 9.1.11, 9.0.15, 8.4.19"
    --critical 9.3.2,9.2.6,9.1.11,9.0.15,8.4.19
    --critical 9.3.2/9.2.6/9.1.11/9.0.15/8.4.19
 
 
 Anything other than a 3 numbered dot-separated version will be ignored.
 if the running PostgreSQL major version is not found, the service rises an
 unknown status.
 
 Using the offline version rises either a critical or a warning depending
 on which one has been set.
 
 Perfdata returns the numerical version of PostgreSQL.
 


\ **hot_standby_delta**\  (9.0)
 
 Check the data delta between a cluster and its Hot standbys.
 
 You must give two or more hosts' connection parameters.
 
 Perfdata returns the data delta in bytes between the master and all given Hot
 standbys.
 
 Critical and Warning thresholds can takes one or two values separated by a
 comma. If only one value given, it applies on both received and replayed data.
 If two values given, the first one applies on received data, the second one on
 replayed ones. These threshold only accept a size (eg. 2.5G).
 
 This service rise a critical if it doesn't find exactly ONE cluster production
 (ie. critical when 0 or 2 and more masters).
 


\ **streaming_delta**\  (9.1+)
 
 Check the data delta between a cluster and its standbys in streaming replication.
 
 Optionnal argument \ ``--slave``\  allows to specify some slaves that MUST be
 connected. This argument can be used as many time as you need to check multiple
 slaves connections, or you can specify multiple slaves connections at one time,
 using comma separated values. Both methods can be used in a single call. The
 given value must be of the form "APPLICATION_NAME IP".
 Any of those two following examples will check for the presence of two slaves:
 
 
 .. code-block:: perl
 
    --slave 'slave1 192.168.1.11' --slave 'slave2 192.168.1.12'
    --slave 'slave1 192.168.1.11','slave2 192.168.1.12'
 
 
 Perfdata returns the data delta in bytes between the master and all standbys
 found and the number of slave connected.
 
 Critical and Warning thresholds can takes one or two values separated by a
 comma. If only one value given, it applies on both flushed and replayed data.
 If two values given, the first one applies on flushed data, the second one on
 replayed ones. These thresholds only accept a size (eg. 2.5G).
 


\ **hit_ratio**\  (all)
 
 Check the cache hit ratio on the cluster.
 
 Perfdata contains the hit ratio per database. Template databases and
 databases that does not allow connections wont be checked, nor the
 databases which has never been accessed.
 
 Critical and Warning thresholds are optionnal. They only accept a percentage.
 


\ **backup_label_age**\  (8.1+)
 
 Check the age of the backup label file.
 
 Perfdata returns the age of the backup_label file, -1 if not present.
 
 Critical and Warning thresholds only accept an interval (eg. 1h30m25s).
 


\ **oldest_2pc**\  (8.1+)
 
 Check the oldest two phase commit transaction (aka. prepared transaction) in
 the cluster.
 
 Perfdata contains the max/avg age time and the number of prepared
 transaction per databases.
 
 Critical and Warning thresholds only accept an interval.
 


\ **oldest_xact**\  (8.3+)
 
 Check the oldest idle transaction.
 
 Perfdata contains the max/avg age time and the number of idle
 transaction per databases.
 
 Critical and Warning thresholds only accept an interval.
 


\ **longest_query**\  (all)
 
 Check the longest running query in the cluster. This service supports argument
 \ ``--exclude REGEX``\  to exclude queries matching the given regexp from the check.
 You can give multiple \ ``--exclude REGEX``\ .
 
 Perfdata contains the max/avg/min running time and the number of query per
 databases.
 
 Critical and Warning thresholds only accept an interval.
 


\ **connection**\  (all)
 
 Perform a simple connection test.
 
 No perfdata is returned.
 
 This service ignore critical and warning arguments.
 


\ **custom_query**\  (all)
 
 Perform the given user query.
 
 The query is specified with the \ ``--query parameter``\ . The first column will be
 used to perform the test for the status, if warning and critical are provided.
 
 The warning and critical arguments are optionnal. They can be treated as integer
 (default), size or time depending to the \ ``--type``\  argument. Warning and critical
 will be raised if they are greater than the first column, or less if the
 \ ``--reverse``\  option is used.
 
 All others columns will be used to generate the perfdata. The query has to
 display them in the perfdata format, with unit if needed (eg. "size=35B").
 If a field contains multiple values, they have to be space separated.
 


\ **configuration**\  (8.0+)
 
 Check the most important settings.
 
 Warning and critical tresholds are ignored.
 
 Specific parameters are :
 \ ``--work_mem``\ , \ ``--maintenance_work_mem``\ , \ ``--shared_buffers``\ ,\ ``-- wal_buffers``\ ,
 \ ``--checkpoint_segments``\ , \ ``--effective_cache_size``\ , \ ``--no_check_autovacuum``\ ,
 \ ``--no_check_fsync``\ , \ ``--no_check_enable``\ , \ ``--no_check_track_counts``\ .
 


\ **max_freeze_age**\  (all)
 
 Checks oldest database in transaction age.
 
 Critical and Warning thresholds are optionnal. They accept either a raw number
 or percentage for PostgreSQL 8.2 and more. If percentage is given, the
 thresholds are computed based on the "autovacuum_freeze_max_age" parameter.
 100% means some table(s) reached the maximum age and will trigger an autovacuum
 freeze. Percentage thresholds should therefore be greater than 100%.
 
 Even with no threshold, this service will raise a critical alert if one database
 has a negative age.
 
 Perfdatas return the age of each database.
 


\ **is_master**\  (all)
 
 Checks if the cluster accepts read and/or write queries. This state is reported
 as "in production" by pg_controldata.
 
 This service ignores critical and warning arguments.
 
 No perfdata are returned.
 


\ **is_hot_standby**\  (9.0+)
 
 Checks if the cluster is in recovery and accepts read only queries.
 
 This service ignores critical and warning arguments.
 
 No perfdata are returned.
 


\ **pga_version**\ 
 
 Checks if this script is running the given version of check_pgactivity.
 You must provide the expected version using either \ ``--warning``\  OR
 \ ``--critical``\ .
 
 No perfdata are returned.
 


\ **is_replay_paused**\  (9.1+)
 
 Checks if the replication is paused. The service will return UNKNOWN if called
 on a master server.
 
 Thresholds are optionnals. They must be specified as interval. OK will always be returned if
 the standby is not paused, even if replication delta time hits the thresholds.
 
 Critical or warning are raised if last reported replayed timestamp is greater than given
 threshold AND some data received from the master are not applied yet. OK will always be
 returned if the standby is paused, already replayed everything from master and until some
 writes activity happen on the master.
 
 Perfdata returned are :
   \* paused status (0 no, 1 yes, NaN if master)
   \* lag time (in second)
   \* data delta with master (0 no, 1 yes)
 


\ **btree_bloat**\ 
 
 Estimate bloat on B-tree indexes.
 
 Warning and critical thresholds accept a comma separated list of either
 raw number(for a size), size (eg. 125M) or percentage. The thresholds stand for
 bloat size, not object size. If a percentage is given, the threshold will apply
 to the bloat size compared to the total index size. If multiple threshold
 values are passed, check_pgactivity will choose the one resulting in the biggest
 bloat size allowed.
 
 This service supports argument \ ``--exclude REGEX``\  to exclude relation matching
 the given regexp from the check. The regexps apply on
 "schema_name.relation_name". You can give multiple \ ``--exclude REGEX``\ .
 
 Warning, with a non-superuser role, only index on table the role is granted to
 access are checked!
 
 Perfdata will give the number of indexes concerned by warning and critical
 threshold per database.
 


\ **table_bloat**\ 
 
 Estimate bloat on tables.
 
 Warning and critical thresholds accept a comma separated list of either
 raw number(for a size), size (eg. 125M) or percentage. The thresholds stand for
 bloat size, not object size. If a percentage is given, the threshold will apply
 to the bloat size compared to the table+toast size. If multiple threshold
 values are passed, check_pgactivity will choose the one resulting in the biggest
 bloat size allowed.
 
 This service supports argument \ ``--exclude REGEX``\  to exclude relation matching
 the given regexp from the check. The regexps apply on
 "schema_name.relation_name". You can give multiple \ ``--exclude REGEX``\ .
 
 Warning, with a non-superuser role, only index on table the role is granted to
 access are checked!
 
 Perfdata will give the number of tables concerned by warning and critical
 threshold per database.
 



EXAMPLES
========



\ ``check_pgactivity -h localhost -p 5492 -s last_vacuum -w 30m -c 1h30m``\ 
 
 Execute service "last_vacuum" on host "host=localhost port=5432".
 


\ ``check_pgactivity --debug --dbservice pg92,pg92s --service streaming_delta -w 60 -c 90``\ 
 
 Execute service "streaming_delta" between hosts "service=pg92" and "service=pg92s".
 


\ ``check_pgactivity --debug --dbservice pg92 -h slave -U supervisor --service streaming_delta -w 60 -c 90``\ 
 
 Execute service "streaming_delta" between hosts "service=pg92" and "host=slave user=supervisor".
 



LICENSING
=========


This program is open source, licensed under the PostgreSQL license.
For license terms, see the LICENSE provided with the sources.


AUTHORS
=======


Author: Open PostgreSQL Monitoring Development Group
Copyright: (C) 2012-2014 Open PostgreSQL Development Group


