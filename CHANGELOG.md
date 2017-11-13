Changelog
=========

2017-11-13 v2.3:
  - add complete support for PostgreSQL 10, including non-privileged monitoring
    features
  - add some documentation to help new contributors
  - add ability to use time units for thresholds in service backend_status
  - fix a long-standing bug in service backends_status
  - fix sequences_exhausted to work with sequences attached to unusual types
  - fix fetching method for service minor_version

2017-04-28 v2.2:
  - add support for PostgreSQL 9.6
  - add early-support for PostgreSQL 10
  - add service sequences_exhausted to monitor sequence usage
  - add service stat_snapshot_age to detect a stuck stats collector process
  - add service wal_receiver to monitor replication on standby's end
  - add service pgdata_permission to monitor rights and ownership of the PGDATA
  - add support for "pending restart" parameters from PostgreSQL 9.5+ in check_settings
  - add timeline id in perfdata output from wal_files
  - fix wal_files, archiver, check_is_replay_paused, check_hot_standby_delta, check_streaming_delta and check_replication_slots for PostgreSQL 10
  - fix archive_folder to handle compressed archived WAL properly
  - fix backends_status for PostgreSQL 9.6
  - improve and rename "ready_archives" to "archiver"
  - warn when no rows are returned in custom_query service
  - make thresholds optional in service hot_standby_delta
  - make thresholds optional in service streaming_delta
  - remove useless thresholds in backends/maximum_connections perfdata
  - add warn/crit threshold to steaming_delta perfdatas
  - use parameter server_version_num to detect PostgreSQL version
  - fix a race condition in is_storable to handle concurrent executions
    correctly
  - fix a bug in service locks that occurs with PostgreSQL 8.2
  - fix rounding in hit_ratio
  - fix perl warning when some ENV variables are not defined
  - fix bug in "human" output format
  - fix version check for all hosts for service hot_standby_delta
  - fix bug in pg_dump_backups related to age of global files
  - fix documentation about default db connection

2016-08-29 2.0:
  - support various output format
  - add output format "nagios_strict"
  - add output format "debug"
  - add output format "binary"
  - add output format "human"
  - force UTF8 encoding
  - fix a bug where pod2usage couldn't find the original script
  - fix wal size computation for 9.3+ (255 -vs- 256 seg of 16MB)
  - fix perl warning with pg_dump_backup related to unknown database
  - fix buffers_backend unit in check_bgwriter
  - do not connect ot the cluster if using --dbinclude for service pg_dump_backup
  - add argument --dump-status-file, useful for debugging
  - add service "table_unlogged"
  - add basic support to timeline cross in service archive_folder
  - add service "settings"
  - add service "invalid_indexes"

2016-01-28 1.25:
  - add service pg_dump_backup
  - change units of service bgwriter (github issue #29)
  - support PostgreSQL 9.5
  - fix backends service to remove autovacuum from the connection count (github issue #14)
  - fix backends service to add walsenders to the connection count (github issue #14)
  - fix a harmless perl warning
  - fix wal_size service to support 9.5+
  - fix corruption on status file on concurrent access
  - fix bad estimation in btree bloat query with mostly NULL columns

2015-09-28 1.24:
  - improve message for streaming_delta and hot_standby_delta services
  - add replication_slot service
  - enhance table_bloat queries
  - enhance btree_bloat queries
  - add -l option, aliased for --list
  - backends service has a new maximum_connections perfdata
  - backends service now consider the maximum connections as max_connections - superuser_reserved_connections
  - improve checks for hot_standby_delta service
  - fix check_pgactivity to run with Perl 5.10.0
  - add commit_ratio service
  - various documentations improvements

2015-02-05 1.23:
  - better handling of errors related to status file
  - support fillfactor in btree_bloat and table_bloat services
  - compute hit_ratio since last run, which mean the value is now really precise
  - add --dbinclude and --dbexclude arguments
  - fix # of idle in xact in odlest_idlexact service
  - check the temp file creation for queries succeed
  - accept non-decimal only thresholds in pga_version (making it works with beta versions)
  - fix compatibility issue with perl 5.8
  - add perl version to pga_version and --version

2014-12-30 1.22:
  - fix pga_version service to accept non-decimal only versions
  - fix temp_files service bug, leading to "ERROR:   set-valued function called in context that cannot accept a set" errors

2014-12-24 1.21:
  - fix temp_files service

2014-12-24 1.20:
  - add RPM specfile
  - add temp_files service
  - fix bug #13 (illegal division by 0)
  - fux bad regexp in autovacuum service
  - fix wrong curl command line options

2014-12-03 1.19:
  - fix oldest_idlexact service
  - documentation improvements
  - fix last_vacuum/analyze last exec time computation

2014-11-03 1.18:
  - fix issue in locks service with PG 9.0-

2014-10-29 1.17:
  - improve btree index bloat accuracy

2014-10-23 1.16:
  - fix btree_bloat service to support index on expression
  - various documentation improvements
  - fix SIReadLocks output in locks service
  - fix missing database in oldest_idlexact service
  - add warning & critical values in hot_standby service perfdata
  - add predicate locks support in locks service
  - enhance backup_label_service on PG 9.3+
  - fix streaming_delta service when called on a standby

2014-09-19 1.15:
  - do not compute wal_rate on standby in wal_files service

2014-09-09 1.14:
  - return critial if negative age in max_freeze_age service
  - add wal_rate perfdata to wal_files service
  - general enhancement in documentation
  - add perfdata in streaming_delta service
  - fix autovacuum service on PG 8.3+

2014-09-05 1.13:
  - add autovacuum service
  - fix wrong behavior when using 0 in a time unit

2014-08-07 1.12:
  - add wal_keep_segments in wal_files service perfdata
  - fix the expected number of WAL in wal_files service
  - fix issue in table_bloat service leading to precess to indexes
  - remove some useless perfdata from backends_status service

2014-08-05 1.11:
  - handle disabled and insufficient privilege status in backends_status service
  - improve accuracy of table_bloat service

2014-07-31 1.10:
  - split bloat service into more accurate btree_bloat and table_bloat service
  - fix issue if the server name contains a "="
  - fix Perl warning in hot_standby_delta service
