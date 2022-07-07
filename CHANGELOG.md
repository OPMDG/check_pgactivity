Changelog
=========

2022-07-04 v2.6:

  - add: new `session_stats` service
  - add: compatibility with PostgreSQL 14
  - change: service `autovacuum` does not show `max_workers` anymore for 8.2 and below
  - change: various messages cleanup
  - fix: `last_vacuum` and `last_analyse` to reports the correct oldest maintenance
  - fix: service `check_oldest_idlexact` now use `state_change` instead of
         `xact_start` to calculate the idle time
  - fix: improve locking around the status file to avoid dead locks and status file truncation
  - fix: possible division by 0 in `table_bloat` service  
  - fix: threshold check and support interval for service `check_stat_snapshot_age`
  - fix: service `check_archiver` when a .history or .backup file is staled
  - fix: service `sequences_exhausted` now checks that sequences are not owned by a table column
  - fix: service `check_archiver` when no WAL was ever archived

2020-11-24 v2.5:

  - add: new `oldest_xmin` service
  - add: new `extensions_versions` service
  - add: new `checksum_errors` service
  - add: support for v13 and other improvements on `replication_slots`
  - add: v13 compatibility for service `wal_files`
  - add: various documentation details and examples
  - add: support service `replication_slots` on standby
  - add: accept single `b` or `o` as size unit
  - add: json and json_strict output formats
  - add: `size` and/or `delta` thresholds for `database_size` service
  - add: thresholds are now optional for service `database_size`
  - add: support for v12 and v13 `archive_folder`
  - regression: threshold `repslot` becomes `spilled` in service `replication_slots`
  - regression: in services latest_vacuum and latest_analyze: a critical alert
    is now raised on tables that were never analyzed/vacuumed or whose maintenance date was lost due to a crash
  - fix: avoid alerts for lack of maintenance on inactive db
  - fix: forbid rare cases of division by zero in `wal_files`
  - fix: do not alert on missing file in `temp_files` for v10+
  - fix: detect lack of maintenance in `last_vacuum` and `last_analyze` for never maintained tables
  - fix: backend count for v10+
  - fix: replace NaN with "U" for strict outputs
  - fix: do not count walsenders as part of `max_connections`
  - fix: broken `archiver` service with v10+
  - fix: perl warning when archiver is not active

2019-01-30 v2.4:

  - add a new `uptime` service
  - add ability to filter by application_name in longest_query and oldest_idlexact service
  - add minimal delta size to pgdump_backup service to avoid alert when backup grows small in size
  - allow psql connections without providing connection arguments:
     rely on the binary default behaviour and environment variables
  - returns CRITICAL if connection fails for service `connection`, instead of UNKNOWN
  - add documentation example for pgback in pgdump_service
  - add documentation for archive_folder
  - add information on necessary priviledges for all services
  - replication_slots service handle wal files and pg_replslots files separately
  - take account of the new BRIN summarize state of autovacuum
  - avoid warning for -dev versions in pga_version service
  - ignore startup and backup replication states in service streaming_delta
  - fix handling or file reading errors in archive_folder service
  - fix wal magic number for version 10
  - fix service stat_snapshot_age to output the correct age
  - fix archiver and replication_slots services to work properly on a standby node
  - fix archiver to raise OK on a slave
  - fix is_replay_paused for PostgreSQL 10
  - fix max_nb_wal calculation in wal_files service
  - fix uninitialized bug in hit_ratio when database do not yet have statistics
  - fix check_backend_status in order to ignore unknown status
  - fix service sequences_exhausted to take account of sequence's minvalue
  - fix sequences_exhausted to take account of sequences only in the current db
  - fix exclude option in backends_status service
  - fix archive_folder: timeline numbers are hexadecimal
  - fix head levels in man page
  - check for errors when saving status

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
