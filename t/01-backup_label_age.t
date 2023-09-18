#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use TestLib ();
use IPC::Run ();
use Test::More;
use PostgresVersion;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;
my $pgversion;

$node->init;

$node->start;
$pgversion=PostgresVersion->new($node->version());
$node->stop('immediate');

### Settings according to PostgreSQL version
if ($pgversion ge "9.6") {
    $node->append_conf('postgresql.conf', 'wal_level=replica');
}
elsif ($pgversion ge "9.0") {
    $node->append_conf('postgresql.conf', 'wal_level=archive');
}

$node->append_conf('postgresql.conf', 'archive_mode=on');
$node->append_conf('postgresql.conf', 'archive_command=\'/bin/true\'');
$node->start;

### Beginning of tests ###


# basic check without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backup_label_age',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

# first check, with no backup being performed
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backup_label_age',
                          '--username' => getlogin,
                          '--format'   => 'human',
			  '--warning'  => '5s',
			  '--critical' => '10s'
    ],
    0,
    [ qr/^Service *: POSTGRES_BACKUP_LABEL_AGE$/m,
      qr/^Returns *: 0 \(OK\)$/m,
      qr/^Message *: backup_label file absent$/m,
      qr/^Perfdata *: age=0s warn=5 crit=10$/m,
     ],
    [ qr/^$/ ],
    'basic check'
);

# The following tests cases are only valid for pg<15.
# Since exclusive backups were deprecated in pg15, we ignore this part
# starting this release.
if ($pgversion ge 15) {
   # stop immediate to kill any remaining backends
   $node->stop('immediate');
   done_testing();
   exit 0;
}
push @procs, pgSession->new($node, 'postgres');

$procs[0]->query('SELECT pg_start_backup(\'check_pga\')');

sleep 1;

# first check, with no backup being performed
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backup_label_age',
                          '--username' => getlogin,
                          '--format'   => 'human',
			  '--warning'  => '5s',
			  '--critical' => '10s'
    ],
    0,
    [ qr/^Service *: POSTGRES_BACKUP_LABEL_AGE$/m,
      qr/^Returns *: 0 \(OK\)$/m,
      qr/^Message *: backup_label file present \(age: \ds\)$/m,
      qr/^Perfdata *: age=\ds warn=5 crit=10$/m,
     ],
    [ qr/^$/ ],
    'basic check with exclusive backup'
);

sleep 3;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backup_label_age',
                          '--username' => getlogin,
                          '--format'   => 'human',
			  '--warning'  => '2s',
			  '--critical' => '10s'
    ],
    1,
    [ qr/^Service *: POSTGRES_BACKUP_LABEL_AGE$/m,
      qr/^Returns *: 1 \(WARNING\)$/m,
      qr/^Message *: age: \ds$/m,
      qr/^Perfdata *: age=\ds warn=2 crit=10$/m,
     ],
    [ qr/^$/ ],
    'warn with exclusive backup'
);

$procs[0]->query('SELECT pg_stop_backup()');

# stop immediate to kill any remaining backends
$node->stop('immediate');

done_testing();

