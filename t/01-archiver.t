#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use TestLib ();
use pgNode;
use Test::More;

my $node      = pgNode->get_new_node('prod');
my $pga_data = "$TestLib::tmp_check/pga.data";
my $wal;
my @stdout;

$node->init(has_archiving => 1);

if ( $node->version >= 9.6 ) {
    $node->append_conf('postgresql.conf', "wal_level = replica");
}
elsif ( $node->version >= 9.0 ) {
    $node->append_conf('postgresql.conf', "wal_level = archive");
}

$node->start;

### Beginning of tests ###

# generate one archive
# split create table and insert to produce more data in WAL
$node->psql('template1', 'create table t (i int primary key)');
$node->psql('template1', 'insert into t select generate_series(1,10000) as i');
$wal = $node->switch_wal;


# The WAL sequence starts at 000000010000000000000000 up to v8.4, then
# 000000010000000000000001 starting from v9.0.
# Make sure we have the exact same archive sequence whatever the version so
# following tests apply no matter the version.
if ($node->version < 9.0) {
    $node->psql('template1', 'insert into t select generate_series(-1000,0) as i');
    $wal = $node->switch_wal;
}

# FIXME: there's a race condition in archiver check when it get the mtime
# of the next WAL to archive while it hasn't been created yet.
# Write a checkpoint to force the creation of the new WAL.
$node->psql('template1', 'checkpoint');

$node->wait_for_archive($wal);

$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'archiver',
                          '--username'    => getlogin,
                          '--status-file' => $pga_data,
                          '--format'      => 'human'
    ],
    0,
    [
        qr/^Service  *: POSTGRES_ARCHIVER$/m,
        qr/^Returns  *: 0 \(OK\)$/m,
        qr/^Message  *: 0 WAL files ready to archive$/m,
        qr/^Perfdata *: ready_archive=0 min=0$/m,
        qr/^Perfdata *: oldest_ready_wal=0s min=0$/m
    ],
    [ qr/^$/ ],
    'basic check without thresholds with superuser'
);

# archiver failing

$node->append_conf('postgresql.conf', "archive_command = 'false'");
$node->reload;

$node->psql('template1', 'insert into t select generate_series(10001,20000) as i');
$wal = $node->switch_wal;
# avoid same race condition
$node->psql('template1', 'checkpoint');

# FIXME: arbitrary sleep time to wait for archiver to fail at least one time
sleep 1;

# for 9.6 and before, the alert is raised on second call.
TestLib::system_or_bail('./check_pgactivity',
    '--service'     => 'archiver',
    '--username'    => getlogin,
    '--host'        => $node->host,
    '--port'        => $node->port,
    '--status-file' => $pga_data,
    '--format'      => 'human'
) if $node->version < 10;

@stdout = (
    qr/^Service      *: POSTGRES_ARCHIVER$/m,
    qr/^Returns      *: 2 \(CRITICAL\)$/m,
    qr/^Message      *: 1 WAL files ready to archive$/m,
    qr/^Message      *: archiver failing on 000000010000000000000002$/m,
    qr/^Long message *: 000000010000000000000002 not archived since (?:\ds|last check)$/m,
    qr/^Perfdata     *: ready_archive=1 min=0$/m,
    qr/^Perfdata     *: oldest_ready_wal=\ds min=0$/m
);

$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'archiver',
                          '--username'    => getlogin,
                          '--status-file' => $pga_data,
                          '--format'      => 'human'
    ],
    2,
    \@stdout,
    [ qr/^$/ ],
    'failing archiver with superuser'
);

# For PostgreSQL 10+, we now create a non-superuser monitoring role
done_testing() if ( $node->version < 10 );

$node->psql('postgres', 'create role check_pga login');
$node->psql('postgres', 'grant pg_monitor to check_pga');
$node->psql('postgres', 'grant execute on function pg_catalog.pg_stat_file(text) to check_pga');

# With pg10, the perfdata oldest_ready_wal cannot be computed, thus is not
# present in the perfdata.
@stdout = (
    qr/^Service      *: POSTGRES_ARCHIVER$/m,
    qr/^Returns      *: 2 \(CRITICAL\)$/m,
    qr/^Message      *: 1 WAL files ready to archive$/m,
    qr/^Message      *: archiver failing on 000000010000000000000002$/m,
    qr/^Long message *: 000000010000000000000002 not archived since (?:\ds|last check)$/m,
    qr/^Perfdata     *: ready_archive=1 min=0$/m,
);

# For pg11+, oldest_ready_wal is always present.
push @stdout, ( qr/^Perfdata     *: oldest_ready_wal=\ds min=0$/m )
  unless ( $node->version < 11 );

$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'archiver',
                          '--username'    => 'check_pga',
                          '--status-file' => $pga_data,
                          '--format'      => 'human'
    ],
    2,
    \@stdout,
    [ qr/^$/ ],
    'failing archiver with non-superuser'
);

### End of tests ###

$node->stop;

done_testing();
