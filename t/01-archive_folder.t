#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 35;

my $node        = pgNode->new('prod'); # declare instance named "prod"
my $archive_dir = $node->archive_dir;
my $wal;
my $time;

# create the instance and start it
$node->init(has_archiving => 1);

if ( $node->version >= 9.6 ) {
    $node->append_conf('postgresql.conf', "wal_level = replica");
}
elsif ( $node->version >= 9.0 ) {
    $node->append_conf('postgresql.conf', "wal_level = archive");
}

$node->start;

# generate three archives
# split create table and insert to produce more data in WAL
$node->psql('template1', 'create table t (i int primary key)');
$node->psql('template1', 'insert into t select generate_series(1,10000) as i');
$node->switch_wal;
$node->psql('template1', 'insert into t select generate_series(10001,20000) as i');
$node->switch_wal;
$node->psql('template1', 'insert into t select generate_series(20001,30000) as i');
$wal = $node->switch_wal;

# The WAL sequence starts at 000000010000000000000000 up to v8.4, then
# 000000010000000000000001 starting from v9.0.
# Make sure we have the exact same archive sequence whatever the version so
# following tests apply no matter the version.
if ($node->version < 9.0) {
    $node->psql('template1', 'insert into t select generate_series(30001,40000) as i');
    $wal = $node->switch_wal;
    unlink "$archive_dir/000000010000000000000000";
}

$node->wait_for_archive($wal);

### Beginning of tests ###

# simple success check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'archive_folder',
                          '--username' => getlogin,
                          '--warning'  => '5m',
                          '--critical' => '10m',
                          '--path'     => $archive_dir,
                          '--format'   => 'human'
    ],
    0,
    [
        qr/^Service  *: POSTGRES_ARCHIVES$/m,
        qr/^Returns  *: 0 \(OK\)$/m,
        qr{^Message  *: 3 WAL archived in '$archive_dir'}m,
        qr/^Perfdata *: num_archives=3$/m,
        qr/^Perfdata *: latest_archive_age=\d+s warn=300 crit=600$/m
    ],
    [ qr/^$/ ],
    'simple archives check'
);

# test hole in the sequence
rename "$archive_dir/000000010000000000000002",
       "$archive_dir/000000010000000000000002.bak";

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'archive_folder',
                          '--username' => getlogin,
                          '--warning'  => '5m',
                          '--critical' => '10m',
                          '--path'     => $archive_dir,
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_ARCHIVES$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr{^Message  *: Wrong sequence or file missing @ '000000010000000000000002'}m,
        qr/^Perfdata *: num_archives=2$/m,
        qr/^Perfdata *: latest_archive_age=\d+s warn=300 crit=600$/m
    ],
    [ qr/^$/ ],
    'error missing one archive'
);

rename "$archive_dir/000000010000000000000002.bak",
       "$archive_dir/000000010000000000000002";

# test warning archive too old
$time = time - 360;
utime $time, $time,
    "$archive_dir/000000010000000000000001",
    "$archive_dir/000000010000000000000002",
    "$archive_dir/000000010000000000000003";

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'archive_folder',
                          '--username' => getlogin,
                          '--warning'  => '5m',
                          '--critical' => '10m',
                          '--path'     => $archive_dir,
                          '--format'   => 'human'
    ],
    1,
    [
        qr/^Service  *: POSTGRES_ARCHIVES$/m,
        qr/^Returns  *: 1 \(WARNING\)$/m,
        qr{^Message  *: 3 WAL archived in '$archive_dir'}m,
        qr/^Perfdata *: num_archives=3$/m,
        qr/^Perfdata *: latest_archive_age=36\ds warn=300 crit=600$/m
    ],
    [ qr/^$/ ],
    'warn archive too old'
);

# test critical archive too old
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'archive_folder',
                          '--username' => getlogin,
                          '--warning'  => '2m',
                          '--critical' => '5m',
                          '--path'     => $archive_dir,
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_ARCHIVES$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr{^Message  *: 3 WAL archived in '$archive_dir'}m,
        qr/^Perfdata *: num_archives=3$/m,
        qr/^Perfdata *: latest_archive_age=36\ds warn=120 crit=300$/m
    ],
    [ qr/^$/ ],
    'critical archive too old'
);

# wrong sequence order
# setting 02 older than 01, the check gather archives in this mtime order:
#   02 01 03
# because of this, after checking 02 validity, it expects 03 to be the
# next file but find 01 and warn that 03 was expected.
$time = time - 400;
utime $time, $time, "$archive_dir/000000010000000000000002";
    
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'archive_folder',
                          '--username' => getlogin,
                          '--warning'  => '10m',
                          '--critical' => '15m',
                          '--path'     => $archive_dir,
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_ARCHIVES$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr{^Message  *: Wrong sequence or file missing @ '000000010000000000000003}m,
        qr/^Perfdata *: num_archives=3$/m,
        qr/^Perfdata *: latest_archive_age=36\ds warn=600 crit=900$/m
    ],
    [ qr/^$/ ],
    'wrong sequence order'
);

### End of tests ###

$node->stop;
