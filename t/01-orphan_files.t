#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use File::Copy;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 75;

my $node = pgNode->get_new_node('prod');
my $proc;

$node->init;
$node->start;

### Beginning of tests ###

# Find the OID of the postgres database
my ($cmdret, $dboid, $stderr) =
      $node->psql('postgres', "SELECT oid FROM pg_database WHERE datname='postgres'");

# This service can run without thresholds

# basic check without an orphan and no -w -c => Returns OK
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No orphan files found!$/m,
    ],
    [ qr/^$/ ],
    'basic check, no threshold'
);

# basic check without an orphan and thresholds at 0 => Returns OK
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '0',
                          '--critical' => '0',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No orphan files found!$/m,
    ],
    [ qr/^$/ ],
    'basic check with thresholds at 0/0'
);

# basic check without an orphan and thresholds at 0kB/0MB => Returns OK
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '0kB',
                          '--critical' => '0MB',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No orphan files found!$/m,
    ],
    [ qr/^$/ ],
    'basic check with thresholds 0kB/0MB'
);

# basic check without an orphan and thresholds at 0/0MB => Returns OK
# (mix number/size)
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '0',
                          '--critical' => '0MB',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No orphan files found!$/m,
    ],
    [ qr/^$/ ],
    'basic check with thresholds at 0/0MB'
);


# First orphan, ~120 kB
#
copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712471');

# basic check with an orphan without warning/critical - OK
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'basic check with an orphan file and no error'
);

# check threshold as size : OK
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '1MB',
                          '--critical' => '2MB',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'basic check with an orphan file below 2MB'
);

# check threshold as size : WARNING
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '20kB',
                          '--critical' => '2MB',
    ],
    1,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 1 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'basic check with an orphan file above 20 kB, warning'
);

# check threshold as size : CRITICAL
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '10kB',
                          '--critical' => '20kB',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 1 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'basic check with an orphan file above 20 kB, critical'
);

# 3 orphans, WARNING

copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712472');
copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712473');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
    ],
    1,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 3 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in warning for number of files'
);

copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712474');

# 4 orphans, CRITICAL (number of files)

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 4 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in critical for number of files'
);

# 4 orphans, only WARNING

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '1MB',
    ],
    1,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 4 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in warning, mix files/size'
);

# 4 orphans, CRITICAL (size)

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '1',
                          '--critical' => '100kB',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 4 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in warning, mix files/size'
);

# 5th file in a tablespace

mkdir $node->basedir . '/tablespace1';
$node->psql('postgres', 'CREATE TABLESPACE tbl1 LOCATION \'' . $node->basedir . '/tablespace1\';');
$node->psql('postgres', 'CREATE TABLE onetable() TABLESPACE tbl1' );
my ($cmdrettbl, $tbloid, $stderrtbl) =
      $node->psql('postgres', "SELECT oid FROM pg_tablespace WHERE spcname='tbl1'");
my $tbsp=$node->data_dir . '/pg_tblspc/' . $tbloid ;  # eg. pg_tblspc/16856/
print ("DEBUG : chemin de tbsp : $tbsp \n");
opendir my $dhandle, $tbsp || die "Can't opendir $tbsp: $!";
    my @reps = grep { /PG_[.0-9]+_\d+/ } readdir($dhandle);
    my $tbsp2 = $reps[0];
    print ("DEBUG : tbsp2 : $tbsp2 \n");
    my $tbspath = "$tbsp/$tbsp2";  # eg pg_tblspc/16856/PG_9.6_201608131
    print ("DEBUG : tbspath : $tbspath \n");
close $dhandle;
copy($node->data_dir . '/base/' . $dboid . '/1247',  $tbspath . '/' . $dboid . '/124712475');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '1',
                          '--critical' => '100kB',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 5 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'orphan in tablespace'
);

# detailed version

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
                          '--detailed',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 5 orphan files found, total size .*$/m,
      qr/^Long message  *: File .*base.*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*base.*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*base.*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*base.*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*pg_tblspc.*, size \d+.\d\d.*$/m,
    ],
    [ qr/^$/ ],
    'detailed check ending in critical for number of files'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
