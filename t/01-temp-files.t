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
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 143;

my $node = pgNode->get_new_node('prod');
my $proc;
my $t0; # use to avoid two check_pga calls within the same second.
        # See comment before first call of usleep.

$node->init;
$node->append_conf('postgresql.conf', 'work_mem=64kB');
$node->start;

### Beginning of tests ###

# This service can run without thresholds

# Tests for PostreSQL 8.1 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 8.0 and before", 3
        if $node->version >= 8.1;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service temp_files is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

SKIP: {
    skip "incompatible tests with PostgreSQL < 8.1", 34 if $node->version < 8.1;

    # basic check => Returns OK
    $t0 = [gettimeofday];
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: [2-4] tablespace\(s\)\/database\(s\) checked$/m,
        ],
        [ qr/^$/ ],
        'basic check'
    );

    $t0 = [gettimeofday];

    # unit test based on the file count => Returns OK
    # Generate one temp file of 140000 bytes
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    # The added sleep ensures that two tests are not executed within the same
    # seconds.
    # The time difference is used by check_pga to compute the Fpm and Bpm
    # perfstats. As check_pga doesn work with sub-second time, if it is called
    # twice in the same second, it ends with division by zero error.
    # In consequence, this usleep recipe is repeated between each call of
    # check_pga.
    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '3',
                              '--critical' => '4'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: [2-4] tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=3 crit=4$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=3 crit=4$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B$/m,
        ],
        [ qr/^$/ ],
        'test file count OK'
    );

    $t0 = [gettimeofday];

    # unit test based on the file count => Returns WARN
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '0',
                              '--critical' => '3'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=0 crit=3$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=0 crit=3$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B$/m,
        ],
        [ qr/^$/ ],
        'test file count WARN'
    );

    $t0 = [gettimeofday];

    # unit test based on the file count => Returns CRIT
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '0',
                              '--critical' => '0'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=0 crit=0$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=0 crit=0$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B$/m,
        ],
        [ qr/^$/ ],
        'test file count CRIT'
    );

    $t0 = [gettimeofday];

    # unit test based on the file size => Returns OK
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '200kB',
                              '--critical' => '300kB'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: [2-4] tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=200kB crit=300kB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=200kB crit=300kB$/m,
        ],
        [ qr/^$/ ],
        'test file size OK'
    );

    $t0 = [gettimeofday];

    # unit test based on the file size => Returns WARN
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '100kB',
                              '--critical' => '200kB'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=100kB crit=200kB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=100kB crit=200kB$/m,
        ],
        [ qr/^$/ ],
        'test file size WARN'
    );

    $t0 = [gettimeofday];

    # unit test based on the file size => Returns CRIT
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '50kB',
                              '--critical' => '100kB'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=50kB crit=100kB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=50kB crit=100kB$/m,
        ],
        [ qr/^$/ ],
        'test file count CRIT'
    );

    $t0 = [gettimeofday];

    # unit test based on the file size and count => Returns OK
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '3,49254kB',
                              '--critical' => '4,65638kB'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: [2-4] tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=3 crit=4$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=48.099609375MB crit=64.099609375MB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=3 crit=4$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=48.099609375MB crit=64.099609375MB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count OK '
    );

    $t0 = [gettimeofday];

    # unit test based on the file size and count => Returns WARN
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '1,100kB',
                              '--critical' => '3,200kB'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=1 crit=3$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=100kB crit=200kB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=1 crit=3$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=100kB crit=200kB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count WARN'
    );

    $t0 = [gettimeofday];

    # unit test based on the file size and count => Returns CRIT
    $node->psql('postgres', 'SELECT count(*) FROM generate_series(1,10000);');

    usleep(100_000) while tv_interval($t0) < 1.01;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '0,50kB',
                              '--critical' => '1,100kB'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files warn=0 crit=1$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B warn=50kB crit=100kB$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files warn=0 crit=1$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B warn=50kB crit=100kB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count CRIT'
    );

    $t0 = [gettimeofday];

    # unit test with a tablespace => Returns OK
    # * are the tempfiles located in the correct directory ?
    # * do we only account for temp files ? (cf issue #351)
    mkdir $node->basedir . '/tablespace1';
    $node->psql('postgres', 'CREATE TABLESPACE myts1 LOCATION \'' . $node->basedir . '/tablespace1\';');
    my $tbsp2 = $node->basedir . '/tablespace2';
    mkdir $tbsp2;
    $node->psql('postgres', qq{CREATE TABLESPACE myts2 LOCATION '$tbsp2';});

    # Create some tables in the tablespaces to make sure their files are not
    # reported as temp files (gh #351).
    $node->psql('postgres', 'CREATE TABLE matable0(x text);');
    $node->psql('postgres', 'CREATE TABLE matable1(x text) TABLESPACE myts1;');
    $node->psql('postgres', 'CREATE TABLE matable2(x text) TABLESPACE myts2;');
    $node->psql('postgres', 'VACUUM;');

    # Create one fake temp file in tablespace myts2 and make sure only one file
    # is reported be check_pga
    opendir my $dh, $tbsp2 || die "Can't opendir $tbsp2: $!";
    my ($tbsp2_tmp) = grep { /PG_[.0-9]+_\d+/ } readdir($dh);
    $tbsp2_tmp = "$tbsp2/$tbsp2_tmp/pgsql_tmp";
    close $dh;

    mkdir $tbsp2_tmp || die "Can't openmkdir $tbsp2_tmp: $!";
    open my $fh, ">", "$tbsp2_tmp/pgsql_tmp1.1" || die "Can't open $tbsp2_tmp/pgsql_tmp1.1: $!";
    print $fh "DATA"x1024;
    close $fh;
    open $fh, ">", "$tbsp2_tmp/pgsql_tmp1.2" || die "Can't open $tbsp2_tmp/pgsql_tmp1.2: $!";
    print $fh "DATA"x1024;
    close $fh;

    $node->psql('postgres', q{
      SET temp_tablespaces TO myts2;
      SELECT count(*) FROM generate_series(1,10000) ;
    });

    ok(-f "$tbsp2_tmp/pgsql_tmp1.1", "temp file pgsql_tmp1.1 exists in tablespace myts2");
    ok(-f "$tbsp2_tmp/pgsql_tmp1.2", "temp file pgsql_tmp1.2 exists in tablespace myts2");

    usleep(100_000) while tv_interval($t0) < 1.01;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: [3-5] tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: # files in myts2=2File$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: Total size in myts2=8kB$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: postgres=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: postgres=[1-9][.0-9]*[kMGTPE]*B$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*Fpm$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*Bpm$/m,
          qr/^Perfdata *: template1=[1-9][0-9]*Files$/m,
          qr/^Perfdata *: template1=[1-9][.0-9]*[kMGTPE]*B$/m,
        ],
        [ qr/^$/ ],
        'test with a tablespace'
    );
}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
