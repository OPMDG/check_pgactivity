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
use Test::More tests => 39;

my $node = pgNode->get_new_node('prod');
my $proc;

$node->init;
$node->start;

### Beginning of tests ###

# This service can run without thresholds

# Tests for PostreSQL 8.1 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 8.0 and before", 3
        if $node->version >= 8.1;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
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
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 3 tablespace\(s\)\/database\(s\) checked$/m,
        ],
        [ qr/^$/ ],
        'basic check'
    );
   # The added sleep ensures that two tests are not executed within the same seconds. 
   # The time difference is used to compute the Fpm and Bpm perfstats, if it's zero the
   # check crashes (division by zero).
    sleep 2;

    $proc = pgSession->new( $node, 'postgres' );

    # unit test based on the file count => Returns OK
    # The query generates between 17.5MB (9?4) and 11,7MB (14) of WAL
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '3',
                              '--critical' => '4'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 4 tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=3 crit=4$/m,
          qr/^Perfdata *: Total size in pg_default=.*$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=3 crit=4$/m,
          qr/^Perfdata *: postgres=.*B$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=3 crit=4$/m,
          qr/^Perfdata *: template1=0B$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=3 crit=4$/m,
          qr/^Perfdata *: template0=0B$/m,
        ],
        [ qr/^$/ ],
        'test file count OK '
    );
    sleep 2;

    # unit test based on the file count => Returns WARN
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '1',
                              '--critical' => '3'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=1 crit=3$/m,
          qr/^Perfdata *: Total size in pg_default=.*$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=1 crit=3$/m,
          qr/^Perfdata *: postgres=.*B$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=1 crit=3$/m,
          qr/^Perfdata *: template1=0B$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=1 crit=3$/m,
          qr/^Perfdata *: template0=0B$/m,
        ],
        [ qr/^$/ ],
        'test file count WARN'
    );
    sleep 2;

    # unit test based on the file count => Returns CRIT
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '0',
                              '--critical' => '1'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=0 crit=1$/m,
          qr/^Perfdata *: Total size in pg_default=.*$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=0 crit=1$/m,
          qr/^Perfdata *: postgres=.*B$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=0 crit=1$/m,
          qr/^Perfdata *: template1=0B$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=0 crit=1$/m,
          qr/^Perfdata *: template1=0B$/m,
        ],
        [ qr/^$/ ],
        'test file count CRIT'
    );
    sleep 2;

    # unit test based on the file size => Returns OK
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '40MB',
                              '--critical' => '50MB'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 4 tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: # files in pg_default=.*File$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=40MB crit=50MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files$/m,
          qr/^Perfdata *: postgres=.*B warn=40MB crit=50MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files$/m,
          qr/^Perfdata *: template1=0B warn=40MB crit=50MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files$/m,
          qr/^Perfdata *: template0=0B warn=40MB crit=50MB$/m,
        ],
        [ qr/^$/ ],
        'test file size OK'
    );
    sleep 2;

    # unit test based on the file size => Returns WARN
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '4MB',
                              '--critical' => '40MB'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=4MB crit=40MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files$/m,
          qr/^Perfdata *: postgres=.*B warn=4MB crit=40MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files$/m,
          qr/^Perfdata *: template1=0B warn=4MB crit=40MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files$/m,
          qr/^Perfdata *: template0=0B warn=4MB crit=40MB$/m,
        ],
        [ qr/^$/ ],
        'test file size WARN'
    );
    sleep 2;

    # unit test based on the file size => Returns CRIT
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '4MB',
                              '--critical' => '5MB'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=4MB crit=5MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files$/m,
          qr/^Perfdata *: postgres=.*B warn=4MB crit=5MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files$/m,
          qr/^Perfdata *: template1=0B warn=4MB crit=5MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files$/m,
          qr/^Perfdata *: template1=0B warn=4MB crit=5MB$/m,
        ],
        [ qr/^$/ ],
        'test file count CRIT'
    );
    sleep 2;

    # unit test based on the file size and count => Returns OK
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '3,49254kB',
                              '--critical' => '4,65638kB'
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 4 tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=3 crit=4$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=48.102MB crit=64.102MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=3 crit=4$/m,
          qr/^Perfdata *: postgres=.*B warn=48.102MB crit=64.102MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=3 crit=4$/m,
          qr/^Perfdata *: template1=0B warn=48.102MB crit=64.102MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=3 crit=4$/m,
          qr/^Perfdata *: template0=0B warn=48.102MB crit=64.102MB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count OK '
    );
    sleep 2;

    # unit test based on the file size and count => Returns WARN
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '1,16486kB',
                              '--critical' => '3,49254kB'
        ],
        1,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=1 crit=3$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=16.102MB crit=48.102MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=1 crit=3$/m,
          qr/^Perfdata *: postgres=.*B warn=16.102MB crit=48.102MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=1 crit=3$/m,
          qr/^Perfdata *: template1=0B warn=16.102MB crit=48.102MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=1 crit=3$/m,
          qr/^Perfdata *: template0=0B warn=16.102MB crit=48.102MB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count WARN'
    );
    sleep 2;

    # unit test based on the file size and count => Returns CRIT
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '0,0MB',
                              '--critical' => '1,4MB'
        ],
        2,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: pg_default \(.* file\(s\)\/.*\)$/m,
          qr/^Message  *: postgres \(.* file\(s\)\/.*\)$/m,
          qr/^Perfdata *: # files in pg_default=.*File warn=0 crit=1$/m,
          qr/^Perfdata *: Total size in pg_default=.* warn=0B crit=4MB$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files warn=0 crit=1$/m,
          qr/^Perfdata *: postgres=.*B warn=0B crit=4MB$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files warn=0 crit=1$/m,
          qr/^Perfdata *: template1=0B warn=0B crit=4MB$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files warn=0 crit=1$/m,
          qr/^Perfdata *: template1=0B warn=0B crit=4MB$/m,
        ],
        [ qr/^$/ ],
        'test file size and count CRIT'
    );
    sleep 2;

    # unit test with a tablespace => Returns OK
    # * are the tempfiles located in the correct directory ?
    # * do we only account for temp files ? (cf issue #351)
    mkdir $node->basedir . '/tablespace1';
    $node->psql('postgres', 'CREATE TABLESPACE myts1 LOCATION \'' . $node->basedir . '/tablespace1\';');
    mkdir $node->basedir . '/tablespace2';
    $node->psql('postgres', 'CREATE TABLESPACE myts2 LOCATION \'' . $node->basedir . '/tablespace2\';');

    # Create some tables in the tablespaces to make sure their files are not
    # reported as temp files (gh #351).
    $node->psql('postgres', 'CREATE TABLE matable0(x text);');
    $node->psql('postgres', 'CREATE TABLE matable1(x text) TABLESPACE myts1;');
    $node->psql('postgres', 'CREATE TABLE matable2(x text) TABLESPACE myts2;');
    $node->psql('postgres', 'VACUUM;');

    $proc->query('SET temp_tablespaces TO myts2;');
    $proc->query('SELECT random() * x FROM generate_series(1,1000000) AS F(x) ORDER BY 1;', 2);

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'temp_files',
                              '--username' => getlogin,
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
        ],
        0,
        [ qr/^Service  *: POSTGRES_TEMP_FILES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 4 tablespace\(s\)\/database\(s\) checked$/m,
          qr/^Perfdata *: # files in myts2=.*File$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: Total size in myts2=.*$/m,
          qr/^Perfdata *: postgres=.*Fpm$/m,
          qr/^Perfdata *: postgres=.*Bpm$/m,
          qr/^Perfdata *: postgres=.*Files$/m,
          qr/^Perfdata *: postgres=.*B$/m,
          qr/^Perfdata *: template1=0Fpm$/m,
          qr/^Perfdata *: template1=0Bpm$/m,
          qr/^Perfdata *: template1=0Files$/m,
          qr/^Perfdata *: template1=0B$/m,
          qr/^Perfdata *: template0=0Fpm$/m,
          qr/^Perfdata *: template0=0Bpm$/m,
          qr/^Perfdata *: template0=0Files$/m,
          qr/^Perfdata *: template0=0B$/m,
        ],
        [ qr/^$/ ],
        'test with a tablespace'
    );
}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
