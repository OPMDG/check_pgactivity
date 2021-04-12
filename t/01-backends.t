#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2021: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgaTester;
use TestLib ();
use IPC::Run ();
use Test::More tests => 34;

my $node = pgaTester->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;

$node->init;
$node->append_conf('postgresql.conf', 'max_connections=8');
$node->start;

### Begin of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

# basic check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    0,
    [ qr/^Service  *: POSTGRES_BACKENDS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 connections on 5$/m,
      qr/^Perfdata *: template1=1 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: maximum_connections=5 min=0 max=5$/m
    ],
    [ qr/^$/ ],
    'basic check'
);

# two sessions on two different db

TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

sub get_session {
    my $db = shift ;
    my %s  = ();

    $db = 'template1' unless defined $db;

    $s{'timer'} = IPC::Run::timer(5);
    $s{'in'}    = '';
    $s{'out'}   = '';
    $s{'proc'}  = $node->interactive_psql(
        $db, \$s{'in'}, \$s{'out'}, $s{'timer'}
    );

    return \%s;
}

sub exec_session {
    my ($s, $q, $t) = @_;

    $s->{'out'} = '';
    $s->{'in'}  = "$q;\n";
    $s->{'timer'}->start($t);
    $s->{'proc'}->pump while length $s->{'in'};
}

push @procs, get_session('testdb');

exec_session($procs[0], 'select pg_sleep(60)', 60);

# wait for backend to be connected and active
$node->poll_query_until('template1', q{
    SELECT query_start < now()
    FROM pg_catalog.pg_stat_activity
    WHERE datname = 'testdb'
    LIMIT 1
});

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    0,
    [ qr/^Service  *: POSTGRES_BACKENDS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 2 connections on 5$/m,
      qr/^Perfdata *: template1=1 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: testdb=1 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: maximum_connections=5 min=0 max=5$/m
    ],
    [ qr/^$/ ],
    'two sessions'
);

# add two new backends and test warning
push( @procs, get_session ) for (1..2);

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    1,
    [ qr/^Service  *: POSTGRES_BACKENDS$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 4 connections on 5$/m,
      qr/^Perfdata *: template1=3 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: testdb=1 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: maximum_connections=5 min=0 max=5$/m
    ],
    [ qr/^$/ ],
    'warning with four sessions'
);

# add a new backends and test critical
push @procs, get_session;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    2,
    [ qr/^Service  *: POSTGRES_BACKENDS$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 5 connections on 5$/m,
      qr/^Perfdata *: template1=4 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: testdb=1 warn=4 crit=5 min=0 max=5$/m,
      qr/^Perfdata *: maximum_connections=5 min=0 max=5$/m
    ],
    [ qr/^$/ ],
    'critical with five sessions'
);

# finish all sessions
for (my $i = 0; $i < @procs; $i++ ) {
    $procs[$i]{'proc'}->signal('INT');
    $procs[$i]{'timer'}->start(2);
    $procs[$i]{'in'} .= "\\q\n";
    $procs[$i]{'proc'}->pump while length $procs[$i]{'in'};
    $procs[$i]{'proc'}->finish or BAIL_OUT "psql $i returned $?";
    $procs[$i]{'timer'}->reset;
}

### End of tests ###

$node->stop;
