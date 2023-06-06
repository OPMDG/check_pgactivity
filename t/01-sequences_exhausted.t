#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

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

### Beginning of tests ###


# basic check without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

$node->psql('testdb', 'CREATE TABLE test1 (i smallint PRIMARY KEY)');
$node->psql('testdb', 'CREATE SEQUENCE test1seq AS smallint INCREMENT BY 1000 START WITH 32000 OWNED BY test1.i');

sleep 1;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => getlogin,
                          '--format'   => 'human',
			  '--warning'  => '5%',
			  '--critical' => '10%'
    ],
    0,
    [ qr/^Service *: POSTGRES_CHECK_SEQ_EXHAUSTED$/m,
      qr/^Returns *: 0 \(OK\)$/m,
     ],
    [ qr/^$/ ],
    'basic check'
);

$node->psql('testdb', "SELECT nextval('test1seq')");

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => getlogin,
                          '--format'   => 'human',
			  '--warning'  => '5%',
			  '--critical' => '10%'
    ],
    2,
    [ qr/^Service *: POSTGRES_CHECK_SEQ_EXHAUSTED$/m,
      qr/^Returns *: 2 \(CRITICAL\)$/m,
     ],
    [ qr/^$/ ],
    'check critical'
);

# stop immediate to kill any remaining backends
$node->stop('immediate');

done_testing();

