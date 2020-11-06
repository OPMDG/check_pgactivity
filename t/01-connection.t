#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't';
use PostgresNode;
use Test::More;
use TestLib 'command_checks_all';

my $node = PostgresNode->get_new_node('prod'); # declare instance named "prod"

# create the instance and start it
$node->init;
$node->start;

### Begin of tests ###

command_checks_all( [
    # command to run
    './check_pgactivity', '--service'  => 'connection',
                          '--host'     => $node->host,
                          '--port'     => $node->port,
                          '--username' => getlogin
    ],
    # expected return code
    0,
    # array of regex matching expected standard output
    [ qr/POSTGRES_CONNECTION OK: Connection successful/ ],
    # array of regex matching expected error output
    undef,
    # a name for this test
    'check_connection'
);

### End of tests ###

$node->stop;
done_testing;
