#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 8;

my $node = pgNode->new('prod'); # declare instance named "prod"

# create the instance and start it
$node->init;
$node->start;

### Beginning of tests ###

# This command sets PGHOST and PGPORT, then call and test the given command
$node->command_checks_all( [
    # command to run
    './check_pgactivity', '--service'  => 'connection',
                          '--username' => getlogin
    ],
    # expected return code
    0,
    # array of regex matching expected standard output
    [ qr/^POSTGRES_CONNECTION OK: Connection successful at [-+:\. \d]+, on PostgreSQL [\d\.]+.*$/ ],
    # array of regex matching expected error output
    [ qr/^$/ ],
    # a name for this test
    'connection successful'
);

# Failing to connect
# TODO: should stdout only report the user message and stderr the psql error?
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'connection',
                          '--port'     => $node->port -1, # wrong port
                          '--username' => getlogin
    ],
    2,
    [
        qr/^CHECK_PGACTIVITY CRITICAL: Query failed !$/m,
        # v12 and after adds " error:" in output
        qr/^psql:(?: error:)? (connection to server .* failed|could not connect to server):/m,
        qr/^\s*Is the server running locally and accepting/m
    ],
    [ qr/^$/ ],
    'connection failing'
);


### End of tests ###

$node->stop;
