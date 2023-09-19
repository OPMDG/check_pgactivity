#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

# This file gathers various regression tests against the same cluster to avoid
# creating one cluster per regression test.

use strict;
use warnings;

use Storable ('store');
use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 10;

my $node = pgNode->get_new_node('prod');

$node->init;

$node->append_conf('postgresql.conf', 'stats_block_level = on')
    if $node->version < 8.3;

$node->start;

### Beginning of tests ###

# == Regression test for #326 ==
# check_pga should not complain when using an existing status file
# without an existing lock file.
my $pga_data = "$TestLib::tmp_check/tmp-status-file.data";

# make sure there's no leftover files from previous tests...
unlink $pga_data;
unlink "${pga_data}.lock";

ok( ! -f $pga_data, "double check the status file does not exist" );
ok( ! -f "${pga_data}.lock", "double check the lock file does not exist" );

# First call to create the status and lock files
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101%',
                          '--critical'    => '0%'
    ],
    0, [], [ qr/^$/ ], 'No error should occur'
);

ok( -f $pga_data, "status file created from first check_pga call" );
ok( -f "${pga_data}.lock", "lock file created from first check_pga call" );

# Remove the lock file to trigger the failure described in issue #326
unlink( "${pga_data}.lock" ) or BAIL_OUT( "could not remove the lock file" );

ok( ! -f "${pga_data}.lock", "lock file removed" );

# The hit ratio is computed relatively to the previous check.
# We need to wait at least 1 second to avoid a NaN as a ratio
sleep 1;

# trigger the failure described in issue #326
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101%',
                          '--critical'    => '0%'
    ],
    1, [], [ qr/^$/ ], 'No error should occur if the lock file is missing'
);

ok( -f "${pga_data}.lock", "lock file created from second check_pga call" );

# cleanup everything for the next regression test
unlink($pga_data, "${pga_data}.lock");
