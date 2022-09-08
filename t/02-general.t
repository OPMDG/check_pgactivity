#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

# This file gather various regression tests against the same cluster to avoid
# creating one cluster per regression test.

use strict;
use warnings;

use Storable ('store');
use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 2;

my $node = pgNode->get_new_node('prod');

$node->init;

$node->append_conf('postgresql.conf', 'stats_block_level = on')
    if $node->version < 8.3;

$node->start;

### Begin of tests ###

# == Regression test for #326 ==
# check_pga should not complain when using an existing status file missing
# without an existing lock file.
my $pga_data = "$TestLib::tmp_check/tmp-status-file.data";

# Create an empty status file
store( {}, $pga_data);
# make there's no lock file leftovers from previous tests...
unlink "${pga_data}.lock";

# Trigger the failure
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101%',
                          '--critical'    => '0%'
    ],
    0, [], [ qr/^$/ ], 'No error should occurs'
);
# cleanup everything for the next regression test
unlink($pga_data, "${pga_data}.lock");
