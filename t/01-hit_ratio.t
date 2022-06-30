#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 20;

my $node = pgNode->get_new_node('prod');
my $pga_data = "$TestLib::tmp_check/pga.data";

$node->init;

$node->append_conf('postgresql.conf', 'stats_block_level = on')
    if $node->version < 8.3;

$node->start;

### Begin of tests ###

# Check thresholds only accept percentages
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101',
                          '--critical'    => '0'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: critical and warning thresholds only accept percentages.$/m ],
    'Check percentage thresholds'
);

# First check. Returns no perfdata
# Even with ridiculous threshold, no alert is possible during the first call
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101%',
                          '--critical'    => '0%'
    ],
    0,
    [
      qr/^Service  *: POSTGRES_HIT_RATIO$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 database\(s\) checked$/m,
    ],
    [ qr/^$/ ],
    'first basic check'
);

# The hit ratio is computed relatively to the previous check.
# We need to wait at least 1 second to avoid a NaN as a ratio
sleep 1;

# Ridiculous thresholds to trigger a warning
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '101%',
                          '--critical'    => '0%'
    ],
    1,
    [ qr/^Service  *: POSTGRES_HIT_RATIO$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: postgres: [\d.]+%$/m,
      qr/^Perfdata *: postgres=[\d.]+% warn=101 crit=0$/m,
    ],
    [ qr/^$/ ],
    'Warning check'
);

# Ridiculous thresholds to trigger a critical
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'hit_ratio',
                          '--username'    => getlogin,
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
                          '--warning'     => '110%',
                          '--critical'    => '101%'
    ],
    2,
    [ qr/^Service  *: POSTGRES_HIT_RATIO$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: postgres: [\d.]+%$/m,
      qr/^Perfdata *: postgres=[\d.]+% warn=110 crit=101$/m,
    ],
    [ qr/^$/ ],
    'Critical check'
);


### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
