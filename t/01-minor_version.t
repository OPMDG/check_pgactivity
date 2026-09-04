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
use Test::More tests => 12;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# Get current version
my $version_output = $node->safe_psql('postgres', q{
    SELECT to_char(setting::int, 'fm000000')
    FROM pg_settings
    WHERE name = 'server_version_num'
});

my ($v1, $v2, $v3) = $version_output =~ /(\d{2})(\d{2})(\d{2})/;

my $current_version;
if ($v1 >= 10) {
    $current_version = sprintf "%d.%d", $v1, $v3;
}
else {
    $current_version = sprintf "%d.%d.%d", $v1, $v2, $v3;
}

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'minor_version',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => $current_version,
    ],
    0,
    [ qr/^Service  *: POSTGRES_MINOR_VERSION$/m,
      qr/^Message  *: PostgreSQL version .*$/m,
      qr/^Perfdata *: version=.*$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

$v3 += 1;
if ($v1 >= 10) {
    $current_version = sprintf "%d.%d", $v1, $v3;
}
else {
    $current_version = sprintf "%d.%d.%d", $v1, $v2, $v3;
}

# minor version change => Returns WARNING
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'minor_version',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => $current_version,
    ],
    1,
    [ qr/^Service  *: POSTGRES_MINOR_VERSION$/m,
      qr/^Message  *: PostgreSQL version .*\(should be .*\)$/m,
      qr/^Perfdata *: version=.*$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
    ],
    [ qr/^$/ ],
    'minor version upgrade needed'
);


### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
