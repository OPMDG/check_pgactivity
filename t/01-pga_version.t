#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use TestLib;
use Test::More tests => 18;

### Beginning of tests ###

my $good_version = '2.6';
my $bad_version  = '0.0';
my $not_version  = 'whatever';

command_checks_all( [
    # command to run
    './check_pgactivity', '--service' => 'pga_version',
                          '--warning' => $good_version
    ],
    # expected return code
    0,
    # array of regex matching expected standard output
    [ qr/PGACTIVITY_VERSION OK: check_pgactivity $good_version, Perl [\d\.]+/ ],
    # array of regex matching expected error output
    [ qr/^$/ ],
    # a name for this test
    'pga_version OK using --warning'
);

command_checks_all( [
    './check_pgactivity', '--service'  => 'pga_version',
                          '--critical' => $good_version
    ],
    0,
    [ qr/^PGACTIVITY_VERSION OK: check_pgactivity $good_version, Perl [\d\.]+$/ ],
    [ qr/^$/ ],
    'pga_version OK using --critical'
);

command_checks_all( [
    './check_pgactivity', '--service' => 'pga_version',
                          '--warning' => $bad_version
    ],
    1,
    [ qr/^PGACTIVITY_VERSION WARNING: check_pgactivity $good_version \(should be $bad_version!\), Perl [\d\.]+$/ ],
    [ qr/^$/ ],
    'pga_version failing using --warning'
);

command_checks_all( [
    './check_pgactivity', '--service'  => 'pga_version',
                          '--critical' => $bad_version
    ],
    2,
    [ qr/^PGACTIVITY_VERSION CRITICAL: check_pgactivity $good_version \(should be $bad_version!\), Perl [\d\.]+$/ ],
    [ qr/^$/ ],
    'pga_version failing using --critical'
);

command_checks_all( [
    './check_pgactivity', '--service'  => 'pga_version',
                          '--warning'  => $good_version,
                          '--critical' => $good_version
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must provide a warning or a critical threshold for service pga_version!$/m ],
    'pga_version error with both --warning and --critical'
);

command_checks_all( [
    './check_pgactivity', '--service'  => 'pga_version',
                          '--critical' => $not_version
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: given version does not look like a check_pgactivity version!$/m ],
    'pga_version error on wrong version format'
);

### End of tests ###
