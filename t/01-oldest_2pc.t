#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2025: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use Test::More tests => 22;

my $node = pgNode->get_new_node('prod');

$node->init();
$node->append_conf('postgresql.conf', "max_prepared_transactions = 10");
$node->start;

### Beginning of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_2pc',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);


$node->psql('postgres', 'CREATE TABLE preptx (x text)');
$node->psql('postgres', '
    INSERT INTO preptx (x)
    SELECT md5(i::text)
    FROM generate_series(1, 10) i'
);

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_2pc',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '10s',
                          '--critical' => '2h',
    ],
    0,
    [ qr/^Service  *: POSTGRES_OLDEST_2PC$/m,
      qr/^Message  *: 0 prepared transaction\(s\)$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

my $proc = pgSession->new($node, 'postgres');

$proc->query('BEGIN', 0);
$proc->query("INSERT INTO preptx(x) VALUES ('test 2pc');", 0);
$proc->query("PREPARE TRANSACTION 'testpreptx'", 0);

sleep(2);

# check one prepared xact, no alert
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_2pc',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '1h',
                          '--critical' => '2h',
    ],
    0,
    [ qr/^Service  *: POSTGRES_OLDEST_2PC$/m,
      qr/^Message  *: 1 prepared transaction\(s\)$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Perfdata *: postgres # prep. xact=1$/m,
    ],
    [ qr/^$/ ],
    'check one prepared xact no alert'
);


# check one prepared xact, no alert
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_2pc',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '1s',
                          '--critical' => '2h',
    ],
    1,
    [ qr/^Service  *: POSTGRES_OLDEST_2PC$/m,
      qr/^Message  *: 1 prepared transaction\(s\)$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Perfdata *: postgres max=[0-9][.0-9]*s warn=1 crit=7200$/m,
      qr/^Perfdata *: postgres avg=[0-9]s warn=1 crit=7200$/m,
      qr/^Perfdata *: postgres # prep. xact=1$/m,
    ],
    [ qr/^$/ ],
    'check one prepared xact with alert'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
