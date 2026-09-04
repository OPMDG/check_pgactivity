#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More;

plan tests => 12;

# declare instances
note "3 instances creation";
my $prim      = pgNode->get_new_node('prim');
my $sec1      = pgNode->get_new_node('sec1');
my $sec2      = pgNode->get_new_node('sec2');

my $backup    = 'backup_from_prim';
my $pgversion = $prim->version;
my $service_file = "tmp_check/pg_service.conf";

note "\ntesting on version $pgversion";
my $primp = $prim->port ;
my $primh = $prim->host ;
my $sec1h = $sec1->host ;
my $sec1p = $sec1->port ;
my $sec2h = $sec2->host ;
my $sec2p = $sec2->port ;
my $u = $ENV{'USER'} || 'postgres';

# Tests for PostreSQL 9.0 and after
SKIP: {
    skip "these tests requires PostgreSQL 9.1 and after", 12
        unless $pgversion >= '9.0';

    $ENV{PGSERVICEFILE} = $service_file;

    # create primary and start it
    $prim->init(allows_streaming => 1);
    $prim->start;
    note("prim started");

    # create backup
    $prim->backup($backup);
    note("backup done");

    # create standby from backup and start it
    $sec1->init_from_backup($prim, $backup, has_streaming => 1);
    $sec1->start;
    note("sec1 started");

    # create standby from backup and start it
    $sec2->init_from_backup($prim, $backup, has_streaming => 1);
    $sec2->start;
    note("standby 2 started");

    # checkpoint to avoid waiting long time for the standby to catchup
    $prim->safe_psql('template1', 'checkpoint');

    # service file creation
    note "Creating service file $service_file" ;
    note "Ports $primp $sec1p $sec2p" ;
    note "Hosts $primh $sec1h $sec2h" ;
    open(my $fh, '>', $service_file)
        or die "creation of $service_file failed!";

    print $fh <<"EOSF";
[prim]
host=$primh
port=$primp
user=$u
dbname=postgres
[sec1]
host=$sec1h
port=$sec1p
user=$u
dbname=postgres
[sec2]
host=$sec2h
port=$sec2p
user=$u
dbname=postgres
EOSF
    close($fh);

    # wait for standbys to catchup
    $prim->wait_for_catchup($sec1, 'replay', $prim->lsn('insert'));
    $prim->wait_for_catchup($sec2, 'replay', $prim->lsn('insert'));
    note("standbys caught up");

    ### Beginning of tests ###

    # Check that all clusters are identical and ignore what is not
    note "Normal check with one standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'configuration_difference',
                              '--format'   => 'human',
                              '--dbservice'   => 'prim,sec1,sec2',
                              '--exclude'   => 'cluster_name',
                              '--exclude'   => 'port',
                              '--ignore-paths'
        ],
        0,
        [
            qr/Service  *: POSTGRES_CONFIGURATION_DIFFERENCE$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 3 Configurations checked :prim sec1 sec2$/m
        ],
        [ qr/^$/ ],
        'three clusters supposed identical'
    );

    # Must detect that cluster_name is different (2 clusters only)
    note "Normal check with one standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'configuration_difference',
                              '--format'   => 'human',
                              '--dbservice'   => 'prim,sec1',
                              '--exclude'   => 'port',
                              '--ignore-paths'
        ],
        1,
        [
            qr/Service  *: POSTGRES_CONFIGURATION_DIFFERENCE$/m,
            qr/Returns  *: 1 \(WARNING\)$/m,
            qr/Message  *: \[sec1\] : cluster_name=sec1 \(≠ prim\)$/m
        ],
        [ qr/^$/ ],
        'two clusters, one difference'
    );

    # Failure with a single service
    note "Normal check with one standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'configuration_difference',
                              '--format'   => 'human',
                              '--dbservice'   => 'prim',
        ],
        127,
        [
        ],
        [ qr/^FATAL.*$/m ],
        "one cluster and it won't work"
    );



} # end of SKIP

### End of tests ###
done_testing();

unlink($service_file) or warn "$0: could not unlink $service_file!\n";

# stop immediate to kill any remaining backends
$prim->stop('immediate');
$sec1->stop('immediate');
$sec2->stop('immediate');
