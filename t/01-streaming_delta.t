#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More;

my $num_tests = 118;

# we have $num_tests normal tests + three tests for incompatible pg versions
plan tests => $num_tests + 3;


# declare instance named "prim"
my $prim   = pgNode->get_new_node('prim');
# declare standby instances "sec1" and "sec2"
my $stb1   = pgNode->get_new_node('sec1');
my $stb2   = pgNode->get_new_node('sec2');
my $backup = 'backup'; # backup name
my $pgversion;

$pgversion = $prim->version;
note "testing on version $pgversion";

# Tests for PostreSQL 9.0 and before
SKIP: {
    # "skip" allows to ignore the whole bloc based on the given a condition
    skip "skip non-compatible test on PostgreSQL 9.0 and before", 3
        unless $pgversion <= '9.0';

    $prim->init;
    $prim->start;

    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human'
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service streaming_delta is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

# Tests for PostreSQL 9.1 and after
SKIP: {
    skip "these tests requires PostgreSQL 9.1 and after", $num_tests
        unless $pgversion >= '9.1';

    # create primary and start it
    $prim->init(allows_streaming => 1);
    $prim->start;
    note("primary started");

    # create backup
    $prim->backup($backup);
    note("backup done");

    # create standby from backup and start it
    $stb1->init_from_backup($prim, $backup, has_streaming => 1);
    $stb1->start;
    note("standby 1 started");

    # create standby from backup and start it
    $stb2->init_from_backup($prim, $backup, has_streaming => 1);
    $stb2->start;
    note("standby 2 started");

    # checkpoint to avoid waiting long time for the standby to catchup
    $prim->safe_psql('template1', 'checkpoint');

    # wait for standbys to catchup
    $prim->wait_for_catchup($stb1, 'replay', $prim->lsn('insert'));
    $prim->wait_for_catchup($stb2, 'replay', $prim->lsn('insert'));
    note("standbys caught up");

    ### Beginning of tests ###

    # Normal check with two standby
    note "Normal check with two standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human'
        ],
        0,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 2 slaves checked$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=0B$/m,
            qr/Perfdata *: flushed delta sec1@=0B$/m,
            qr/Perfdata *: replay delta sec1@=0B$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: sent delta sec2@=0B$/m,
            qr/Perfdata *: wrote delta sec2@=0B$/m,
            qr/Perfdata *: flushed delta sec2@=0B$/m,
            qr/Perfdata *: replay delta sec2@=0B$/m,
            qr/Perfdata *: pid sec2@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=2$/m
        ],
        [ qr/^$/ ],
        'two standbys streaming'
    );

    # Normal check excluding one
    note "Normal check with two standby, excluding one";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--exclude'  => 'sec1',
                              '--format'   => 'human'
        ],
        0,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 1 slaves checked$/m,
            qr/Perfdata *: sent delta sec2@=0B$/m,
            qr/Perfdata *: wrote delta sec2@=0B$/m,
            qr/Perfdata *: flushed delta sec2@=0B$/m,
            qr/Perfdata *: replay delta sec2@=0B$/m,
            qr/Perfdata *: pid sec2@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=1$/m,
            qr/Perfdata *: # of slaves=2$/m
        ],
        [ qr/^$/ ],
        'excluding one standby'
    );

    # Normal check excluding both
    note "Normal check excluding both standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--exclude'  => 'sec[12]',
                              '--format'   => 'human'
        ],
        0,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 0 slaves checked$/m,
            qr/Perfdata *: # of excluded slaves=2$/m,
            qr/Perfdata *: # of slaves=2$/m
        ],
        [ qr/^$/ ],
        'excluding one standby'
    );

    # normal check with one explicit standby
    note "normal check with one explicit standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--format'   => 'human'
        ],
        0,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 2 slaves checked$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=0B$/m,
            qr/Perfdata *: flushed delta sec1@=0B$/m,
            qr/Perfdata *: replay delta sec1@=0B$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: sent delta sec2@=0B$/m,
            qr/Perfdata *: wrote delta sec2@=0B$/m,
            qr/Perfdata *: flushed delta sec2@=0B$/m,
            qr/Perfdata *: replay delta sec2@=0B$/m,
            qr/Perfdata *: pid sec2@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=2$/m
        ],
        [ qr/^$/ ],
        'one explicit standby'
    );

    # failing check when called with an explicit standby not connected
    note "failing check when called with an explicit standby not connected";
    $stb1->stop( 'fast' );
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--format'   => 'human'
        ],
        2,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 2 \(CRITICAL\)$/m,
            qr/Message  *: sec1  not connected$/m,
            qr/Perfdata *: sent delta sec2@=0B$/m,
            qr/Perfdata *: wrote delta sec2@=0B$/m,
            qr/Perfdata *: flushed delta sec2@=0B$/m,
            qr/Perfdata *: replay delta sec2@=0B$/m,
            qr/Perfdata *: pid sec2@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=1$/m
        ],
        [ qr/^$/ ],
        'one failing explicit standby'
    );

    # no standby connected
    note "no standby connected";
    $stb2->stop( 'fast' );
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human'
        ],
        3,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 3 \(UNKNOWN\)$/m,
            qr/Message  *: No slaves connected$/m,
        ],
        [ qr/^$/ ],
        'no standby connected'
    );

    ## warning on flush
    note "warning on flush";
    $stb1->start;
    $prim->wait_for_catchup($stb1, 'write', $prim->lsn('insert'));

    $prim->command_checks_all( [
        'perl', '-It/lib', '-MMocker::Streaming',
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--warning'  => '512,4MB',
                              '--critical' => '4MB,4MB',
                              '--format'   => 'human'
        ],
        1,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 1 \(WARNING\)$/m,
            qr/Message  *: warning flush lag: 2MB for sec1\@$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=1024kB$/m,
            qr/Perfdata *: flushed delta sec1@=2MB warn=512B crit=4MB$/m,
            qr/Perfdata *: replay delta sec1@=3MB warn=4MB crit=4MB$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=1$/m
        ],
        [ qr/^$/ ],
        'one explicit standby warning on flush lag'
    );

    ## critical on flush
    note "critical on flush";

    $prim->command_checks_all( [
        'perl', '-It/lib', '-MMocker::Streaming',
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--warning'  => '512,4MB',
                              '--critical' => '1MB,4MB',
                              '--format'   => 'human'
        ],
        2,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 2 \(CRITICAL\)$/m,
            qr/Message  *: critical flush lag: 2MB for sec1\@$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=1024kB$/m,
            qr/Perfdata *: flushed delta sec1@=2MB warn=512B crit=1024kB$/m,
            qr/Perfdata *: replay delta sec1@=3MB warn=4MB crit=4MB$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=1$/m
        ],
        [ qr/^$/ ],
        'one explicit standby critical on flush lag'
    );

    ## warning on replay
    note "warning on replay";

    $prim->command_checks_all( [
        'perl', '-It/lib', '-MMocker::Streaming',
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--warning'  => '3MB,512',
                              '--critical' => '4MB,4MB',
                              '--format'   => 'human'
        ],
        1,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 1 \(WARNING\)$/m,
            qr/Message  *: warning replay lag: 3MB for sec1\@$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=1024kB$/m,
            qr/Perfdata *: flushed delta sec1@=2MB warn=3MB crit=4MB$/m,
            qr/Perfdata *: replay delta sec1@=3MB warn=512B crit=4MB$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=1$/m
        ],
        [ qr/^$/ ],
        'one explicit standby warning on replay lag'
    );

    ## critical on replay
    note "critical on replay";

    $prim->command_checks_all( [
        'perl', '-It/lib', '-MMocker::Streaming',
        './check_pgactivity', '--service'  => 'streaming_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--slave'    => 'sec1 ',
                              '--warning'  => '3MB,512',
                              '--critical' => '4MB,2MB',
                              '--format'   => 'human'
        ],
        2,
        [
            qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
            qr/Returns  *: 2 \(CRITICAL\)$/m,
            qr/Message  *: critical replay lag: 3MB for sec1\@$/m,
            qr/Perfdata *: sent delta sec1@=0B$/m,
            qr/Perfdata *: wrote delta sec1@=1024kB$/m,
            qr/Perfdata *: flushed delta sec1@=2MB warn=3MB crit=4MB$/m,
            qr/Perfdata *: replay delta sec1@=3MB warn=512B crit=2MB$/m,
            qr/Perfdata *: pid sec1@=\d+$/m,
            qr/Perfdata *: # of excluded slaves=0$/m,
            qr/Perfdata *: # of slaves=1$/m
        ],
        [ qr/^$/ ],
        'one explicit standby critical on replay lag'
    );

    $stb1->stop( 'immediate' );
    $stb2->stop( 'immediate' );
} # End of SKIP

### End of tests ###

$prim->stop( 'immediate' );
