# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package pgaTester;

use strict;
use warnings;
use version;

use Carp;
use Test::More;
use TestLib ();
use Cwd 'cwd';

our $class;

# factory
BEGIN {
    my $ver;
    my $err;

    ($ver, $err) = TestLib::run_command(['pg_config', '--version']);
    die "could not find pg_config version". ($err)? ": $err":''
        unless $ver;

    $ver =~ /PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?/;
    die "could not parse initdb version: $ver" unless defined $1 and defined $2;

    if ($1 > 9) {
        $class = "PostgresNode$1";
    }
    else {
        $class = "PostgresNode$1$2";
    }

    require "${class}.pm";

    $ENV{TESTDIR} = cwd;
}

sub new {
    shift;

    return $class->new(@_);
}

sub get_new_node {
    shift;

    return $class->get_new_node(@_);
}
1
