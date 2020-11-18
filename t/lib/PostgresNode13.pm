# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode13;

use strict;
use warnings;

use Test::More;
use parent 'PostgresNode';

sub new
{
    my $class = shift;
    my $self;

    $self = $class->SUPER::new(@_);

    $self->{test_pghost}    = $PostgresNode::test_pghost;
    $self->{test_localhost} = $PostgresNode::test_localhost;
    $self->{use_tcp}        = $PostgresNode::use_tcp;

    return $self;
}

sub is_default_host { return $_[0]->host eq $_[0]->{test_pghost} }
sub test_localhost  { return $_[0]->{test_localhost} }
sub use_tcp         { return $_[0]->{use_tcp} }
sub version         { return 13 }

1
