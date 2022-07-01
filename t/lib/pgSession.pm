# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

package pgSession;

use strict;
use warnings;
use version;

use Carp;

sub new {
    my $class = shift;
    my $node  = shift;
    my $db    = shift;
    my $self;

    $db = 'template1' unless defined $db;

    $self->{'timer'} = IPC::Run::timer(5);
    $self->{'in'}    = '';
    $self->{'out'}   = '';
    $self->{'proc'}  = $node->interactive_psql(
        $db, \$self->{'in'}, \$self->{'out'}, $self->{'timer'},
        extra_params=>[ '--pset=pager' ]
    );

    return bless $self, $class;
}

sub query {
    my ($self, $q, $t) = @_;

    $self->{'out'} = '';
    $self->{'in'}  .= "$q;\n";
    $self->{'timer'}->start($t);
    $self->{'proc'}->pump while length $self->{'in'};
}

1
