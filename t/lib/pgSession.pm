# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

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
    $self->{'delim'} = 'CHECK_PGA_PROMPT_DELIM=>';
    $self->{'in'}    = '';
    $self->{'out'}   = '';
    $self->{'proc'}  = $node->interactive_psql(
        $db, \$self->{'in'}, \$self->{'out'}, $self->{'timer'},
        extra_params=>[
            '--pset=pager',
            '--variable=PROMPT1='. $self->{'delim'}
        ]
    );

    return bless $self, $class;
}

sub query {
    my ($self, $q, $t) = @_;

    $self->{'out'} = '';
    $self->{'in'} = '';

    $self->{'timer'}->start($t);

    # wait for the prompt to appear
    $self->{'proc'}->pump until $self->{'out'} =~ $self->{'delim'};;

    # reset the output to forget the banner + prompt
    $self->{'out'} = '';

    # write and run the query (this echoes the query in $out :/)
    $self->{'in'}  .= "$q;\n";

    # push $in to the procs
    $self->{'proc'}->pump while length $self->{'in'};
}

1
