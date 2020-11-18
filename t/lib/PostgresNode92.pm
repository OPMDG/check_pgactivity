# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode92;

use strict;
use warnings;

use TestLib 'slurp_file';
use Test::More;
use parent 'PostgresNode93';

sub version { return 9.2 }

sub init {
    my $self = shift;

    $self->SUPER::init(@_);
    $self->rename_unix_socket_dir;

    return;
}

sub init_from_backup {
    my $self = shift;

    $self->SUPER::init_from_backup(@_);
    $self->rename_unix_socket_dir;

    return;
}

sub rename_unix_socket_dir {
    my $self = shift;
    my $file = $self->data_dir ."/postgresql.conf";
    my $conf;

    $conf = slurp_file($file);

    $conf =~ s/unix_socket_directories/unix_socket_directory/g;

    open my $fd, '>', $file;
    print $fd $conf;
    close $fd;

    return;
}

sub can_skip_init_fsync { return 0 }

1;
