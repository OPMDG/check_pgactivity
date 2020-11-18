# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode11;

use strict;
use warnings;

use Exporter 'import';

use Test::More;
use TestLib ();
use parent 'PostgresNode13';

sub version { return 11 }

# superuser_reserved_connections + max_wal_senders must be less
# than max_connections
sub init {
    my $self   = shift;
    my %params = @_;

    $self->SUPER::init(@_);

    $self->append_conf('postgresql.conf', 'max_wal_senders = 5')
        if $params{allows_streaming};

    return;
}

# Internal routine to enable streaming replication on a standby node.
sub enable_streaming {
    my $self         = shift;
    my $root_node    = shift;
    my $root_connstr = $root_node->connstr;
    my $name         = $self->name;

    print "### Enabling streaming replication for node \"$name\"\n";

    $self->append_conf( 'recovery.conf', qq{
        primary_conninfo='$root_connstr application_name=$name'
        standby_mode=on
    });
}

# Internal routine to enable archiving
# Imported/edited from PostgresNode.pm from branch REL_11_STABLE
sub enable_archiving {
    my $self = shift;
    my $path = TestLib::perl2host($self->archive_dir);
    my $name = $self->name;
    my $copy_command;

    print "### Enabling WAL archiving for node \"$name\"\n";

    # On Windows, the path specified in the restore command needs to use
    # double back-slashes to work properly and to be able to detect properly
    # the file targeted by the copy command, so the directory value used
    # in this routine, using only one back-slash, need to be properly changed
    # first. Paths also need to be double-quoted to prevent failures where
    # the path contains spaces.
    $path =~ s{\\}{\\\\}g if $TestLib::windows_os;

    $copy_command = $TestLib::windows_os ?
        qq{copy "%p" "$path\\\\%f"}
        : qq{cp "%p" "$path/%f"};

    # Enable archive_mode and archive_command on node
    $self->append_conf( 'postgresql.conf', qq{
        archive_mode = on
        archive_command = '$copy_command'
    });
}

# Internal routine to enable archive recovery command on a standby node
# Imported/edited from PostgresNode.pm from branch REL_11_STABLE
sub enable_restoring {
    my $self      = shift;
    my $root_node = shift;
    my $path      = TestLib::perl2host($root_node->archive_dir);
    my $name      = $self->name;
    my $copy_command;

    print "### Enabling WAL restore for node \"$name\"\n";

    # On Windows, the path specified in the restore command needs to use
    # double back-slashes to work properly and to be able to detect properly
    # the file targeted by the copy command, so the directory value used
    # in this routine, using only one back-slash, need to be properly changed
    # first. Paths also need to be double-quoted to prevent failures where
    # the path contains spaces.
    $path =~ s{\\}{\\\\}g if $TestLib::windows_os;

    $copy_command = $TestLib::windows_os ?
        qq{copy "$path\\\\%f" "%p"}
        : qq{cp "$path/%f" "%p"};

    $self->append_conf( 'recovery.conf', qq{
        restore_command = '$copy_command'
        standby_mode = on
    });
}

1;
