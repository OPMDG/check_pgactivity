# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode95;

use strict;
use warnings;

use Test::More;
use parent 'PostgresNode96';

sub version { return 9.5 }

sub init
{
	my ($self, %params) = @_;
	my $port   = $self->port;
	my $pgdata = $self->data_dir;
	my $host   = $self->host;
    my @cmd    = ( 'initdb', '-D', $pgdata, '-A', 'trust' );

    push @cmd, @{ $params{extra} } if defined $params{extra};
    push @cmd, '-N' if $self->can_skip_init_fsync;

	$params{allows_streaming} = 0 unless defined $params{allows_streaming};
	$params{has_archiving}    = 0 unless defined $params{has_archiving};

	mkdir $self->backup_dir;
	mkdir $self->archive_dir;

	TestLib::system_or_bail(@cmd);
	TestLib::system_or_bail($ENV{PG_REGRESS}, '--config-auth', $pgdata,
		@{ $params{auth_extra} });

	open my $conf, '>>', "$pgdata/postgresql.conf";
	print $conf "\n# Added by PostgresNode.pm\n";
	print $conf "fsync = off\n";
	print $conf "restart_after_crash = off\n" if $self->can_restart_after_crash;
	print $conf "log_line_prefix = '%m [%p] %q%a '\n";
	print $conf "log_statement = all\n";
    if ($self->version >= 9.5)
    {
        print $conf "log_replication_commands = on\n";
        print $conf "wal_retrieve_retry_interval = '500ms'\n";
    }

	# If a setting tends to affect whether tests pass or fail, print it after
	# TEMP_CONFIG.  Otherwise, print it before TEMP_CONFIG, thereby permitting
	# overrides.  Settings that merely improve performance or ease debugging
	# belong before TEMP_CONFIG.
	print $conf TestLib::slurp_file($ENV{TEMP_CONFIG})
	  if defined $ENV{TEMP_CONFIG};

	# XXX Neutralize any stats_temp_directory in TEMP_CONFIG.  Nodes running
	# concurrently must not share a stats_temp_directory.
	print $conf "stats_temp_directory = 'pg_stat_tmp'\n";

	if ($params{allows_streaming})
	{
		if ($params{allows_streaming} eq "logical")
		{
			print $conf "wal_level = logical\n";
		}
		else
		{
			print $conf "wal_level = hot_standby\n";
		}
		print $conf "max_wal_senders = 5\n";
		print $conf "max_replication_slots = 10\n" if $self->can_slots;
		print $conf "wal_log_hints = on\n" if $self->can_log_hints;
		print $conf "hot_standby = on\n";
		# conservative settings to ensure we can run multiple postmasters:
		print $conf "shared_buffers = 1MB\n";
		print $conf "max_connections = 10\n";
		# limit disk space consumption, too:
		print $conf "max_wal_size = 128MB\n" if $self->version >= 9.5;
	}
	else
	{
		print $conf "wal_level = minimal\n";
		print $conf "max_wal_senders = 0\n";
	}

	print $conf "port = $port\n";
	if ($self->use_tcp)
	{
		print $conf "unix_socket_directories = ''\n";
		print $conf "listen_addresses = '$host'\n";
	}
	else
	{
		print $conf "unix_socket_directories = '$host'\n";
		print $conf "listen_addresses = ''\n";
	}
	close $conf;

	chmod($self->group_access ? 0640 : 0600, "$pgdata/postgresql.conf")
	  or die("unable to set permissions for $pgdata/postgresql.conf");

	$self->set_replication_conf if $params{allows_streaming};
	$self->enable_archiving     if $params{has_archiving};
	return;
}

sub can_slots               { return 1 }
sub can_log_hints           { return 1 }
sub can_restart_after_crash { return 1 }
sub can_skip_init_fsync     { return 1 }

1;
