# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode96;

use strict;
use warnings;

use Carp;
use Scalar::Util qw(blessed);
use Test::More;
use parent 'PostgresNode11';

sub version { return 9.6 }

# pg_ctl doesn't wait for full-start in 9.6 and before
sub start {
    my $self   = shift;
    my %params = @_;
    my $port   = $self->port;
    my $pgdata = $self->data_dir;
    my $name   = $self->name;
    my $ret;

    BAIL_OUT("node \"$name\" is already running") if defined $self->{_pid};

    print("### Starting node \"$name\"\n");

    {
        # Temporarily unset PGAPPNAME so that the server doesn't
        # inherit it.  Otherwise this could affect libpqwalreceiver
        # connections in confusing ways.
        local %ENV = %ENV;
        delete $ENV{PGAPPNAME};

        # Note: We set the cluster_name here, not in postgresql.conf (in
        # sub init) so that it does not get copied to standbys.
        $ret = TestLib::system_log('pg_ctl', '-D', $self->data_dir, '-l',
            $self->logfile, '-o', "--cluster-name=$name", '-w', 'start');
    }

    if ($ret != 0) {
        print "# pg_ctl start failed; logfile:\n";
        print TestLib::slurp_file($self->logfile);
        BAIL_OUT("pg_ctl start failed") unless $params{fail_ok};
        return 0;
    }

    $self->_update_pid(1);
    return 1;
}

# '--no-sync' doesn't exist in 9.6 and before
sub backup {
    my $self        = shift;
    my $backup_name = shift;
    my $backup_path = $self->backup_dir . '/' . $backup_name;
    my $name        = $self->name;

    print "# Taking pg_basebackup $backup_name from node \"$name\"\n";
    TestLib::system_or_bail(
        'pg_basebackup', '-D', $backup_path, '-h',
        $self->host,     '-p', $self->port,  '--checkpoint',
        'fast', '-x' );
    print "# Backup finished\n";
    return;
}

# need to explicitly allow replication in pg_hba.conf for 9.6 and before
sub set_replication_conf
{
    my ($self) = @_;
    my $pgdata = $self->data_dir;

    $self->is_default_host
        or die "set_replication_conf only works with the default host";

    open my $hba, ">>$pgdata/pg_hba.conf";
    print $hba "\n# Allow replication (set up by PostgresNode.pm)\n";
    if (!$TestLib::windows_os)
    {
        print $hba "local replication all trust\n";
    }
    else
    {
        my $test_localhost = $self->test_localhost;
        print $hba
"host replication all $test_localhost/32 sspi include_realm=1 map=regress\n";
    }
    close $hba;
}

# various function have been renamed after 9.6
sub lsn
{
    my ($self, $mode) = @_;
    my %modes = (
        'insert'  => 'pg_current_xlog_insert_location()',
        'flush'   => 'pg_current_xlog_flush_location()',
        'write'   => 'pg_current_xlog_location()',
        'receive' => 'pg_last_xlog_receive_location()',
        'replay'  => 'pg_last_xlog_replay_location()'
    );

    $mode = '<undef>' unless defined $mode;
    croak "unknown mode for 'lsn': '$mode', valid modes are "
        . join(', ', keys %modes)
        if !defined($modes{$mode});

    my $result = $self->safe_psql('postgres', "SELECT $modes{$mode}");
    chomp($result);
    if ($result eq '')
    {
        return;
    }
    else
    {
        return $result;
    }
}

# field renamed after 9.6

sub wait_for_catchup
{
    my ($self, $standby_name, $mode, $target_lsn) = @_;
    my $lsn_expr;
    my $query;
    my %valid_modes;

    $mode = defined($mode) ? $mode : 'replay';
    %valid_modes = (
        'sent'   => 1,
        'write'  => 1,
        'flush'  => 1,
        'replay' => 1
    );
    croak "unknown mode $mode for 'wait_for_catchup', valid modes are "
        . join(', ', keys(%valid_modes))
        unless exists($valid_modes{$mode});

    # Allow passing of a PostgresNode instance as shorthand
    if (blessed($standby_name) && $standby_name->isa("PostgresNode"))
    {
        $standby_name = $standby_name->name;
    }
    
    if (defined($target_lsn))
    {
        $lsn_expr = "'$target_lsn'";
    }
    else
    {
        $lsn_expr = 'pg_current_xlog_location()';
    }

    print "Waiting for replication conn ${standby_name}'s ${mode}_location "
        . "to pass $lsn_expr on " . $self->name . "\n";

    $query = qq{
        SELECT $lsn_expr <= ${mode}_location AND state = 'streaming'
        FROM pg_catalog.pg_stat_replication
        WHERE application_name = '$standby_name'
    };

    $self->poll_query_until('postgres', $query)
        or croak "timed out waiting for catchup";

    print "done\n";
    return;
}

1;
