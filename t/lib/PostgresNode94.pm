# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode94;

use strict;
use warnings;

use Test::More;
use Time::HiRes qw(usleep);
use parent 'PostgresNode95';

sub version { return 9.4 }

# * "cluster-name" not supported in 9.4 and before
# * pg_ctl -w does not wait for postmaster.pid to be ready
#   see: f13ea95f9e473
sub start
{
    my ($self, %params) = @_;
    my $port    = $self->port;
    my $pgdata  = $self->data_dir;
    my $name    = $self->name;
    my $pidfile = $self->data_dir . "/postmaster.pid";
    my $max_attempts = 30 * 10;
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
            $self->logfile, '-w', 'start');

        while ($max_attempts and not -f $pidfile) {
            $max_attempts--;
            # Wait 0.1 second before retrying.
            usleep(100_000);
        }
    }

    if ($ret != 0)
    {
        print "# pg_ctl start failed; logfile:\n";
        print TestLib::slurp_file($self->logfile);
        BAIL_OUT("pg_ctl start failed") unless $params{fail_ok};
        return 0;
    }

    if (not -f $pidfile)
    {
        print "# timeout while waiting for postmaster.pid; logfile:\n";
        print TestLib::slurp_file($self->logfile);
        BAIL_OUT("pg_ctl start failed") unless $params{fail_ok};
        return 0;
    }

    $self->_update_pid(1);
    return 1;
}

1;
