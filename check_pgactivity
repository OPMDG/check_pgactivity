#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

=head1 NAME

check_pgactivity - PostgreSQL plugin for Nagios

=head1 SYNOPSIS

  check_pgactivity {-w|--warning THRESHOLD} {-c|--critical THRESHOLD} [-s|--service SERVICE ] [-h|--host HOST] [-U|--username ROLE] [-p|--port PORT] [-d|--dbname DATABASE] [-S|--dbservice SERVICE_NAME] [-P|--psql PATH] [--debug] [--status-file FILE] [--path PATH] [-t|--timemout TIMEOUT]
  check_pgactivity [-l|--list]
  check_pgactivity [--help]

=head1 DESCRIPTION

check_pgactivity is designed to monitor PostgreSQL clusters from Nagios. It
offers many options to measure and monitor useful performance metrics.

=head1 COMPATIBILITY

Each service is available from a different PostgreSQL version,
from 7.4, as documented below.
The psql client must be 8.3 at least. It can be used with an older server.
Please report any undocumented incompatibility.

=cut

use vars qw($VERSION $PROGRAM);

use strict;
use warnings;
use 5.008;

use POSIX;
use Data::Dumper;
use File::Basename;
use File::Spec;
use File::Temp ();
use Getopt::Long qw(:config bundling no_ignore_case_always);
use List::Util qw(max);
use Pod::Usage;
use Scalar::Util qw(looks_like_number);
use Fcntl qw(:flock);
use Storable qw(lock_store lock_retrieve);

use Config;
use FindBin;

# messing with PATH so pod2usage always finds this script
my @path = split /$Config{'path_sep'}/ => $ENV{'PATH'};
push @path => $FindBin::Bin;
$ENV{'PATH'} = join $Config{'path_sep'} => @path;
undef @path;

# force the env in English
delete $ENV{'LC_ALL'};
$ENV{'LC_ALL'} = 'C';
setlocale( LC_ALL, 'C' );
delete $ENV{'LANG'};
delete $ENV{'LANGUAGE'};

$| = 1;

$VERSION = '2.5';
$PROGRAM = 'check_pgactivity';

my $PG_VERSION_MIN =  70400;
my $PG_VERSION_74  =  70400;
my $PG_VERSION_80  =  80000;
my $PG_VERSION_81  =  80100;
my $PG_VERSION_82  =  80200;
my $PG_VERSION_83  =  80300;
my $PG_VERSION_84  =  80400;
my $PG_VERSION_90  =  90000;
my $PG_VERSION_91  =  90100;
my $PG_VERSION_92  =  90200;
my $PG_VERSION_93  =  90300;
my $PG_VERSION_94  =  90400;
my $PG_VERSION_95  =  90500;
my $PG_VERSION_96  =  90600;
my $PG_VERSION_100 = 100000;
my $PG_VERSION_110 = 110000;
my $PG_VERSION_120 = 120000;
my $PG_VERSION_130 = 130000;

# reference to the output sub
my $output_fmt;

# Available services and descriptions.
#
# The referenced sub called to exec each service takes one parameter: a
# reference to the arguments hash (%args)
#
# Note that we cannot use Perl prototype for these subroutine as they are
# called indirectly (thus the args given by references).

my %services = (
    # 'service_name' => {
    #    'sub'     => sub reference to call to run this service
    #    'desc'    => 'a description of the service'
    # }

    'autovacuum' => {
        'sub'  => \&check_autovacuum,
        'desc' => 'Check the autovacuum activity.'
    },
    'backends' => {
        'sub'  => \&check_backends,
        'desc' => 'Number of connections, compared to max_connections.'
    },
    'backends_status' => {
        'sub'  => \&check_backends_status,
        'desc' => 'Number of connections in relation to their status.'
    },
    'checksum_errors' => {
        'sub'  => \&check_checksum_errors,
        'desc' => 'Check data checksums errors.'
    },
    'commit_ratio' => {
        'sub'  => \&check_commit_ratio,
        'desc' => 'Commit and rollback rate per second and commit ratio since last execution.'
    },
    'database_size' => {
        'sub'  => \&check_database_size,
        'desc' => 'Variation of database sizes.',
    },
    'extensions_versions' => {
        'sub'  => \&check_extensions_versions,
        'desc' => 'Check that installed extensions are up-to-date.'
    },
    'table_unlogged' => {
        'sub'  => \&check_table_unlogged,
        'desc' => 'Check unlogged tables'
    },
    'wal_files' => {
        'sub'  => \&check_wal_files,
        'desc' => 'Total number of WAL files.',
    },
    'archiver' => {
        'sub'  => \&check_archiver,
        'desc' => 'Check the archiver status and number of wal files ready to archive.',
    },
    'last_vacuum' => {
        'sub'  => \&check_last_vacuum,
        'desc' =>
            'Check the oldest vacuum (from autovacuum or not) on the database.',
    },
    'last_analyze' => {
        'sub'  => \&check_last_analyze,
        'desc' =>
            'Check the oldest analyze (from autovacuum or not) on the database.',
    },
    'locks' => {
        'sub'  => \&check_locks,
        'desc' => 'Check the number of locks on the hosts.'
    },
    'oldest_2pc' => {
        'sub'  => \&check_oldest_2pc,
        'desc' => 'Check the oldest two-phase commit transaction.'
    },
    'oldest_idlexact' => {
        'sub'  => \&check_oldest_idlexact,
        'desc' => 'Check the oldest idle transaction.'
    },
    'oldest_xmin' => {
        'sub'  => \&check_oldest_xmin,
        'desc' => 'Check the xmin horizon from distinct sources of xmin retention.'
    },
    'longest_query' => {
        'sub'  => \&check_longest_query,
        'desc' => 'Check the longest running query.'
    },
    'bgwriter' => {
        'sub'  => \&check_bgwriter,
        'desc' => 'Check the bgwriter activity.',
    },
    'archive_folder' => {
        'sub'  => \&check_archive_folder,
        'desc' => 'Check archives in given folder.',
    },
    'minor_version' => {
        'sub'  => \&check_minor_version,
        'desc' => 'Check if the PostgreSQL minor version is the latest one.',
    },
    'hot_standby_delta' => {
        'sub'  => \&check_hot_standby_delta,
        'desc' => 'Check delta in bytes between a master and its hot standbys.',
    },
    'streaming_delta' => {
        'sub'  => \&check_streaming_delta,
        'desc' => 'Check delta in bytes between a master and its standbys in streaming replication.',
    },
    'settings' => {
        'sub'  => \&check_settings,
        'desc' => 'Check if the configuration file changed.',
    },
    'hit_ratio' => {
        'sub'  => \&check_hit_ratio,
        'desc' => 'Check hit ratio on databases.'
    },
    'backup_label_age' => {
        'sub'  => \&check_backup_label_age,
        'desc' => 'Check age of backup_label file.',
    },
    'connection' => {
        'sub'  => \&check_connection,
        'desc' => 'Perform a simple connection test.'
    },
    'custom_query' => {
        'sub'  => \&check_custom_query,
        'desc' => 'Perform the given user query.'
    },
    'configuration' => {
        'sub'  => \&check_configuration,
        'desc' => 'Check the most important settings.',
    },
    'btree_bloat' => {
        'sub'  => \&check_btree_bloat,
        'desc' => 'Check B-tree index bloat.'
    },
    'max_freeze_age' => {
        'sub'  => \&check_max_freeze_age,
        'desc' => 'Check oldest database in transaction age.'
    },
        'invalid_indexes' => {
        'sub'  => \&check_invalid_indexes,
        'desc' => 'Check for invalid indexes.'
    },
    'is_master' => {
        'sub'  => \&check_is_master,
        'desc' => 'Check if cluster is in production.'
    },
    'is_hot_standby' => {
        'sub'  => \&check_is_hot_standby,
        'desc' => 'Check if cluster is a hot standby.'
    },
    'pga_version' => {
        'sub'  => \&check_pga_version,
        'desc' => 'Check the version of this check_pgactivity script.'
    },
    'is_replay_paused' => {
        'sub'  => \&check_is_replay_paused,
        'desc' => 'Check if the replication is paused.'
    },
    'table_bloat' => {
        'sub'  => \&check_table_bloat,
        'desc' => 'Check tables bloat.'
    },
    'temp_files' => {
        'sub'  => \&check_temp_files,
        'desc' => 'Check temp files generation.'
    },
    'replication_slots' => {
        'sub'  => \&check_replication_slots,
        'desc' => 'Check delta in bytes of the replication slots.'
    },
    'pg_dump_backup' => {
        'sub'  => \&check_pg_dump_backup,
        'desc' => 'Check pg_dump backups age and retention policy.'
    },
    'stat_snapshot_age' => {
        'sub'  => \&check_stat_snapshot_age,
        'desc' => 'Check stats collector\'s stats age.'
    },
    'sequences_exhausted' => {
        'sub'  => \&check_sequences_exhausted,
        'desc' => 'Check that auto-incremented colums aren\'t reaching their upper limit.'
    },
    'pgdata_permission' => {
        'sub'  => \&check_pgdata_permission,
        'desc' => 'Check that the permission on PGDATA is 700.'
    },
    'uptime' => {
        'sub'  => \&check_uptime,
        'desc' => 'Time since postmaster start or configurtion reload.'
    },
);


=over

=item B<-s>, B<--service> SERVICE

The Nagios service to run. See section SERVICES for a description of
available services or use C<--list> for a short service and description
list.

=item B<-h>, B<--host> HOST

Database server host or socket directory (default: $PGHOST or "localhost")

See section C<CONNECTIONS> for more informations.

=item B<-U>, B<--username> ROLE

Database user name (default: $PGUSER or "postgres").

See section C<CONNECTIONS> for more informations.

=item B<-p>, B<--port> PORT

Database server port (default: $PGPORT or "5432").

See section C<CONNECTIONS> for more informations.

=item B<-d>, B<--dbname> DATABASE

Database name to connect to (default: $PGDATABASE or "template1").

B<WARNING>! This is not necessarily one of the database that will be
checked. See C<--dbinclude> and C<--dbexclude> .

See section C<CONNECTIONS> for more informations.

=item B<-S>, B<--dbservice> SERVICE_NAME

The connection service name from pg_service.conf to use.

See section C<CONNECTIONS> for more informations.

=item B<--dbexclude> REGEXP

Some services automatically check all the databases of your
cluster (note: that does not mean they always need to connect on all
of them to check them though). C<--dbexclude> excludes any
database whose name matches the given Perl regular expression.
Repeat this option as many time as needed.

See C<--dbinclude> as well. If a database match both dbexclude and
dbinclude arguments, it is excluded.

=item B<--dbinclude> REGEXP

Some services automatically check all the databases of your
cluster (note: that does not imply that they always need to connect to all
of them though). Some always exclude the 'postgres'
database and templates. C<--dbinclude> checks B<ONLY>
databases whose names match the given Perl regular expression.
Repeat this option as many time as needed.

See C<--dbexclude> as well. If a database match both dbexclude and
dbinclude arguments, it is excluded.

=item B<-w>, B<--warning> THRESHOLD

The Warning threshold.

=item B<-c>, B<--critical> THRESHOLD

The Critical threshold.

=item B<-F>, B<--format> OUTPUT_FORMAT

The output format. Supported output are: C<binary>, C<debug>, C<human>,
C<nagios>, C<nagios_strict>, C<json> and C<json_strict>.

Using the C<binary> format, the results are written in a binary file (using
perl module C<Storable>) given in argument C<--output>. If no output is given,
defaults to file C<check_pgactivity.out> in the same directory as the script.

The C<nagios_strict> and C<json_strict> formats are equivalent to the C<nagios>
and C<json> formats respectively. The only difference is that they enforce the
units to follow the strict Nagios specs: B, c, s or %. Any unit absent from
this list is dropped (Bps, Tps, etc).

=item B<--tmpdir> DIRECTORY

Path to a directory where the script can create temporary files. The
script relies on the system default temporary directory if possible.

=item B<-P>, B<--psql> FILE

Path to the C<psql> executable (default: "psql").
It should be version 8.3 at least, but the server can be older.

=item B<--status-file> PATH

Path to the file where service status information is kept between
successive calls. Default is to save check_pgactivity.data in the same
directory as the script.

=item B<--dump-status-file>

Dump the content of the status file and exit. This is useful for debugging
purpose.

=item B<--dump-bin-file> [PATH]

Dump the content of the given binary file previously created using
C<--format binary>. If no path is given, defaults to file
C<check_pgactivity.out> in the same directory as the script.

=item B<-t>, B<--timeout> TIMEOUT

Timeout (default: "30s"), as raw (in seconds) or as
an interval. This timeout will be used as C<statement_timeout> for psql and URL
timeout for C<minor_version> service.

=item B<-l>, B<--list>

List available services.

=item B<-V>, B<--version>

Print version and exit.

=item B<--debug>

Print some debug messages.

=item B<-?>, B<--help>

Show this help page.

=back

=cut

my %args = (
    'service'               => undef,
    'host'                  => undef,
    'username'              => undef,
    'port'                  => undef,
    'dbname'                => undef,
    'dbservice'             => undef,
    'detailed'              => 0,
    'warning'               => undef,
    'critical'              => undef,
    'exclude'               => [],
    'dbexclude'             => [],
    'dbinclude'             => [],
    'tmpdir'                => File::Spec->tmpdir(),
    'psql'                  => undef,
    'path'                  => undef,
    'status-file'           => dirname(__FILE__) . '/check_pgactivity.data',
    'output'                => dirname(__FILE__) . '/check_pgactivity.out',
    'query'                 => undef,
    'type'                  => undef,
    'reverse'               => 0,
    'work_mem'              => undef,
    'maintenance_work_mem'  => undef,
    'shared_buffers'        => undef,
    'wal_buffers'           => undef,
    'checkpoint_segments'   => undef,
    'effective_cache_size'  => undef,
    'no_check_autovacuum'   => 0,
    'no_check_fsync'        => 0,
    'no_check_enable'       => 0,
    'no_check_track_counts' => 0,
    'ignore-wal-size'       => 0,
    'unarchiver'            => '',
    'save'                  => 0,
    'suffix'                => '',
    'slave'                 => [],
    'list'                  => 0,
    'help'                  => 0,
    'debug'                 => 0,
    'timeout'               => '30s',
    'dump-status-file'      => 0,
    'dump-bin-file'         => undef,
    'format'                => 'nagios',
    'uid'                   => undef
);

# Set name of the program without path*
my $orig_name = $0;
$0 = $PROGRAM;

# Die on kill -1, -2, -3 or -15
$SIG{'HUP'} = $SIG{'INT'} = $SIG{'QUIT'} = $SIG{'TERM'} = \&terminate;

# Handle SIG
sub terminate() {
    my ($signal) = @_;
    die ("SIG $signal caught");
}

# Print the version and exit
sub version() {
    printf "check_pgactivity version %s, Perl %vd\n",
        $VERSION, $^V;

    exit 0;
}

# List services that can be performed
sub list_services() {

    print "List of available services:\n\n";

    foreach my $service ( sort keys %services ) {
        printf "\t%-17s\t%s\n", $service, $services{$service}{'desc'};
    }

    exit 0;
}

# Check wrapper around Storable::file_magic to fallback on
# Storable::read_magic under perl 5.8 and below
sub is_storable($) {
    my $storage = shift;
    my $head;

    open my $fh, '<', $storage;
    flock($fh, LOCK_SH) or die "can't get shared lock on $storage: $!";

    if ( defined *Storable::file_magic{CODE}
         and Storable::file_magic( $storage )
    ) {
        close $fh; # release the shared lock
        return 1;
    }

    read $fh, $head, 64;
    close $fh;

    return defined Storable::read_magic($head);
}

# Find a unique string for the database instance connection.
# Used by save and load.
#
# Parameter: host structure ref that holds the "host" and "port" parameters
sub find_hostkey($) {
    my $host = shift;

    return "$host->{'host'}$host->{'port'}" if defined $host->{'host'}
                                           and defined $host->{'port'};
    return $host->{'dbservice'} if defined $host->{'dbservice'};
    return "binary defaults";
}

# Record the given ref content for the given host in a file on disk.
# The file is defined by argument "--status-file" on command line. By default:
#
#  dirname(__FILE__) . '/check_pgactivity.data'
#
# Format of data in this file is:
#   {
#     "${host}${port}" => {
#       "$name" => ref
#     }
#   }
# data can be retrieved later using the "load" sub.
#
# Parameters are :
#  * the host structure ref that holds the "host" and "port" parameters
#  * the name of the structure to save
#  * the ref of the structure to save
#  * the path to the file storage
sub save($$$$) {
    my $host    = shift;
    my $name    = shift;
    my $ref     = shift;
    my $storage = shift;
    my $all     = {};
    my $hostkey = find_hostkey($host);


    die "File «${storage}» not recognized as a check_pgactivity status file.\n\n"
        ."Please, check its path or move away this wrong file"
        if -r $storage and not is_storable $storage;

    $all = lock_retrieve($storage) if -r $storage;

    $all->{$hostkey}{$name} = $ref;

    eval { lock_store( $all, $storage ) };
    die "Can't store data in '$storage':\n  $@" if $@;
}

# Load the given ref content for the given host from the file on disk.
#
# See "save" sub comments for more info.
# Parameters are :
#  * the host structure ref that holds the "host" and "port" parameters
#  * the name of the structure to load
#  * the path to the file storage
sub load($$$) {
    my $host    = shift;
    my $name    = shift;
    my $storage = shift;
    my $hostkey = find_hostkey($host);
    my $all;

    return undef unless -e $storage;

    die "No permission to read status file «${storage}»" unless -r $storage;

    die "File «${storage}» not recognized as a check_pgactivity status file.\n\n"
        ."Please, check its path or move away this wrong file"
        unless is_storable $storage;

    eval { $all = lock_retrieve($storage) };
    die "Could not read status file «${storage}»:\n  $@" if $@;

    return $all->{$hostkey}{$name};
}

sub dump_status_file {
    my $f = shift;
    my $all;

    $f = $args{'status-file'} unless defined $f;
    $f = $args{'output'} unless $f ;

    $all = lock_retrieve($f);

    print Data::Dumper->new( [ $all ] )->Terse(1)->Dump;

    exit 0;
}

# Return formatted size string with units.
# Parameter: size in bytes
sub to_size($) {
    my $val  = shift;
    my @units = qw{B kB MB GB TB PB EB};
    my $size = '';
    my $mod = 0;
    my $i;

    return $val if $val =~ /^(-?inf)|(NaN$)/i;

    $val = int($val);

    for ( $i=0; $i < 6 and abs($val) > 1024; $i++ ) {
        $mod = $val%1024;
        $val = int( $val/1024 );
    }

    $val = "$val.$mod" unless $mod == 0;

    return "${val}$units[$i]";
}

# Return formatted time string with units.
# Parameter: duration in seconds
sub to_interval($) {
    my $val      = shift;
    my $interval = '';

    return $val if $val =~ /^-?inf/i;

    $val = int($val);

    if ( $val > 604800 ) {
        $interval = int( $val / 604800 ) . "w ";
        $val %= 604800;
    }

    if ( $val > 86400 ) {
        $interval .= int( $val / 86400 ) . "d ";
        $val %= 86400;
    }

    if ( $val > 3600 ) {
        $interval .= int( $val / 3600 ) . "h";
        $val %= 3600;
    }

    if ( $val > 60 ) {
        $interval .= int( $val / 60 ) . "m";
        $val %= 60;
    }

    $interval .= "${val}s" if $val > 0;

    return "${val}s" unless $interval; # return a value if $val <= 0

    return $interval;
}

=head2 THRESHOLDS

THRESHOLDS provided as warning and critical values can be raw numbers,
percentages, intervals or sizes. Each available service supports one or more
formats (eg. a size and a percentage).

=over

=item B<Percentage>

If THRESHOLD is a percentage, the value should end with a '%' (no space).
For instance: 95%.

=item B<Interval>

If THRESHOLD is an interval, the following units are accepted (not case
sensitive): s (second), m (minute), h (hour), d (day). You can use more than
one unit per given value. If not set, the last unit is in seconds.
For instance: "1h 55m 6" = "1h55m6s".

=cut


sub is_size($){
    my $str_size = lc( shift() );
    return 1 if $str_size =~ /^\s*[0-9]+([kmgtpez]?[bo]?)?\s*$/ ;
    return 0;
}


sub is_time($){
    my $str_time = lc( shift() );
    return 1 if ( $str_time
        =~ /^(\s*([0-9]\s*[smhd]?\s*))+$/
    );
    return 0;
}


# Return a duration in seconds from an interval (with units).
sub get_time($) {
    my $str_time = lc( shift() );
    my $ts       = 0;
    my @date;

    die(      "Malformed interval: «$str_time»!\n"
            . "Authorized unit are: dD, hH, mM, sS\n" )
        unless is_time($str_time);

    # no bad units should exist after this line!

    @date = split( /([smhd])/, $str_time );

LOOP_TS: while ( my $val = shift @date ) {

        $val = int($val);
        die("Wrong value for an interval: «$val»!") unless defined $val;

        my $unit = shift(@date) || '';

        if ( $unit eq 'm' ) {
            $ts += $val * 60;
            next LOOP_TS;
        }

        if ( $unit eq 'h' ) {
            $ts += $val * 3600;
            next LOOP_TS;
        }

        if ( $unit eq 'd' ) {
            $ts += $val * 86400;
            next LOOP_TS;
        }

        $ts += $val;
    }

    return $ts;
}

=pod

=item B<Size>

If THRESHOLD is a size, the following units are accepted (not case sensitive):
b (Byte), k (KB), m (MB), g (GB), t (TB), p (PB), e (EB) or Z (ZB). Only
integers are accepted. Eg. C<1.5MB> will be refused, use C<1500kB>.

The factor between units is 1024 bytes. Eg. C<1g = 1G = 1024*1024*1024.>

=back

=cut

# Return a size in bytes from a size with unit.
# If unit is '%', use the second parameter to compute the size in bytes.
sub get_size($;$) {
    my $str_size = shift;
    my $size     = 0;
    my $unit     = '';

    die "Only integers are accepted as size. Adjust the unit to your need."
        if $str_size =~ /[.,]/;

    $str_size =~ /^([0-9]+)(.*)$/;

    $size = int($1);
    $unit = lc($2);

    return $size unless $unit ne '';

    if ( $unit eq '%' ) {
        my $ratio = shift;

        die("Can not compute a ratio without the factor!")
            unless defined $unit;

        return int( $size * $ratio / 100 );
    }

    return $size           if $unit eq 'b';
    return $size * 1024    if $unit =~ '^k[bo]?$';
    return $size * 1024**2 if $unit =~ '^m[bo]?$';
    return $size * 1024**3 if $unit =~ '^g[bo]?$';
    return $size * 1024**4 if $unit =~ '^t[bo]?$';
    return $size * 1024**5 if $unit =~ '^p[bo]?$';
    return $size * 1024**6 if $unit =~ '^e[bo]?$';
    return $size * 1024**7 if $unit =~ '^z[bo]?$';

    die("Unknown size unit: $unit");
}


=head2 CONNECTIONS

check_pgactivity allows two different connection specifications: by service or
by specifying values for host, user, port, and database.
Some services can run on multiple hosts, or needs to connect to multiple hosts.

You might specify one of the parameters below to connect to your PostgreSQL
instance.  If you don't, no connection parameters are given to psql: connection
relies on binary defaults and environment.

The format for connection parameters is:

=over

=item B<Parameter> C<--dbservice SERVICE_NAME>

Define a new host using the given service. Multiple hosts can be defined by
listing multiple services separated by a comma. Eg.

  --dbservice service1,service2

For more information about service definition, see:
L<https://www.postgresql.org/docs/current/libpq-pgservice.html>

=item B<Parameters> C<--host HOST>, C<--port PORT>, C<--user ROLE> or C<--dbname DATABASE>

One parameter is enough to define a new host. Usual environment variables
(PGHOST, PGPORT, PGDATABASE, PGUSER, PGSERVICE, PGPASSWORD) or default values
are used for missing parameters.

As for usual PostgreSQL tools, there is no command line argument to set the
password, to avoid exposing it. Use PGPASSWORD, .pgpass or a service file
(recommended).

If multiple values are given, define as many host as maximum given values.

Values are associated by position. Eg.:

  --host h1,h2 --port 5432,5433

Means "host=h1 port=5432" and "host=h2 port=5433".

If the number of values is different between parameters, any host missing a
parameter will use the first given value for this parameter. Eg.:

  --host h1,h2 --port 5433

Means: "host=h1 port=5433" and "host=h2 port=5433".

=item B<Services are defined first>

For instance:

  --dbservice s1 --host h1 --port 5433

means: use "service=s1" and "host=h1 port=5433" in this order. If the service
supports only one host, the second host is ignored.

=item B<Mutual exclusion between both methods>

You can not overwrite services connections variables with parameters C<--host HOST>,
C<--port PORT>, C<--user ROLE> or C<--dbname DATABASE>

=back

=cut

sub parse_hosts(\%) {
    my %args = %{ shift() };
    my @hosts = ();

    if (defined $args{'dbservice'}) {
        push
            @hosts,
            {   'dbservice' => $_,
                'name'      => "service:$_",
                'pgversion' => undef
            }
        foreach split /,/, $args{'dbservice'};
    }


    # Add as many hosts than necessary depending on given parameters
    # host/port/db/user.
    # Any missing parameter will be set to its default value.
    if (defined $args{'host'}
        or defined $args{'username'}
        or defined $args{'port'}
        or defined $args{'dbname'}
    ) {
        $args{'host'} = $ENV{'PGHOST'} || 'localhost'
            unless defined $args{'host'};
        $args{'username'} = $ENV{'PGUSER'} || 'postgres'
            unless defined $args{'username'};
        $args{'port'} = $ENV{'PGPORT'} || '5432'
            unless defined $args{'port'};
        $args{'dbname'} = $ENV{'PGDATABASE'} || 'template1'
            unless defined $args{'dbname'};

        my @dbhosts = split( /,/, $args{'host'} );
        my @dbnames = split( /,/, $args{'dbname'} );
        my @dbusers = split( /,/, $args{'username'} );
        my @dbports = split( /,/, $args{'port'} );
        my $nbhosts = max $#dbhosts, $#dbnames, $#dbusers, $#dbports;

        # Take the first value for each connection property as default.
        # eg. "-h localhost -p 5432,5433" gives two hosts:
        #    * localhost:5432
        #    * localhost:5433
        for ( my $i = 0; $i <= $nbhosts; $i++ ) {
            push(
                @hosts,
                {   'host'      => $dbhosts[$i] || $dbhosts[0],
                    'port'      => $dbports[$i] || $dbports[0],
                    'db'        => $dbnames[$i] || $dbnames[0],
                    'user'      => $dbusers[$i] || $dbusers[0],
                    'pgversion' => undef
                }
            );

            $hosts[-1]{'name'} = sprintf('host:%s port:%d db:%s',
                $hosts[-1]{'host'}, $hosts[-1]{'port'}, $hosts[-1]{'db'}
            );
        }
    }

    if ( not @hosts ) {
        # No connection parameters given.
        # The psql execution relies on binary defaults and env variables.
        # We look for libpq environment variables to save them and preserve
        # default psql behaviour as query() always resets them.
        my $name = 'binary defaults';

        push @hosts, {
            'name'      => 'binary defaults',
            'pgversion' => undef
        };

        $hosts[0]{'host'}      = $ENV{'PGHOST'}     if defined $ENV{'PGHOST'};
        $hosts[0]{'port'}      = $ENV{'PGPORT'}     if defined $ENV{'PGPORT'};
        $hosts[0]{'db'}        = $ENV{'PGDATABASE'} if defined $ENV{'PGDATABASE'};
        $hosts[0]{'user'}      = $ENV{'PGUSER'}     if defined $ENV{'PGUSER'};
        $hosts[0]{'dbservice'} = $ENV{'PGSERVICE'}  if defined $ENV{'PGSERVICE'};

        if (defined $ENV{'PGHOST'} ) {
            $hosts[0]{'host'} = $ENV{'PGHOST'};
            $name .= " host:$ENV{'PGHOST'}";
        }

        if (defined $ENV{'PGPORT'} ) {
            $hosts[0]{'port'} = $ENV{'PGPORT'};
            $name .= " port:$ENV{'PGPORT'}";
        }

        if (defined $ENV{'PGDATABASE'} ) {
            $hosts[0]{'db'} = $ENV{'PGDATABASE'};
            $name .= " db:$ENV{'PGDATABASE'}";
        }

        if (defined $ENV{'PGSERVICE'} ) {
            $hosts[0]{'dbservice'} = $ENV{'PGSERVICE'};
            $name .= " service:$ENV{'PGSERVICE'}";
        }

        $hosts[0]{'user'} = $ENV{'PGUSER'} if defined $ENV{'PGUSER'};

        $hosts[0]{'name'} = $name;
    }

    dprint ('Hosts: '. Dumper(\@hosts));

    return \@hosts;
}



# Execute a query on a host.
# Params:
#   * host
#   * query
#   * (optional) database
#   * (optional) get_fields
# The result is an array of arrays:
#   [
#     [column1, ...] # line1
#     ...
#   ]
sub query($$;$$$) {
    my $host       = shift;
    my $query      = shift;
    my $db         = shift;
    my @res        = ();
    my $res        = '';
    my $RS         = chr(30); # ASCII RS  (record separator)
    my $FS         = chr(3);  # ASCII ETX (end of text)
    my $get_fields = shift;
    my $onfail     = shift || \&status_unknown;
    my $tmpfile;
    my $psqlcmd;
    my $rc;

    local $/ = undef;

    delete $ENV{PGSERVICE};
    delete $ENV{PGDATABASE};
    delete $ENV{PGHOST};
    delete $ENV{PGPORT};
    delete $ENV{PGUSER};
    delete $ENV{PGOPTIONS};

    $ENV{PGDATABASE} = $host->{'db'}        if defined $host->{'db'};
    $ENV{PGSERVICE}  = $host->{'dbservice'} if defined $host->{'dbservice'};
    $ENV{PGHOST}     = $host->{'host'}      if defined $host->{'host'};
    $ENV{PGPORT}     = $host->{'port'}      if defined $host->{'port'};
    $ENV{PGUSER}     = $host->{'user'}      if defined $host->{'user'};
    $ENV{PGOPTIONS}  = '-c client_encoding=utf8 -c client_min_messages=error -c statement_timeout=' . get_time($args{'timeout'}) * 1000;

    dprint ("Query: $query\n");
    dprint ("Env. service: $ENV{PGSERVICE} \n") if defined $host->{'dbservice'};
    dprint ("Env. host   : $ENV{PGHOST}    \n") if defined $host->{'host'};
    dprint ("Env. port   : $ENV{PGPORT}    \n") if defined $host->{'port'};
    dprint ("Env. user   : $ENV{PGUSER}    \n") if defined $host->{'user'};
    dprint ("Env. db     : $ENV{PGDATABASE}\n") if defined $host->{'db'};

    $tmpfile = File::Temp->new(
        TEMPLATE => 'check_pga-XXXXXXXX',
        DIR      => $args{'tmpdir'}
    ) or die "Could not create or write in a temp file!";

    print $tmpfile "$query;" or die "Could not create or write in a temp file!";

    $psqlcmd  = qq{ $args{'psql'} -w --set "ON_ERROR_STOP=1" }
              . qq{ -qXAf $tmpfile -R $RS -F $FS };
    $psqlcmd .= qq{ --dbname='$db' } if defined $db;
    $res      = qx{ $psqlcmd 2>&1 };
    $rc       = $?;

    dprint("Query rc: $rc\n");
    dprint( sprintf( "  stderr (%u): «%s»\n", length $res, $res ) )
        if $rc;

    exit $onfail->('CHECK_PGACTIVITY', [ "Query failed !\n" . $res ] ) unless $rc == 0;

    if (defined $res) {
        chop $res;
        my $col_num;

        push @res, [ split(chr(3) => $_, -1) ]
            foreach split (chr(30) => $res, -1);

        $col_num = scalar( @{ $res[0] } );

        shift @res unless defined $get_fields;
        pop @res if $res[-1][0] =~ m/^\(\d+ rows?\)$/;

        # check the number of column is valid.
        # FATAL if the parsing was unsuccessful, eg. if one field contains x30
        # or x03.  see gh issue #155
        foreach my $row ( @res ) {
            exit status_unknown('CHECK_PGACTIVITY',
                [ "Could not parse query result!\n" ]
            ) if scalar( @$row ) != $col_num;
        }
    }

    dprint( "Query result: ". Dumper( \@res ) );

    return \@res;
}

# Select the appropriate query among an hash of queries according to the
# backend version and execute it. Same argument order than in "query" sub.
# Hash of query must be of this form:
#   {
#     pg_version_num => $query1,
#     ...
#   }
#
# where pg_version_num is the minimum PostgreSQL version which can run the
# query. This version number is numeric. See "set_pgversion" about
# how to compute a PostgreSQL num version, or globals $PG_VERSION_*.
sub query_ver($\%;$) {
    my $host    = shift;
    my %queries = %{ shift() };

    # Shift returns undef if the db is not given. The value is then set in
    # "query" sub
    my $db = shift;

    set_pgversion($host);

    foreach my $ver ( sort { $b <=> $a } keys %queries ) {
        return query( $host, $queries{$ver}, $db )
            if ( $ver <= $host->{'version_num'} );
    }

    return undef;
}

# Return an array with all databases in given host but
# templates and "postgres".
# By default does not return templates and 'postgres' database
# except if the 2nd optional parameter non empty. Each service
# has to decide what suits it.
sub get_all_dbname($;$) {
    my @dbs;
    my $host = shift;
    my $cond = shift;
    my $query = 'SELECT datname FROM pg_database WHERE datallowconn ';

    $query .= q{ AND NOT datistemplate AND datname <> 'postgres' } if not defined $cond;
    $query .= ' ORDER BY datname';

    push @dbs => $_->[0] foreach (
        @{ query( $host, $query ) }
    );

    return \@dbs;
}

# Query and set the version for the given host
sub set_pgversion($) {
    my $host = shift;

    unless ( $host->{'version'} ) {

        my $rs = query( $host, q{SELECT setting FROM pg_catalog.pg_settings WHERE name IN ('server_version_num', 'server_version') ORDER BY name = 'server_version_num'} );

        if ( $? != 0 ) {
            dprint("FATAL: psql error, $!\n");
            exit 1;
        }

        $host->{'version'} = $rs->[0][0];

        chomp( $host->{'version'} );

        if ( scalar(@$rs) > 1 ) {
            # only use server_version_num for PostgreSQL 8.2+
            $host->{'version_num'} = $rs->[1][0];

            chomp( $host->{'version_num'} );
        }
        elsif ( $host->{'version'} =~ /^(\d+)\.(\d+)(.(\d+))?/ ) {
            # get back to the regexp handling for PostgreSQL <8.2
            $host->{'version_num'} = int($1) * 10000 + int($2) * 100;

            # alpha/beta version have no minor version number
            $host->{'version_num'} += int($4) if defined $4;
        }
        dprint(sprintf ("host %s is version %s/%s\n",
            $host->{'name'},
            $host->{'version'},
            $host->{'version_num'})
        );
        return;
    }

    return 1;
}

# Check host compatibility, with warning
sub is_compat($$$;$) {
    my $host    = shift;
    my $service = shift;
    my $min     = shift;
    my $max     = shift() || 9999999;;
    my $ver;

    set_pgversion($host);
    $ver = 100*int($host->{'version_num'}/100);

    unless (
        $ver >= $min
        and $ver <= $max
    ) {
        warn sprintf "Service %s is not compatible with host '%s' (v%s).\n",
            $service, $host->{'name'}, $host->{'version'};
        return 0;
    }

    return 1;
}

# Check host compatibility, without warning
sub check_compat($$;$) {
    my $host    = shift;
    my $min     = shift;
    my $max     = shift() || 9999999;
    my $ver;

    set_pgversion($host);
    $ver = 100*int($host->{'version_num'}/100);

    return 0 unless (
        $ver >= $min
        and $ver <= $max
    );

    return 1;
}

# check guc value
sub is_guc($$$) {
    my $host = shift;
    my $guc  = shift;
    my $val  = shift;
    my $ans;

    $ans = query( $host, "
        SELECT setting
        FROM pg_catalog.pg_settings
        WHERE name = '$guc'
    ");

    unless (exists $ans->[0][0]) {
        warn sprintf "Unknown GUC \"$guc\".";
        return 0;
    }

    dprint("GUC '$guc' value is '$ans->[0][0]', expected '$val'\n");

    unless ( $ans->[0][0] eq $val ) {
        warn sprintf "This service requires \"$guc=$val\".";
        return 0;
    }

    return 1;
}

sub dprint {
    return unless $args{'debug'};
    foreach (@_) {
        print "DEBUG: $_";
    }
}

sub status_unknown($;$$$) {
    return $output_fmt->( 3, $_[0], $_[1], $_[2], $_[3] );
}

sub status_critical($;$$$) {
    return $output_fmt->( 2, $_[0], $_[1], $_[2], $_[3] );
}

sub status_warning($;$$$) {
    return $output_fmt->( 1, $_[0], $_[1], $_[2], $_[3] );
}

sub status_ok($;$$$) {
    return $output_fmt->( 0, $_[0], $_[1], $_[2], $_[3] );
}

sub bin_output ($$;$$$) {
    my $rc      = shift;
    my $service = shift;
    my $all = {};
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    $all = lock_retrieve( $args{'output'} ) if -r $args{'output'};

    $all->{ $args{'service'} } = {
        'timestamp' => time,
        'rc'        => $rc,
        'service'   => $service,
        'messages'  => \@msg,
        'perfdata'  => \@perfdata,
        'longmsg'   => \@longmsg
    };

    lock_store( $all, $args{'output'} )
        or die "Can't store data in '$args{'output'}'!\n";
}

sub debug_output ($$;$$$) {
    my $rc      = shift;
    my $service = shift;
    my $ret;
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    $ret  = sprintf "%-15s: %s\n", 'Service', $service;

    $ret .= sprintf "%-15s: 0 (%s)\n", "Returns", "OK"       if $rc == 0;
    $ret .= sprintf "%-15s: 1 (%s)\n", "Returns", "WARNING"  if $rc == 1;
    $ret .= sprintf "%-15s: 2 (%s)\n", "Returns", "CRITICAL" if $rc == 2;
    $ret .= sprintf "%-15s: 3 (%s)\n", "Returns", "UNKNOWN"  if $rc == 3;

    $ret .= sprintf "%-15s: %s\n", "Message", $_ foreach @msg;
    $ret .= sprintf "%-15s: %s\n", "Long message", $_ foreach @longmsg;
    $ret .= sprintf "%-15s: %s\n", "Perfdata",
        Data::Dumper->new([ $_ ])->Indent(0)->Terse(1)->Dump foreach @perfdata;

    print $ret;

    return $rc;
}

sub human_output ($$;$$$) {
    my $rc      = shift;
    my $service = shift;
    my $ret;
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    $ret  = sprintf "%-15s: %s\n", 'Service', $service;

    $ret .= sprintf "%-15s: 0 (%s)\n", "Returns", "OK"       if $rc == 0;
    $ret .= sprintf "%-15s: 1 (%s)\n", "Returns", "WARNING"  if $rc == 1;
    $ret .= sprintf "%-15s: 2 (%s)\n", "Returns", "CRITICAL" if $rc == 2;
    $ret .= sprintf "%-15s: 3 (%s)\n", "Returns", "UNKNOWN"  if $rc == 3;

    $ret .= sprintf "%-15s: %s\n", "Message", $_ foreach @msg;
    $ret .= sprintf "%-15s: %s\n", "Long message", $_ foreach @longmsg;

    foreach my $perfdata ( @perfdata ) {
        map {$_ = undef unless defined $_} @{$perfdata}[2..6];

        if ( defined $$perfdata[2] and $$perfdata[2] =~ /B$/ ) {
            $ret .= sprintf "%-15s: %s=%s", "Perfdata",
                $$perfdata[0], to_size($$perfdata[1]);
            $ret .= sprintf " warn=%s", to_size( $$perfdata[3] ) if defined $$perfdata[3];
            $ret .= sprintf " crit=%s", to_size( $$perfdata[4] ) if defined $$perfdata[4];
            $ret .= sprintf " min=%s", to_size( $$perfdata[5] ) if defined $$perfdata[5];
            $ret .= sprintf " max=%s", to_size( $$perfdata[6] ) if defined $$perfdata[6];
            $ret .= "\n";
        }
        elsif ( defined $$perfdata[2] and $$perfdata[2] =~ /\ds$/ ) {
            $ret .= sprintf "%-15s: %s=%s", "Perfdata",
                $$perfdata[0], to_interval( $$perfdata[1] );
            $ret .= sprintf " warn=%s", to_interval( $$perfdata[3] ) if defined $$perfdata[3];
            $ret .= sprintf " crit=%s", to_interval( $$perfdata[4] ) if defined $$perfdata[4];
            $ret .= sprintf " min=%s", to_interval( $$perfdata[5] ) if defined $$perfdata[5];
            $ret .= sprintf " max=%s", to_interval( $$perfdata[6] ) if defined $$perfdata[6];
            $ret .= "\n";
        }
        else {
            $ret .= sprintf "%-15s: %s=%s", "Perfdata",
                $$perfdata[0], $$perfdata[1];
            $ret .= sprintf "%s", $$perfdata[2] if defined $$perfdata[2];
            $ret .= sprintf " warn=%s", $$perfdata[3] if defined $$perfdata[3];
            $ret .= sprintf " crit=%s", $$perfdata[4] if defined $$perfdata[4];
            $ret .= sprintf " min=%s", $$perfdata[5] if defined $$perfdata[5];
            $ret .= sprintf " max=%s", $$perfdata[6] if defined $$perfdata[6];
            $ret .= "\n";
        }
    }

    print $ret;

    return $rc;
}

sub nagios_output ($$;$$$) {
    my $rc  = shift;
    my $ret = shift;
    my @msg;
    my @perfdata;
    my @longmsg;

    $ret .= " OK"       if $rc == 0;
    $ret .= " WARNING"  if $rc == 1;
    $ret .= " CRITICAL" if $rc == 2;
    $ret .= " UNKNOWN"  if $rc == 3;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    $ret .= ": ".  join( ', ', @msg )     if @msg;
    if ( scalar @perfdata ) {
        $ret .= " |";
        foreach my $perf ( @perfdata ) {
            # escape quotes
            $$perf[0] =~ s/'/''/g;
            # surounding quotes if space in the label
            $$perf[0] = "'$$perf[0]'" if $$perf[0] =~ /\s/;
            # the perfdata itself and its unit
            $ret .= " $$perf[0]=$$perf[1]";

            # init and join optional values (unit/warn/crit/min/max)
            map {$_ = "" unless defined $_} @{$perf}[2..6];
            $ret .= join ';' => @$perf[2..6];

            # remove useless semi-colons at end
            $ret =~ s/;*$//;
        }
    }
    $ret .= "\n". join( ' ', @longmsg ) if @longmsg;

    print $ret;

    return $rc;
}

sub set_strict_perfdata {
    my $perfdata = shift;

    map {
        $$_[1] = 'U' if $$_[1] eq 'NaN';
        $$_[2] = ''  if exists $$_[2]
                     and defined $$_[2]
                     and $$_[2] !~ /\A[Bcs%]\z/;
    } @{ $perfdata };
}

sub nagios_strict_output ($$;$$$) {

    my $rc  = shift;
    my $ret = shift;
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    set_strict_perfdata ( \@perfdata );

    return nagios_output( $rc, $ret, \@msg, \@perfdata, \@longmsg );
}

sub json_output ($$;$$$) {
    my $rc  = shift;
    my $service = shift;
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    my $obj = {};
    $obj->{'service'} = $service;
    $obj->{'status'} = 'OK' if $rc == 0;
    $obj->{'status'} = 'WARNING' if $rc == 1;
    $obj->{'status'} = 'CRITICAL' if $rc == 2;
    $obj->{'status'} = 'UNKNOWN' if $rc == 3;
    $obj->{'msg'} = \@msg;
    $obj->{'longmsg'} = \@longmsg;

    my %data = map{ $$_[0] => {
            'val' => $$_[1],
            'unit' => $$_[2],
            'warn' => $$_[3],
            'crit' => $$_[4],
            'min' => $$_[5],
            'max' => $$_[6] }
    } @perfdata;
    $obj->{'perfdata'} = \%data;

    print encode_json( $obj );
    return $rc;
}

sub json_strict_output ($$;$$$) {

    my $rc  = shift;
    my $ret = shift;
    my @msg;
    my @perfdata;
    my @longmsg;

    @msg      = @{ $_[0] } if defined $_[0];
    @perfdata = @{ $_[1] } if defined $_[1];
    @longmsg  = @{ $_[2] } if defined $_[2];

    set_strict_perfdata ( \@perfdata );

    return json_output( $rc, $ret, \@msg, \@perfdata, \@longmsg );
}

=head2 SERVICES

Descriptions and parameters of available services.

=over


=item B<archive_folder>

Check if all archived WALs exist between the oldest and the latest WAL in the
archive folder and make sure they are 16MB. The given folder must have archived
files from ONE cluster. The version of PostgreSQL that created the archives is
only checked on the last one, for performance consideration.

This service requires the argument C<--path> on the command line to specify the
archive folder path to check. Obviously, it must have access to this
folder at the filesystem level: you may have to execute it on the archiving
server rather than on the PostgreSQL instance.

The optional argument C<--suffix> defines the suffix of your archived
WALs; this is useful for compressed WALs (eg. .gz, .bz2, ...).
Default is no suffix.

This service needs to read the header of one of the archives to define how many
segments a WAL owns. Check_pgactivity automatically handles files with
extensions .gz, .bz2, .xz, .zip or .7z using the following commands:

  gzip -dc
  bzip2 -dc
  xz -dc
  unzip -qqp
  7z x -so

If needed, provide your own command that writes the uncompressed file
to standard output with the C<--unarchiver> argument.

Optional argument C<--ignore-wal-size> skips the WAL size check. This is useful
if your archived WALs are compressed and check_pgactivity is unable to guess
the original size. Here are the commands check_pgactivity uses to guess the
original size of .gz, .xz or .zip files:

  gzip -ql
  xz -ql
  unzip -qql

Default behaviour is to check the WALs size.

Perfdata contains the number of archived WALs and the age of the most recent
one.

Critical and Warning define the max age of the latest archived WAL as an
interval (eg. 5m or 300s ).

Required privileges: unprivileged role; the system user needs read access
to archived WAL files.

Sample commands:

  check_pgactivity -s archive_folder --path /path/to/archives -w 15m -c 30m
  check_pgactivity -s archive_folder --path /path/to/archives --suffix .gz -w 15m -c 30m
  check_pgactivity -s archive_folder --path /path/to/archives --ignore-wal-size --suffix .bz2 -w 15m -c 30m
  check_pgactivity -s archive_folder --path /path/to/archives --unarchiver "unrar p" --ignore-wal-size --suffix .rar -w 15m -c 30m

=cut

sub check_archive_folder {
    my @msg;
    my @longmsg;
    my @msg_crit;
    my @msg_warn;
    my @perfdata;
    my @history_files;
    my @filelist;
    my @filelist_sorted;
    my @branch_wals;
    my $w_limit;
    my $c_limit;
    my $timeline;
    my $start_tl;
    my $end_tl;
    my $wal;
    my $seg;
    my $latest_wal_age;
    my $dh;
    my $fh;
    my $wal_version;
    my $filename_re;
    my $history_re;
    my $suffix         = $args{'suffix'};
    my $check_size     = not $args{'ignore-wal-size'};
    my $me             = 'POSTGRES_ARCHIVES';
    my $seg_per_wal    = 255; # increased later for pg > 9.2
    my %args           = %{ $_[0] };
    my %unarchive_cmd  = (
        '.gz'  => "gzip -dc",
        '.bz2' => "bzip2 -dc",
        '.xz'  => "xz -dc",
        '.zip' => "unzip -qqp",
        '.7z'  => "7z x -so"
    );
    my %wal_versions   = (
        '80' => 53340,
        '81' => 53341,
        '82' => 53342,
        '83' => 53346,
        '84' => 53347,
        '90' => 53348,
        '91' => 53350,
        '92' => 53361,
        '93' => 53365,
        '94' => 53374,
        '95' => 53383,
        '96' => 53395,  # 0xD093
        '100' => 53399, # 0xD097
        '110' => 53400, # 0xD098
        '120' => 53505, # 0xD101
        '130' => 53510  # 0xD106
    );

    # "path" argument must be given
    pod2usage(
        -message => 'FATAL: you must specify the archive folder using "--path <dir>".',
        -exitval => 127
    ) unless defined $args{'path'};

    # invalid "path" argument
    pod2usage(
        -message => "FATAL: \"$args{'path'}\" is not a valid folder.",
        -exitval => 127
    ) unless -d $args{'path'};

    # warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );


    opendir( $dh, $args{'path'} )
        or die "Cannot opendir $args{'path'} : $!\n";

    $filename_re = qr/^[0-9A-F]{24}$suffix$/;
    @filelist = map { [ $_ => (stat("$args{'path'}/$_"))[9,7] ] }
        grep( /$filename_re/, readdir($dh) );

    seekdir( $dh, 0 );

    $history_re = qr/^[0-9A-F]{8}.history$suffix$/;
    @history_files = grep /$history_re/, readdir($dh) ;

    closedir($dh);

    return status_unknown( $me, ['No archived WAL found.'] ) unless @filelist;


    $w_limit = get_time($args{'warning'});
    $c_limit = get_time($args{'critical'});

    # Sort by mtime
    @filelist_sorted = sort { ($a->[1] <=> $b->[1]) || ($a->[0] cmp $b->[0]) }
        grep{ (defined($_->[0]) and defined($_->[1]))
                or die "Cannot read WAL files"
        } @filelist;

    $latest_wal_age = time() - $filelist_sorted[-1][1];

    # Read the XLOG_PAGE_MAGIC header to guess $seg_per_wal

    if ( $args{'unarchiver'} eq '' and $suffix =~ /^.(?:gz|bz2|zip|xz|7z)$/ ) {
        open $fh, "-|",
            qq{ $unarchive_cmd{$suffix} "$args{'path'}/$filelist_sorted[-1][0]" 2>/dev/null }
            or
            die "could not read first WAL using '$unarchive_cmd{$suffix}': $!";
    }

    elsif ( $args{'unarchiver'} ne '' ) {
        open $fh, "-|",
            qq{ $args{'unarchiver'} "$args{'path'}/$filelist_sorted[-1][0]" 2>/dev/null }
            or die "could not read first WAL using '$args{'unarchiver'}': $!";
    }

    else {
        # Fallback on raw parsing of first WAL
        open $fh, "<", "$args{'path'}/$filelist_sorted[-1][0]"
            or die ("Could not read first WAL: $!\n");
    }

    read( $fh, $wal_version, 2 );
    close $fh;
    $wal_version = unpack('S', $wal_version);

    die ("Could not parse XLOG_PAGE_MAGIC") unless defined $wal_version;

    dprint ("wal version: $wal_version\n");

    die "Unknown WAL XLOG_PAGE_MAGIC $wal_version!"
        unless grep /^$wal_version$/ => values %wal_versions;

    # FIXME: As there is no consensus about XLOG_PAGE_MAGIC algo across
    # PostgreSQL versions this piece of code should be checked for
    # compatibility for each new PostgreSQL version to confirm the new
    # XLOG_PAGE_MAGIC is still greater than the previous one (or at least the
    # 9.2 one).
    $seg_per_wal++ if $wal_version >= $wal_versions{'93'};

    push @perfdata, [
        'latest_archive_age', $latest_wal_age, 's', $w_limit, $c_limit
    ];
    push @perfdata, [ 'num_archives', scalar(@filelist_sorted) ];

    dprint ("first wal: $filelist_sorted[0][0]\n");
    dprint ("last wal: $filelist_sorted[-1][0]\n");

    $start_tl = substr($filelist_sorted[0][0], 0, 8);
    $end_tl   = substr($filelist_sorted[-1][0], 0, 8);
    $timeline = hex($start_tl);
    $wal = hex(substr($filelist_sorted[0][0], 8, 8));
    $seg = hex(substr($filelist_sorted[0][0], 16, 8));

    # look for history files if timeline differs
    if ( $start_tl ne $end_tl ) {
        if ( -s "$args{'path'}/$end_tl.history" ) {
            open my $fd, "<", "$args{'path'}/$end_tl.history";
            while ( <$fd> ) {
                next unless m{^\s*(\d)\t([0-9A-F]+)/([0-9A-F]+)\t.*$};
                push @branch_wals =>
                    sprintf("%08d%08s%08X", $1, $2, hex($3)>>24);
            }
            close $fd;
        }
    }

    # Check ALL archives are here.
    for ( my $i=0, my $j=0; $i <= $#filelist_sorted ; $i++, $j++ ) {
        dprint ("Checking WAL $filelist_sorted[$i][0]\n");
        my $curr = sprintf('%08X%08X%08X%s',
            $timeline,
            $wal + int(($seg + $j)/$seg_per_wal),
            ($seg + $j)%$seg_per_wal,
            $suffix
        );

        if ( $curr ne $filelist_sorted[$i][0] ) {
            push @msg => "Wrong sequence or file missing @ '$curr'";
            last;
        }

        if ( $check_size ) {

            if ( $suffix eq '.gz' ) {
                my $ans = qx{ gzip -ql "$args{'path'}/$curr" 2>/dev/null };

                $filelist_sorted[$i][2] = 16777216
                    if $ans =~ /^\s*\d+\s+16777216\s/;
            }
            elsif ( $suffix eq '.xz' ) {
                my @ans = qx{ xz -ql --robot "$args{'path'}/$curr" 2>/dev/null };

                $filelist_sorted[$i][2] = 16777216
                    if $ans[-1] =~ /\w+\s+\d+\s+\d+\s+16777216\s+/;
            }
            elsif ( $suffix eq '.zip' ) {
                my $ans;
                $ans = qx{ unzip -qql "$args{'path'}/$curr" 2>/dev/null };
                $filelist_sorted[$i][2] = 16777216
                    if $ans =~ /^\s*16777216/;
            }

            if ( $filelist_sorted[$i][2] != 16777216 ) {
                push @msg => "'$curr' is not 16MB";
                last;
            }
        }

        if ( grep /$curr/, @branch_wals ) {
            dprint( "Found a boundary @ $curr !\n" );
            $timeline++;
            $j--;
        }
    }

    return status_critical( $me, \@msg, \@perfdata ) if @msg;

    push @msg => scalar(@filelist_sorted)." WAL archived in '$args{'path'}', "
        ."latest archived since ". to_interval($latest_wal_age);

    return status_critical( $me, \@msg, \@perfdata, \@longmsg )
        if $latest_wal_age >= $c_limit;

    return status_warning( $me, \@msg, \@perfdata, \@longmsg )
        if $latest_wal_age >= $w_limit;

    return status_ok( $me, \@msg, \@perfdata, \@longmsg );
}

=item B<archiver> (8.1+)

Check if the archiver is working properly and the number of WAL files ready to
archive.

Perfdata returns the number of WAL files waiting to be archived.

Critical and Warning thresholds are optional. They apply on the number of files
waiting to be archived. They only accept a raw number of files.

Whatever the given threshold, a critical alert is raised if the archiver
process did not archive the oldest waiting WAL to be archived since last call.

Required privileges: superuser (<v11), grant execute on function pg_stat_file(text) for v11+.

=cut

sub check_archiver {
    my @rs;
    my @perfdata;
    my @msg;
    my @longmsg;
    my @hosts;
    my $prev_archiving;
    my $nb_files;
    my %args  = %{ $_[0] };
    my $me    = 'POSTGRES_ARCHIVER';

    # warning and critical must be raw
    pod2usage(
        -message => "FATAL: critical and warning thresholds only accept raw numbers.",
        -exitval => 127
    ) if defined $args{'critical'} and $args{'warning'} !~ m/^([0-9]+)$/
        and  $args{'critical'} !~ m/^([0-9]+)$/;

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "archiver".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'archiver', $PG_VERSION_81 or exit 1;

    if (check_compat $hosts[0], $PG_VERSION_81, $PG_VERSION_96) {
        # cf. pgarch_readyXlog in src/backend/postmaster/pgarch.c about how the
        # archiver process pick the next WAL to archive.
        # We try to reproduce the same algo here
        my $query = q{
           SELECT s.f,
             extract(epoch from (pg_stat_file('pg_xlog/archive_status/'||s.f)).modification),
             extract(epoch from current_timestamp)
           FROM pg_ls_dir('pg_xlog/archive_status') AS s(f)
           WHERE f ~ '^[0123456789ABCDEF.history.backup.partial]{16,40}\.ready$'
           ORDER BY s.f ASC};


        $prev_archiving = load( $hosts[0], 'archiver', $args{'status-file'} ) || '';

        @rs = @{ query( $hosts[0], $query ) };

        $nb_files = scalar @rs;

        push @perfdata => [ 'ready_archive', $nb_files, undef, $args{'warning'}, $args{'critical'}, 0 ];

        if ( $nb_files > 0 ) {
            push @perfdata => [ 'oldest_ready_wal', int( $rs[0][2] - $rs[0][1] ), 's',
                                undef, undef, 0 ];

            if ( $rs[0][0] ne $prev_archiving ) {
                save $hosts[0], 'archiver', $rs[0][0], $args{'status-file'};
            }
            else {
                push @msg => sprintf 'archiver stalling', substr($rs[0][0], 0, 24);
                push @longmsg => sprintf '"%s" not archived since last check',
                                    substr($rs[0][0], 0, -6);
            }
        }
        else {
            push @perfdata => [ 'oldest_ready_wal', 0, 's', undef, undef, 0 ];
            save $hosts[0], 'archiver', '', $args{'status-file'};
        }

        push @msg => "$nb_files WAL files ready to archive";

    }
    else {
        # Version 10 and higher: use pg_stat_archiver
        # as the monitoring user may not be super-user.
        # FIXME: on a slave with archive_mode=always:
        #  1) fails while parsing an .history file
        #  2) pg_last_wal_receive_lsn always returns zero if the slave is fed
        #     with pure log shipping (streaming is ok)
        my $query = q{
        SELECT coalesce(pg_wal_lsn_diff(
                    current_pos,
                    /* compute LSN from last archived offset */
                    (to_hex(last_archived_off/4294967296)
                    ||'/'||to_hex(last_archived_off%4294967296))::pg_lsn
                )::bigint / walsegsize, 0),
                CASE WHEN failing
                THEN extract('epoch' from (current_timestamp - last_archived_time))
                ELSE 0
                END, last_archived_wal, last_failed_wal,
                /* mod time of the next wal to archive */
                extract('epoch' from (current_timestamp -
                    (pg_stat_file('pg_wal/'||pg_walfile_name(
                        (to_hex((last_archived_off+1)/4294967296)
                        ||'/'||to_hex((last_archived_off+1)%4294967296))::pg_lsn
                    ))).modification )
                ) AS oldest
        FROM (
            SELECT last_archived_wal, last_archived_time, last_failed_wal,
                walsegsize,
                /* compute last archive offset */
                -- WAL offset
                ('x'||substr(last_archived_wal, 9, 8))::bit(32)::bigint*4294967296
                    -- offset to the begining of the segment
                    + ('x'||substr(last_archived_wal, 17, 8))::bit(32)::bigint * walsegsize
                    -- offset to the end of the segment
                    + walsegsize AS last_archived_off,
                CASE WHEN pg_is_in_recovery()
                THEN pg_last_wal_receive_lsn()
                ELSE pg_current_wal_lsn()
                END AS current_pos,
                (last_failed_time >= last_archived_time)
                    OR (last_archived_time IS NULL AND last_failed_time IS NOT NULL)
                    AS failing
              FROM pg_stat_archiver, (
                SELECT setting::bigint *
                    CASE unit
                    WHEN '8kB' THEN 8192
                    WHEN 'B' THEN 1
                    ELSE 0
                    END as walsegsize
                FROM pg_catalog.pg_settings
                WHERE name = 'wal_segment_size'
              ) AS s

        ) stats
        };

        @rs = @{ query( $hosts[0], $query ) };

        $nb_files = $rs[0][0];

        push @perfdata => [ 'ready_archive', $nb_files, undef, $args{'warning'}, $args{'critical'}, 0 ];

        if ( $rs[0][1] > 0 ) {
            push @msg => sprintf 'archiver failing on %s', $rs[0][3];
            push @longmsg => sprintf '%s could not be archived since %ds',
                                $rs[0][3], $rs[0][1];
        }

        if ( $nb_files > 0 ) {
            push @perfdata => [ 'oldest_ready_wal', int( $rs[0][4] ), 's',
                                undef, undef, 0 ];
        }
        else {
            push @perfdata => [ 'oldest_ready_wal', 0, 's', undef, undef, 0 ];
        }

        push @msg => "$nb_files WAL files ready to archive";
    }

    return status_critical( $me, \@msg, \@perfdata, \@longmsg ) if scalar @msg > 1;

    if ( defined $args{'critical'} and $nb_files >= $args{'critical'} ) {
        return status_critical( $me, \@msg, \@perfdata );
    }
    elsif ( defined $args{'warning'} and $nb_files >= $args{'warning'} ) {
        return status_warning( $me, \@msg, \@perfdata );
    }

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<autovacuum> (8.1+)

Check the autovacuum activity on the cluster.

Perfdata contains the age of oldest running autovacuum and the number of
workers by type (VACUUM, VACUUM ANALYZE, ANALYZE, VACUUM FREEZE).

Thresholds, if any, are ignored.

Required privileges: unprivileged role.

=cut

sub check_autovacuum {

    my @rs;
    my @perfdata;
    my @msg;
    my @longmsg;
    my @hosts;
    my %args         = %{ $_[0] };
    my $me           = 'POSTGRES_AUTOVACUUM';
    my $oldest       = undef;
    my $numautovac  = 0;
    my $max_workers = "NaN";
    my %activity     = (
        'VACUUM'            => 0,
        'VACUUM_ANALYZE'    => 0,
        'ANALYZE'           => 0,
        'VACUUM_FREEZE'     => 0,
        'BRIN_SUMMARIZE'    => 0
    );

    my %queries      = (
        # field current_query, not autovacuum_max_workers
        $PG_VERSION_81 => q{
            SELECT current_query,
                extract(EPOCH FROM now()-query_start)::bigint,
                'NaN'
            FROM pg_stat_activity
            WHERE current_query LIKE 'autovacuum: %'
            ORDER BY query_start ASC
        },
        # field current_query, autovacuum_max_workers
        $PG_VERSION_83 => q{
            SELECT a.current_query,
                extract(EPOCH FROM now()-a.query_start)::bigint,
                s.setting
            FROM
                (SELECT current_setting('autovacuum_max_workers') AS setting) AS s
            LEFT JOIN (
                SELECT * FROM pg_stat_activity
                WHERE current_query LIKE 'autovacuum: %'
                ) AS a ON true
            ORDER BY query_start ASC
        },
        # field query, still autovacuum_max_workers
        $PG_VERSION_92 => q{
            SELECT a.query,
                extract(EPOCH FROM now()-a.query_start)::bigint,
                s.setting
            FROM
                (SELECT current_setting('autovacuum_max_workers') AS setting) AS s
            LEFT JOIN (
                SELECT * FROM pg_stat_activity
                WHERE query LIKE 'autovacuum: %'
                ) AS a ON true
            ORDER BY a.query_start ASC
        }
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "autovacuum".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'autovacuum', $PG_VERSION_81 or exit 1;

    if (check_compat $hosts[0], $PG_VERSION_81, $PG_VERSION_96) {
        delete $activity{BRIN_SUMMARIZE};
    }

    @rs = @{ query_ver( $hosts[0], %queries ) };

    REC_LOOP: foreach my $r (@rs) {
        if ( not defined $oldest ){
            $max_workers = $r->[2];
            next REC_LOOP if ( $r->[1] eq "" );
            $oldest = $r->[1];
        }
        $numautovac++;
        if ( $r->[0] =~ '\(to prevent wraparound\)$' ) {
            $activity{'VACUUM_FREEZE'}++;
        } else {
            if ( $r->[0] =~ '^autovacuum: VACUUM ANALYZE' ) {
                $activity{'VACUUM_ANALYZE'}++;
            } elsif ( $r->[0] =~ 'autovacuum: VACUUM' ) {
                $activity{'VACUUM'}++;
            } elsif ( $r->[0] =~ 'autovacuum: BRIN summarize' ) {
                $activity{'BRIN_SUMMARIZE'}++;
            } else {
                $activity{'ANALYZE'}++;
            };
        }
        $r->[0] =~ s/autovacuum: //;
        push @longmsg, $r->[0];
    }

    $oldest = 'NaN' if not defined ( $oldest );

    @perfdata = map { [ $_, $activity{$_}  ] } keys %activity;
    push @perfdata, [ 'oldest_autovacuum', $oldest, 's' ];
    push @perfdata, [ 'max_workers', $max_workers ];
    push @msg, "Number of autovacuum: $numautovac";
    push @msg, "Oldest autovacuum: " . to_interval($oldest) if $oldest ne "NaN";

    return status_ok( $me, \@msg , \@perfdata, \@longmsg );

}

=item B<backends> (all)

Check the total number of connections in the PostgreSQL cluster.

Perfdata contains the number of connections per database.

Critical and Warning thresholds accept either a raw number or a percentage (eg.
80%). When a threshold is a percentage, it is compared to the difference
between the cluster parameters C<max_connections> and
C<superuser_reserved_connections>.

Required privileges: an unprivileged user only sees its own queries;
a pg_monitor (10+) or superuser (<10) role is required to see all queries.

=cut

sub check_backends {

    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my %args         = %{ $_[0] };
    my $me           = 'POSTGRES_BACKENDS';
    my $num_backends = 0;
    my %queries      = (
        $PG_VERSION_MIN => q{
            SELECT s.datname, s.numbackends,
                current_setting('max_connections')::int
                    - current_setting('superuser_reserved_connections')::int
            FROM pg_catalog.pg_stat_database AS s
                JOIN pg_catalog.pg_database d ON d.oid = s.datid
            WHERE d.datallowconn },
        # Remove autovacuum connections (autovac introduced in 8.1, but exposed
        # in pg_stat_activity since 8.2)
        $PG_VERSION_82 => q{
            SELECT d.datname, count(*),
                current_setting('max_connections')::int
                    - current_setting('superuser_reserved_connections')::int
            FROM pg_catalog.pg_stat_activity AS s
              JOIN pg_catalog.pg_database AS d ON d.oid = s.datid
            WHERE current_query NOT LIKE 'autovacuum: %'
            GROUP BY d.datname },
        # Add replication connections 9.1
        $PG_VERSION_91 => q{
            SELECT s.*, current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
            FROM (
                SELECT d.datname, count(*)
                FROM pg_catalog.pg_stat_activity AS s
                  JOIN pg_catalog.pg_database AS d ON d.oid = s.datid
                WHERE current_query NOT LIKE 'autovacuum: %'
                GROUP BY d.datname
                UNION ALL
                SELECT 'replication', count(*)
                FROM pg_catalog.pg_stat_replication
            ) AS s },
        # Rename current_query => query
        $PG_VERSION_92 => q{
            SELECT s.*, current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
            FROM (
                SELECT d.datname, count(*)
                FROM pg_catalog.pg_stat_activity AS s
                  JOIN pg_catalog.pg_database AS d ON d.oid = s.datid
                WHERE query NOT LIKE 'autovacuum: %'
                GROUP BY d.datname
                UNION ALL
                SELECT 'replication', count(*)
                FROM pg_catalog.pg_stat_replication
            ) AS s },
        # Only account client backends
        $PG_VERSION_100 => q{
            SELECT s.*, current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
            FROM (
                SELECT d.datname, count(*)
                FROM pg_catalog.pg_stat_activity AS s
                  JOIN pg_catalog.pg_database AS d ON d.oid = s.datid
                WHERE backend_type = 'client backend'
                GROUP BY d.datname
                UNION ALL
                SELECT 'replication', count(*)
                FROM pg_catalog.pg_stat_replication
            ) AS s },
        $PG_VERSION_120 => q{
            SELECT s.*, current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
            FROM (
                SELECT d.datname, count(*)
                FROM pg_catalog.pg_stat_activity AS s
                  JOIN pg_catalog.pg_database AS d ON d.oid = s.datid
                WHERE backend_type = 'client backend'
                GROUP BY d.datname
            ) AS s }
    );

    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    # Warning and critical must be raw or %.
    pod2usage(
        -message => "FATAL: critical and warning thresholds only accept raw numbers or %.",
        -exitval => 127
    ) unless $args{'warning'}  =~ m/^([0-9.]+)%?$/
        and  $args{'critical'} =~ m/^([0-9.]+)%?$/;

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "backends".',
        -exitval => 127
    ) if @hosts != 1;


    @rs = @{ query_ver( $hosts[0], %queries ) };

    $args{'critical'} = int( $rs[0][2] * $1 / 100 )
        if $args{'critical'} =~ /^([0-9.]+)%$/;

    $args{'warning'} = int( $rs[0][2] * $1 / 100 )
        if $args{'warning'} =~ /^([0-9.]+)%$/;

    LOOP_DB: foreach my $db (@rs) {
        $num_backends += $db->[1];
        push @perfdata, [
            $db->[0], $db->[1], '', $args{'warning'}, $args{'critical'}, 0, $db->[2]
        ];
    }

    push @perfdata, [
        'maximum_connections', $rs[0][2], undef, undef, undef, 0, $rs[0][2]
    ];

    push @msg => "$num_backends connections on $rs[0][2]";

    return status_critical( $me, \@msg, \@perfdata )
        if $num_backends >= $args{'critical'};

    return status_warning( $me, \@msg, \@perfdata )
        if $num_backends >= $args{'warning'};

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<backends_status> (8.2+)

Check the status of all backends. Depending on your PostgreSQL version,
statuses are: C<idle>, C<idle in transaction>, C<idle in transaction (aborted)>
(>=9.0 only), C<fastpath function call>, C<active>, C<waiting for lock>,
C<undefined>, C<disabled> and C<insufficient privilege>.
B<insufficient privilege> appears when you are not allowed to see the statuses
of other connections.

This service supports the argument C<--exclude REGEX> to exclude queries
matching the given regular expression.

You can use multiple C<--exclude REGEX> arguments.

Critical and Warning thresholds are optional. They accept a list of
'status_label=value' separated by a comma. Available labels are C<idle>,
C<idle_xact>, C<aborted_xact>, C<fastpath>, C<active> and C<waiting>. Values
are raw numbers or time units and empty lists are forbidden. Here is an example:

    -w 'waiting=5,idle_xact=10' -c 'waiting=20,idle_xact=30,active=1d'

Perfdata contains the number of backends for each status and the oldest one for
each of them, for 8.2+.

Note that the number of backends reported in Nagios message B<includes>
excluded backends.

Required privileges: an unprivileged user only sees its own queries;
a pg_monitor (10+) or superuser (<10) role is required to see all queries.

=cut

sub check_backends_status {
    my @rs;
    my @hosts;
    my @perfdata;
    my @msg_warn;
    my @msg_crit;
    my %warn;
    my %crit;
    my $max_connections;
    my $num_backends = 0;
    my $me           = 'POSTGRES_BACKENDS_STATUS';
    my %status       = (
        'idle'                          => [0, 0],
        'idle in transaction'           => [0, 0],
        'idle in transaction (aborted)' => [0, 0],
        'fastpath function call'        => [0, 0],
        'waiting for lock'              => [0, 0],
        'active'                        => [0, 0],
        'disabled'                      => [0, 0],
        'undefined'                     => [0, 0],
        'insufficient privilege'        => [0, 0],
        'other wait event'              => [0, 0]
    );
    my %translate    = (
        'idle'         => 'idle',
        'idle_xact'    => 'idle in transaction',
        'aborted_xact' => 'idle in transaction (aborted)',
        'fastpath'     => 'fastpath function call',
        'waiting'      => 'waiting for lock',
        'active'       => 'active'
    );
    my %queries      = (
        # Doesn't support "idle in transaction (aborted)" and xact age
        $PG_VERSION_82 => q{
            SELECT CASE
                    WHEN s.current_query = '<IDLE>'
                        THEN 'idle'
                    WHEN s.current_query = '<IDLE> in transaction'
                        THEN 'idle in transaction'
                    WHEN s.current_query = '<FASTPATH> function call'
                        THEN 'fastpath function call'
                    WHEN s.current_query = '<command string not enabled>'
                        THEN 'disabled'
                    WHEN s.current_query = '<backend information not available>'
                        THEN 'undefined'
                    WHEN s.current_query = '<insufficient privilege>'
                        THEN 'insufficient privilege'
                    WHEN s.waiting = 't'
                        THEN 'waiting for lock'
                    ELSE 'active'
                END AS status,
                NULL, current_setting('max_connections'), s.current_query
            FROM pg_stat_activity AS s
                JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
        },
        # Doesn't support "idle in transaction (aborted)"
        $PG_VERSION_83 => q{
            SELECT CASE
                    WHEN s.current_query = '<IDLE>'
                        THEN 'idle'
                    WHEN s.current_query = '<IDLE> in transaction'
                        THEN 'idle in transaction'
                    WHEN s.current_query = '<FASTPATH> function call'
                        THEN 'fastpath function call'
                    WHEN s.current_query = '<command string not enabled>'
                        THEN 'disabled'
                    WHEN s.current_query = '<backend information not available>'
                        THEN 'undefined'
                    WHEN s.current_query = '<insufficient privilege>'
                        THEN 'insufficient privilege'
                    WHEN s.waiting = 't'
                        THEN 'waiting for lock'
                    ELSE 'active'
                END AS status,
                extract('epoch' FROM
                    date_trunc('milliseconds', current_timestamp-s.xact_start)
                ),
                current_setting('max_connections'), s.current_query
            FROM pg_stat_activity AS s
                JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
        },
        # Supports everything
        $PG_VERSION_90 => q{
            SELECT CASE
                    WHEN s.current_query = '<IDLE>'
                        THEN 'idle'
                    WHEN s.current_query = '<IDLE> in transaction'
                        THEN 'idle in transaction'
                    WHEN s.current_query = '<IDLE> in transaction (aborted)'
                        THEN 'idle in transaction (aborted)'
                    WHEN s.current_query = '<FASTPATH> function call'
                        THEN 'fastpath function call'
                    WHEN s.current_query = '<command string not enabled>'
                        THEN 'disabled'
                    WHEN s.current_query = '<backend information not available>'
                        THEN 'undefined'
                    WHEN s.current_query = '<insufficient privilege>'
                        THEN 'insufficient privilege'
                    WHEN s.waiting = 't'
                        THEN 'waiting for lock'
                    ELSE 'active'
                END,
                extract('epoch' FROM
                    date_trunc('milliseconds', current_timestamp-s.xact_start)
                ),
                current_setting('max_connections'), s.current_query
            FROM pg_stat_activity AS s
                JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
        },
        # pg_stat_activity schema change
        $PG_VERSION_92 => q{
            SELECT CASE
                WHEN s.waiting = 't' THEN 'waiting for lock'
                WHEN s.query = '<insufficient privilege>'
                    THEN 'insufficient privilege'
                WHEN s.state IS NULL THEN 'undefined'
                ELSE s.state
              END,
              extract('epoch' FROM
                date_trunc('milliseconds', current_timestamp-s.xact_start)
              ), current_setting('max_connections'), s.query
            FROM pg_stat_activity AS s
              JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
        },
        # pg_stat_activity schema change for wait events
        $PG_VERSION_96 => q{
            SELECT CASE
                WHEN s.wait_event_type = 'Lock' THEN 'waiting for lock'
                WHEN s.wait_event_type IS NOT NULL THEN 'other wait event'
                WHEN s.query = '<insufficient privilege>'
                    THEN 'insufficient privilege'
                WHEN s.state IS NULL THEN 'undefined'
                ELSE s.state
              END,
              extract('epoch' FROM
                date_trunc('milliseconds', current_timestamp-s.xact_start)
              ), current_setting('max_connections'), s.query
            FROM pg_stat_activity AS s
              JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
        },
        # pg_stat_activity now displays background processes
        $PG_VERSION_100 => q{
            SELECT CASE
                WHEN s.wait_event_type = 'Lock' THEN 'waiting for lock'
                WHEN s.query = '<insufficient privilege>'
                    THEN 'insufficient privilege'
                WHEN s.state IS NULL THEN 'undefined'
                WHEN s.wait_event_type IS NOT NULL
                       AND s.wait_event_type NOT IN ('Client', 'Activity')
                     THEN 'other wait event'
                ELSE s.state
              END,
              extract('epoch' FROM
                date_trunc('milliseconds', current_timestamp-s.xact_start)
              ), current_setting('max_connections'), s.query
            FROM pg_stat_activity AS s
              JOIN pg_database d ON d.oid=s.datid
            WHERE d.datallowconn
              AND backend_type IN ('client backend', 'background worker')
        }
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "backends_status".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'backends_status', $PG_VERSION_82 or exit 1;


    if ( defined $args{'warning'} ) {
        my $threshods_re
            = qr/(idle|idle_xact|aborted_xact|fastpath|active|waiting)\s*=\s*(\d+\s*[smhd]?)/i;

        # Warning and critical must be raw
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept a list of 'label=value' separated by comma.\n"
                . "See documentation for more information.",
            -exitval => 127
        ) unless $args{'warning'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/
            and $args{'critical'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/ ;

        while ( $args{'warning'} =~ /$threshods_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $warn{$translate{$threshold}} = $value if $1 and defined $2;
        }

        while ( $args{'critical'} =~ /$threshods_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $crit{$translate{$threshold}} = $value if $1 and defined $2;
        }
    }

    @rs = @{ query_ver( $hosts[0], %queries ) };

    delete $status{'idle in transaction (aborted)'}
        if $hosts[0]->{'version_num'} < $PG_VERSION_90;

    delete $status{'other wait event'}
        if $hosts[0]->{'version_num'} < $PG_VERSION_96;

    $max_connections = $rs[0][2] if scalar @rs;

    REC_LOOP: foreach my $r (@rs) {

        $num_backends++;

        foreach my $exclude_re ( @{ $args{'exclude'} } ) {
            next REC_LOOP if $r->[3] =~ /$exclude_re/;
        }

        if (exists $status{$r->[0]}) {
          $status{$r->[0]}[0]++;

          $status{$r->[0]}[1] = $r->[1]
              if $r->[1] and $r->[1] > $status{$r->[0]}[1];
        }
    }

    STATUS_LOOP: foreach my $s (sort keys %status) {
        my @perf = ( $s, $status{$s}[0], undef );

        push @perf, ( $warn{$s}, $crit{$s}, 0, $max_connections )
          if ( exists $warn{$s} and exists $crit{$s}
               and $warn{$s} =~ /\d+$/ and $crit{$s} =~ /\d+$/ );
        push @perfdata => [ @perf ];

        if ( $hosts[0]->{'version_num'} >= $PG_VERSION_83
             and $s !~ '^(?:disabled|undefined|insufficient)' ) {
          my @perf = ("oldest $s", $status{$s}[1], 's' );
          push @perf, ( $warn{$s}, $crit{$s}, 0, $max_connections )
            if ( exists $warn{$s} and exists $crit{$s}
                 and $warn{$s} =~ /\d+\s*[smhd]/ and $crit{$s} =~ /\d+\s*[smhd]/ );
          push @perfdata => [ @perf ];
        }

        # Criticals
        if ( exists $crit{$s} ) {
            if ( $crit{$s} =~ /\d+\s*[smhd]/ ) {
              if ( $status{$s}[1] >= get_time($crit{$s}) ) {
                push @msg_crit => "$status{$s}[0] $s for $status{$s}[1] seconds";
                next STATUS_LOOP;
              }
        }
            elsif ( $status{$s}[0] >= $crit{$s} ) {
              push @msg_crit => "$status{$s}[0] $s";
              next STATUS_LOOP;
        }
        }

        # Warning
        if ( exists $warn{$s} ) {
            if ( $warn{$s} =~ /\d+\s*[smhd]/ ) {
              if ( $status{$s}[1] >= get_time($warn{$s}) ) {
                push @msg_warn => "$status{$s}[0] $s for $status{$s}[1] seconds";
                next STATUS_LOOP;
              }
        }
            elsif ( $status{$s}[0] >= $warn{$s} ) {
              push @msg_warn => "$status{$s}[0] $s";
              next STATUS_LOOP;
            }
        }
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if scalar @msg_warn > 0;

    return status_ok( $me, [ "$num_backends backend connected" ], \@perfdata );
}


=item B<checksum_errors> (12+)

Check for data checksums error, reported in pg_stat_database.

This service requires that data checksums are enabled on the target instance.
UNKNOWN will be returned if that's not the case.

Critical and Warning thresholds are optional. They only accept a raw number of
checksums errors per database.  If the thresholds are not provided, a default
value of `1` will be used for both thresholds.

Checksums errors are CRITICAL issues, so it's highly recommended to keep
default threshold, as immediate action should be taken as soon as such a
problem arises.

Perfdata contains the number of error per database.

Required privileges: unprivileged user.

=cut

sub check_checksum_errors {
    my @msg_crit;
    my @msg_warn;
    my @rs;
    my @perfdata;
    my @hosts;
    my %args       = %{ $_[0] };
    my $me         = 'POSTGRES_CHECKSUM_ERRORS';
    my $db_checked = 0;
    my $sql        = q{SELECT COALESCE(s.datname, '<shared objects>'),
        checksum_failures
        FROM pg_catalog.pg_stat_database s};
    my $w_limit;
    my $c_limit;

    # Warning and critical are optional
    pod2usage(
        -message => "FATAL: you must specify both critical and warning thresholds.",
        -exitval => 127
    ) if ((defined $args{'warning'} and not defined $args{'critical'})
      or (not defined $args{'warning'} and defined $args{'critical'})) ;

    # Warning and critical default to 1
    if (not defined $args{'warning'} or not defined $args{'critical'}) {
        $w_limit = $c_limit = 1;
    } else {
        $w_limit = $args{'warning'};
        $c_limit = $args{'critical'};
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "database_size".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'checksum_error', $PG_VERSION_120 or exit 1;

    # Check if data checksums are enabled
    @rs = @{ query( $hosts[0], "SELECT pg_catalog.current_setting('data_checksums')" ) };

    return status_unknown( $me, ['Data checksums are not enabled!'] )
        unless ($rs[0][0] eq "on");

    @rs = @{ query( $hosts[0], $sql ) };

    DB_LOOP: foreach my $db (@rs) {
        $db_checked++;

        push @perfdata => [ $db->[0], $db->[1], '', $w_limit, $c_limit ];

        if ( $db->[1] >= $c_limit ) {
            push @msg_crit => "$db->[0]: $db->[1] error(s)";
            next DB_LOOP;
        }

        if ( $db->[1] >= $w_limit ) {
            push @msg_warn => "$db->[0]: $db->[1] error(s)";
            next DB_LOOP;
        }
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if scalar @msg_warn > 0;

    return status_ok( $me, [ "$db_checked database(s) checked" ], \@perfdata );
}

=item B<backup_label_age> (8.1+)

Check the age of the backup label file.

Perfdata returns the age of the backup_label file, -1 if not present.

Critical and Warning thresholds only accept an interval (eg. 1h30m25s).

Required privileges: unprivileged role (9.3+); superuser (<9.3)

=cut

sub check_backup_label_age {
    my $rs;
    my $c_limit;
    my $w_limit;
    my @perfdata;
    my @hosts;
    my %args    = %{ $_[0] };
    my $me      = 'POSTGRES_BACKUP_LABEL_AGE';
    my %queries         = (
        $PG_VERSION_81 => q{SELECT max(s.r) AS value FROM (
            SELECT CAST(extract(epoch FROM current_timestamp - (pg_stat_file(file)).modification) AS integer) AS r
            FROM pg_ls_dir('.') AS ls(file)
            WHERE file='backup_label' UNION SELECT 0
        ) AS s},
        $PG_VERSION_93 => q{
            SELECT CASE WHEN pg_is_in_backup()
                        THEN CAST(extract(epoch FROM current_timestamp - pg_backup_start_time()) AS integer)
                        ELSE 0
                   END}
    );

    # warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );

    $c_limit = get_time( $args{'critical'} );
    $w_limit = get_time( $args{'warning'} );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "backup_label_age".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'backup_label_age', $PG_VERSION_81 or exit 1;

    $rs = @{ query_ver( $hosts[0], %queries )->[0] }[0];

    push @perfdata, [ 'age', $rs, 's', $w_limit, $c_limit ];

    return status_critical( $me, [ "age: ".to_interval($rs) ], \@perfdata )
        if $rs > $c_limit;

    return status_warning( $me, [ "age: ".to_interval($rs) ], \@perfdata )
        if $rs > $w_limit;

    return status_ok( $me, [ "backup_label file ".( $rs == 0 ? "absent":"present (age: ".to_interval($rs).")") ], \@perfdata );
}


=item B<bgwriter> (8.3+)

Check the percentage of pages written by backends since last check.

This service uses the status file (see C<--status-file> parameter).

Perfdata contains the ratio per second for each C<pg_stat_bgwriter> counter
since last execution. Units Nps for checkpoints, max written clean and fsyncs
are the number of "events" per second.

Critical and Warning thresholds are optional. If set, they I<only> accept a
percentage.

Required privileges: unprivileged role.

=cut

sub check_bgwriter {
    my @msg;
    my @msg_crit;
    my @msg_warn;
    my @rs;
    my @perfdata;
    my $delta_ts;
    my $delta_buff_total;
    my $delta_buff_backend;
    my $delta_buff_bgwriter;
    my $delta_buff_checkpointer;
    my $delta_buff_alloc;
    my $delta_checkpoint_timed;
    my $delta_checkpoint_req;
    my $delta_maxwritten_clean;
    my $delta_backend_fsync;
    my %new_bgw;
    my %bgw;
    my @hosts;
    my $now     = time();
    my %args    = %{ $_[0] };
    my $me      = 'POSTGRES_BGWRITER';
    my %queries = (
        $PG_VERSION_83 => q{SELECT checkpoints_timed, checkpoints_req,
              buffers_checkpoint * current_setting('block_size')::numeric,
              buffers_clean * current_setting('block_size')::numeric,
              maxwritten_clean,
              buffers_backend * current_setting('block_size')::numeric,
              buffers_alloc * current_setting('block_size')::numeric,
              0,
              0
            FROM pg_stat_bgwriter;
        },
        $PG_VERSION_91 => q{SELECT checkpoints_timed, checkpoints_req,
              buffers_checkpoint * current_setting('block_size')::numeric,
              buffers_clean * current_setting('block_size')::numeric,
              maxwritten_clean,
              buffers_backend * current_setting('block_size')::numeric,
              buffers_alloc * current_setting('block_size')::numeric,
              buffers_backend_fsync,
              extract ('epoch' from stats_reset)
            FROM pg_stat_bgwriter;
        }
    );

    # Warning and critical must be %.
    pod2usage(
        -message => "FATAL: critical and warning thresholds only accept percentages.",
        -exitval => 127
    ) unless not (defined $args{'warning'} and defined $args{'critical'} )
        or (
            $args{'warning'}  =~ m/^([0-9.]+)%$/
            and $args{'critical'} =~ m/^([0-9.]+)%$/
        );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "bgwriter".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'bgwriter', $PG_VERSION_83 or exit 1;


    %bgw = %{ load( $hosts[0], 'bgwriter', $args{'status-file'} ) || {} };

    @rs = @{ query_ver( $hosts[0], %queries )->[0] };

    $new_bgw{'ts'}               = $now;
    $new_bgw{'checkpoint_timed'} = $rs[0];
    $new_bgw{'checkpoint_req'}   = $rs[1];
    $new_bgw{'buff_checkpoint'}  = $rs[2];
    $new_bgw{'buff_clean'}       = $rs[3];
    $new_bgw{'maxwritten_clean'} = $rs[4];
    $new_bgw{'buff_backend'}     = $rs[5];
    $new_bgw{'buff_alloc'}       = $rs[6];
    $new_bgw{'backend_fsync'}    = $rs[7];
    $new_bgw{'stat_reset'}       = $rs[8];

    save $hosts[0], 'bgwriter', \%new_bgw, $args{'status-file'};

    return status_ok( $me, ['First call'] ) unless keys %bgw
        and defined $bgw{'ts'}; # 'ts' was added in 1.25, check for existence
                                # instead of raising some ugly Perl errors
                                # when upgrading.

    return status_ok( $me, ['Stats reseted since last call'] )
        if $new_bgw{'stat_reset'}       > $bgw{'stat_reset'}
        or $new_bgw{'checkpoint_timed'} < $bgw{'checkpoint_timed'}
        or $new_bgw{'checkpoint_req'}   < $bgw{'checkpoint_req'}
        or $new_bgw{'buff_checkpoint'}  < $bgw{'buff_checkpoint'}
        or $new_bgw{'buff_clean'}       < $bgw{'buff_clean'}
        or $new_bgw{'maxwritten_clean'} < $bgw{'maxwritten_clean'}
        or $new_bgw{'buff_backend'}     < $bgw{'buff_backend'}
        or $new_bgw{'buff_alloc'}       < $bgw{'buff_alloc'}
        or $new_bgw{'backend_fsync'}    < $bgw{'backend_fsync'};

    $delta_buff_total = $rs[2] - $bgw{'buff_checkpoint'}
        + $rs[3] - $bgw{'buff_clean'}
        + $rs[5] - $bgw{'buff_backend'};

    $delta_ts                = $now   - $bgw{'ts'};
    $delta_buff_backend      = ($rs[5] - $bgw{'buff_backend'})     / $delta_ts;
    $delta_buff_bgwriter     = ($rs[3] - $bgw{'buff_clean'})       / $delta_ts;
    $delta_buff_checkpointer = ($rs[2] - $bgw{'buff_checkpoint'})  / $delta_ts;
    $delta_buff_alloc        = ($rs[6] - $bgw{'buff_alloc'})       / $delta_ts;
    $delta_checkpoint_timed  = ($rs[0] - $bgw{'checkpoint_timed'}) / $delta_ts;
    $delta_checkpoint_req    = ($rs[1] - $bgw{'checkpoint_req'})   / $delta_ts;
    $delta_maxwritten_clean  = ($rs[4] - $bgw{'maxwritten_clean'}) / $delta_ts;
    $delta_backend_fsync     = ($rs[7] - $bgw{'backend_fsync'})    / $delta_ts;

    push @perfdata, (
        [ 'buffers_backend', $delta_buff_backend, 'Bps' ],
        [ 'checkpoint_timed', $delta_checkpoint_timed, 'Nps' ],
        [ 'checkpoint_req', $delta_checkpoint_req, 'Nps' ],
        [ 'buffers_checkpoint', $delta_buff_checkpointer, 'Bps' ],
        [ 'buffers_clean', $delta_buff_bgwriter, 'Bps' ],
        [ 'maxwritten_clean', $delta_maxwritten_clean, 'Nps' ],
        [ 'buffers_backend_fsync', $delta_backend_fsync, 'Nps' ],
        [ 'buffers_alloc', $delta_buff_alloc, 'Bps' ] );

    if ($delta_buff_total) {

        push @msg => sprintf(
            "%.2f%% from backends, %.2f%% from bgwriter, %.2f%% from checkpointer",
            100 * $delta_buff_backend      / $delta_buff_total,
            100 * $delta_buff_bgwriter     / $delta_buff_total,
            100 * $delta_buff_checkpointer / $delta_buff_total
        );
    }
    else {
        push @msg => "No writes";
    }

    # Alarm if asked.
    # FIXME: threshold should accept a % and a minimal written size
    if ( defined $args{'warning'}
        and defined $args{'critical'}
        and $delta_buff_total
    ) {
        my $w_limit = get_size( $args{'warning'},  $delta_buff_total );
        my $c_limit = get_size( $args{'critical'}, $delta_buff_total );

        return status_critical( $me, \@msg, \@perfdata )
            if $delta_buff_backend >= $c_limit;
        return status_warning( $me, \@msg, \@perfdata )
            if $delta_buff_backend >= $w_limit;
    }

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<btree_bloat>

Estimate bloat on B-tree indexes.

Warning and critical thresholds accept a comma-separated list of either
raw number(for a size), size (eg. 125M) or percentage. The thresholds apply to
B<bloat> size, not object size. If a percentage is given, the threshold will
apply to the bloat size compared to the total index size. If multiple threshold
values are passed, check_pgactivity will choose the largest (bloat size) value.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

It also supports a C<--exclude REGEX> parameter to exclude relations matching
a regular expression. The regular expression applies to
"database.schema_name.relation_name". This enables you to filter either on a
relation name for all schemas and databases, on a qualified named relation
(schema + relation) for all databases or on a qualified named relation in
only one database.

You can use multiple C<--exclude REGEX> parameters.

Perfdata will return the number of indexes of concern, by warning and critical
threshold per database.

A list of the bloated indexes will be returned after the
perfdata. This list contains the fully qualified bloated index name, the
estimated bloat size, the index size and the bloat percentage.

Required privileges: superuser (<10) able to log in all databases, or at least
those in C<--dbinclude>; superuser (<10);
on PostgreSQL 10+, a user with the role pg_monitor suffices,
provided that you grant SELECT on the system table pg_statistic
to the pg_monitor role, in each database of the cluster:
C<GRANT SELECT ON pg_statistic TO pg_monitor;>

=cut

sub check_btree_bloat {
    my @perfdata;
    my @longmsg;
    my @rs;
    my @hosts;
    my @all_db;
    my $total_index; # num of index checked, without excluded ones
    my $w_count = 0;
    my $c_count = 0;
    my %args    = %{ $_[0] };
    my @dbinclude  = @{ $args{'dbinclude'} };
    my @dbexclude  = @{ $args{'dbexclude'} };
    my $me      = 'POSTGRES_BTREE_BLOAT';
    my %queries = (
      $PG_VERSION_74 =>  q{
      SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
        bs*(relpages-est_pages)::bigint AS bloat_size,
        100 * (relpages-est_pages)::float / relpages AS bloat_ratio
      FROM (
        SELECT coalesce(
            1+ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0
          ) AS est_pages,
          bs, nspname, tblname, idxname, relpages, is_na
        FROM (
          SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam,
            ( index_tuple_hdr_bm +
                maxalign - CASE
                WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
                ELSE index_tuple_hdr_bm%maxalign
                END
            + nulldatawidth + maxalign - CASE
                WHEN nulldatawidth = 0 THEN 0
                WHEN nulldatawidth::numeric%maxalign = 0 THEN maxalign
                ELSE nulldatawidth::numeric%maxalign
                END
            )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
          FROM (
            SELECT n.nspname, sub.tblname, sub.idxname, sub.reltuples, sub.relpages, sub.relam,
              8192::numeric AS bs,
              CASE
                WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
                ELSE 4
              END AS maxalign,
              20 AS pagehdr,
              16 AS pageopqdata,
              CASE WHEN max(coalesce(sub.stanullfrac,0)) = 0
                THEN 2
                ELSE 2 + (( 32 + 8 - 1 ) / 8)
              END AS index_tuple_hdr_bm,
              sum( (1-coalesce(sub.stanullfrac, 0)) * coalesce(sub.stawidth, 1024)) AS nulldatawidth,
              max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0
                OR count(1) <> sub.indnatts AS is_na
            FROM (
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum, i.indnatts
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON ct.oid = i.indrelid
                JOIN pg_catalog.pg_statistic AS s ON s.starelid = i.indrelid
                  AND s.staattnum = ANY (
                    string_to_array(pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)), ' ')::smallint[]
                  )
              WHERE ci.relpages > 0
            ) AS sub
              JOIN pg_catalog.pg_attribute AS a ON sub.starelid = a.attrelid
                  AND sub.staattnum = a.attnum
              JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
              JOIN pg_catalog.pg_namespace AS n ON sub.relnamespace = n.oid
            WHERE a.attnum > 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, sub.indnatts
          ) AS sub2
        ) AS sub3
          JOIN pg_am am ON sub3.relam = am.oid
        WHERE am.amname = 'btree'
      ) AS sub4
      WHERE NOT is_na
      ORDER BY 2,3,4 },
      # Page header is 24 and block_size GUC, support index on expression
      $PG_VERSION_80 =>  q{
      SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
        bs*(relpages-est_pages)::bigint AS bloat_size,
        100 * (relpages-est_pages)::float / relpages AS bloat_ratio
      FROM (
        SELECT coalesce(1 +
               ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0
            ) AS est_pages,
            bs, nspname, tblname, idxname, relpages, is_na
        FROM (
          SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam,
            ( index_tuple_hdr_bm +
                maxalign - CASE
                WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
                ELSE index_tuple_hdr_bm%maxalign
                END
            + nulldatawidth + maxalign - CASE
                WHEN nulldatawidth = 0 THEN 0
                WHEN nulldatawidth::numeric%maxalign = 0 THEN maxalign
                ELSE nulldatawidth::numeric%maxalign
                END
            )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
          FROM (
            SELECT n.nspname, sub.tblname, sub.idxname, sub.reltuples, sub.relpages, sub.relam,
              current_setting('block_size')::numeric AS bs,
              CASE
                WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
                ELSE 4
              END AS maxalign,
              24 AS pagehdr,
              16 AS pageopqdata,
              CASE WHEN max(coalesce(sub.stanullfrac,0)) = 0
                THEN 2
                ELSE 2 + (( 32 + 8 - 1 ) / 8)
              END AS index_tuple_hdr_bm,
              sum( (1-coalesce(sub.stanullfrac, 0)) * coalesce(sub.stawidth, 1024)) AS nulldatawidth,
              max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
            FROM (
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON i.indrelid = ct.oid
                JOIN pg_catalog.pg_statistic AS s ON i.indexrelid = s.starelid
              WHERE ci.relpages > 0
              UNION
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON ct.oid = i.indrelid
                JOIN pg_catalog.pg_statistic AS s ON s.starelid = i.indrelid
                  AND s.staattnum = ANY (
                    string_to_array(pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)), ' ')::smallint[]
                  )
              WHERE ci.relpages > 0
            ) AS sub
              JOIN pg_catalog.pg_attribute AS a ON sub.starelid = a.attrelid
                  AND sub.staattnum = a.attnum
              JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
              JOIN pg_catalog.pg_namespace AS n ON sub.relnamespace = n.oid
            WHERE a.attnum > 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
          ) AS sub2
        ) AS sub3
          JOIN pg_am am ON sub3.relam = am.oid
        WHERE am.amname = 'btree'
      ) AS sub4
      WHERE NOT is_na
      ORDER BY 2,3,4 },
      # Use ANY (i.indkey) w/o function call to cast from vector to array
      $PG_VERSION_81 =>  q{
      SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
        bs*(relpages-est_pages)::bigint AS bloat_size,
        100 * (relpages-est_pages)::float / relpages AS bloat_ratio
      FROM (
        SELECT coalesce(1 +
               ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0
            ) AS est_pages,
            bs, nspname, tblname, idxname, relpages, is_na
        FROM (
          SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam,
            ( index_tuple_hdr_bm +
                maxalign - CASE
                WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
                ELSE index_tuple_hdr_bm%maxalign
                END
            + nulldatawidth + maxalign - CASE
                WHEN nulldatawidth = 0 THEN 0
                WHEN nulldatawidth::numeric%maxalign = 0 THEN maxalign
                ELSE nulldatawidth::numeric%maxalign
                END
            )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
          FROM (
            SELECT n.nspname, sub.tblname, sub.idxname, sub.reltuples, sub.relpages, sub.relam,
              current_setting('block_size')::numeric AS bs,
              CASE
                WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
                ELSE 4
              END AS maxalign,
              24 AS pagehdr,
              16 AS pageopqdata,
              CASE WHEN max(coalesce(sub.stanullfrac,0)) = 0
                THEN 2
                ELSE 2 + (( 32 + 8 - 1 ) / 8)
              END AS index_tuple_hdr_bm,
              sum( (1-coalesce(sub.stanullfrac, 0)) * coalesce(sub.stawidth, 1024)) AS nulldatawidth,
              max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
            FROM (
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON i.indrelid = ct.oid
                JOIN pg_catalog.pg_statistic AS s ON i.indexrelid = s.starelid
              WHERE ci.relpages > 0
              UNION
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON ct.oid = i.indrelid
                JOIN pg_catalog.pg_statistic AS s ON s.starelid = i.indrelid
                  AND s.staattnum = ANY ( i.indkey )
              WHERE ci.relpages > 0
            ) AS sub
              JOIN pg_catalog.pg_attribute AS a ON sub.starelid = a.attrelid
                  AND sub.staattnum = a.attnum
              JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
              JOIN pg_catalog.pg_namespace AS n ON sub.relnamespace = n.oid
            WHERE a.attnum > 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
          ) AS sub2
        ) AS sub3
          JOIN pg_am am ON sub3.relam = am.oid
        WHERE am.amname = 'btree'
      ) AS sub4
      WHERE NOT is_na
      ORDER BY 2,3,4 },
      # New column pg_index.indisvalid
      $PG_VERSION_82 =>  q{
      SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
        bs*(relpages-est_pages_ff) AS bloat_size,
        100 * (relpages-est_pages_ff)::float / relpages AS bloat_ratio
      FROM (
        SELECT coalesce(1 +
               ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0
            ) AS est_pages,
            coalesce(1 +
               ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0
            ) AS est_pages_ff,
            bs, nspname, tblname, idxname, relpages, fillfactor, is_na
        FROM (
          SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, fillfactor,
            ( index_tuple_hdr_bm +
                maxalign - CASE
                WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
                ELSE index_tuple_hdr_bm%maxalign
                END
            + nulldatawidth + maxalign - CASE
                WHEN nulldatawidth = 0 THEN 0
                WHEN nulldatawidth::numeric%maxalign = 0 THEN maxalign
                ELSE nulldatawidth::numeric%maxalign
                END
            )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
          FROM (
            SELECT n.nspname, sub.tblname, sub.idxname, sub.reltuples, sub.relpages, sub.relam, sub.fillfactor,
              current_setting('block_size')::numeric AS bs,
              CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
                WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
                ELSE 4
              END AS maxalign,
              24 AS pagehdr,
              16 AS pageopqdata,
              CASE WHEN max(coalesce(sub.stanullfrac,0)) = 0
                THEN 2 -- IndexTupleData size
                ELSE 2 + (( 32 + 8 - 1 ) / 8)
              END AS index_tuple_hdr_bm,
              sum( (1-coalesce(sub.stanullfrac, 0)) * coalesce(sub.stawidth, 1024)) AS nulldatawidth,
              max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
            FROM (
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum,
                coalesce(substring(
                  array_to_string(ci.reloptions, ' ')
                    from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON i.indrelid = ct.oid
                JOIN pg_catalog.pg_statistic AS s ON i.indexrelid = s.starelid
              WHERE ci.relpages > 0
              UNION
              SELECT ct.relnamespace, ct.relname AS tblname,
                ci.relname AS idxname, ci.reltuples, ci.relpages, ci.relam,
                s.stawidth, s.stanullfrac, s.starelid, s.staattnum,
                coalesce(substring(
                  array_to_string(ci.reloptions, ' ')
                    from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor
              FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class AS ct ON ct.oid = i.indrelid
                JOIN pg_catalog.pg_statistic AS s ON s.starelid = i.indrelid
                  AND s.staattnum = ANY ( i.indkey )
              WHERE ci.relpages > 0
            ) AS sub
              JOIN pg_catalog.pg_attribute AS a ON sub.starelid = a.attrelid
                  AND sub.staattnum = a.attnum
              JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
              JOIN pg_catalog.pg_namespace AS n ON sub.relnamespace = n.oid
            WHERE a.attnum > 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
          ) AS sub2
        ) AS sub3
          JOIN pg_am am ON sub3.relam = am.oid
        WHERE am.amname = 'btree'
      ) AS sub4
      WHERE NOT is_na
      ORDER BY 2,3,4 }
    );

    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "btree_bloat".',
        -exitval => 127
    ) if @hosts != 1;

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db (sort @all_db) {
        my @rc;
        # handle max, avg and count for size and percentage, per relkind
        my $nb_ind      = 0;
        my $idx_bloated = 0;

        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        @rc = @{ query_ver( $hosts[0], %queries, $db ) };

        BLOAT_LOOP: foreach my $bloat (@rc) {

            foreach my $exclude_re ( @{ $args{'exclude'} } ) {
                next BLOAT_LOOP if "$bloat->[0].$bloat->[1].$bloat->[3]" =~ m/$exclude_re/;
            }

            if ( defined $args{'warning'} ) {
                my $w_limit = 0;
                my $c_limit = 0;
                # We need to compute effective thresholds on each object,
                # as the value can be given in percentage
                # The biggest calculated size will be used.
                foreach my $cur_warning (split /,/, $args{'warning'}) {
                    my $size = get_size( $cur_warning, $bloat->[4] );
                    $w_limit = $size if $size > $w_limit;
                }
                foreach my $cur_critical (split /,/, $args{'critical'}) {
                    my $size = get_size( $cur_critical, $bloat->[4] );
                    $c_limit = $size if $size > $c_limit;
                }

                if ( $bloat->[5] > $w_limit ) {
                    $idx_bloated++;
                    $w_count++;
                    $c_count++ if $bloat->[5] > $c_limit;

                    push @longmsg => sprintf "%s.%s.%s %s/%s (%.2f%%);",
                        $bloat->[0], $bloat->[1], $bloat->[3],
                        to_size($bloat->[5]), to_size($bloat->[4]), $bloat->[6];
                }
            }

            $nb_ind++;
        }

        $total_index += $nb_ind;

        push @perfdata => [ "idx bloated in $db", $idx_bloated ];
    }

    # We use the warning count for the **total** number of bloated indexes
    return status_critical( $me,
        [ "$w_count/$total_index index(es) bloated" ],
        [ @perfdata ], [ @longmsg ] )
            if $c_count > 0;

    return status_warning( $me,
        [ "$w_count/$total_index index(es) bloated" ],
        [ @perfdata ], [ @longmsg ] )
            if $w_count > 0;

    return status_ok( $me, [ "Btree bloat ok" ], \@perfdata );
}


=item B<commit_ratio> (all)

Check the commit and rollback rate per second since last call.

This service uses the status file (see --status-file parameter).

Perfdata contains the commit rate, rollback rate, transaction rate and rollback
ratio for each database since last call.

Critical and Warning thresholds are optional. They accept a list of comma
separated 'label=value'. Available labels are B<rollbacks>, B<rollback_rate>
and B<rollback_ratio>, which will be compared to the number of rollbacks, the
rollback rate and the rollback ratio of each database. Warning or critical will
be raised if the reported value is greater than B<rollbacks>, B<rollback_rate>
or B<rollback_ratio>.

Required privileges: unprivileged role.

=cut

sub check_commit_ratio {
    my @rs;
    my @msg_warn;
    my @msg_crit;
    my @perfdata;
    my @hosts;
    my %xacts;
    my %new_xacts;
    my $global_commits;
    my $global_rollbacks;
    my %warn;
    my %crit;
    my %args  = %{ $_[0] };
    my $me    = 'POSTGRES_COMMIT_RATIO';
    my $sql   = q{
        SELECT floor(extract(EPOCH from now())), s.datname,
            s.xact_commit, s.xact_rollback
        FROM pg_stat_database s
          JOIN pg_database d ON s.datid = d.oid
        WHERE d.datallowconn
    };

    if ( defined $args{'warning'} ) {
        my $thresholds_re = qr/(rollbacks|rollback_rate|rollback_ratio)\s*=\s*(\d+)/i;

        # warning and critical must be list of status=value
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept a list of 'label=value' separated by comma.\n"
                . "See documentation for more information about accepted labels.",
            -exitval => 127
        ) unless $args{'warning'} =~ m/^$thresholds_re(\s*,\s*$thresholds_re)*$/
            and $args{'critical'} =~ m/^$thresholds_re(\s*,\s*$thresholds_re)*$/ ;

        while ( $args{'warning'} =~ /$thresholds_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $warn{$threshold} = $value if $1 and defined $2;
        }

        while ( $args{'critical'} =~ /$thresholds_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $crit{$threshold} = $value if $1 and defined $2;
        }
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "commit_ratio".',
        -exitval => 127
    ) if @hosts != 1;

    %xacts = %{ load( $hosts[0], 'commit_ratio', $args{'status-file'} ) || {} };

    @rs = @{ query( $hosts[0], $sql ) };

    $new_xacts{$_->[1]} = {
        'ts'       => $_->[0],
        'commit'   => $_->[2],
        'rollback' => $_->[3]
    } foreach @rs;

    save $hosts[0], 'commit_ratio', \%new_xacts, $args{'status-file'};

    return status_ok( $me, ['First call'] ) unless keys %xacts;

    foreach my $db ( keys %new_xacts ) {

        my $ratio = 0;
        my $commits   = $new_xacts{$db}{'commit'} - $xacts{$db}{'commit'};
        my $rollbacks = $new_xacts{$db}{'rollback'} - $xacts{$db}{'rollback'};

        # default to 1 sec if called twice in the same second
        my $sec = ( $new_xacts{$db}{'ts'} - $xacts{$db}{'ts'} ) || 1;

        my $commit_rate   = $commits   / $sec;
        my $rollback_rate = $rollbacks / $sec;
        my $xact_rate     = ($commits + $rollbacks ) / $sec;

        $global_commits   += $commits;
        $global_rollbacks += $rollbacks;

        $ratio = $rollbacks * 100 / ( $commits + $rollbacks )
            unless $rollbacks == 0;

        push @perfdata => (
            [ "${db}_commit_rate",    sprintf( "%.2f", $commit_rate ),   'tps' ],
            [ "${db}_rollback_rate",  sprintf( "%.2f", $rollback_rate ), 'tps' ],
            [ "${db}_xact_rate",      sprintf( "%.2f", $xact_rate ),     'tps' ],
            [ "${db}_rollback_ratio", sprintf( "%.2f", $ratio ),         '%'   ]
        );

        THRESHOLD_LOOP: foreach my $val ( ('rollbacks', 'rollback_rate', 'rollback_ratio') ) {
            my $prefix = "${db}_${val}";
            # Criticals
            if ( exists $crit{$val} ) {
                if ( $val eq "rollbacks" and $crit{$val} < $rollbacks ) {
                    push @msg_crit => "'$prefix'=$rollbacks";
                    next THRESHOLD_LOOP;
                }
                if ( $val eq "rollback_rate" and $crit{$val} < $rollback_rate ) {
                    push @msg_crit => sprintf "'%s'=%.2ftps", $prefix, $rollback_rate;
                    next THRESHOLD_LOOP;
                }
                if ( $val eq "rollback_ratio" and $crit{$val} < $ratio ) {
                    push @msg_crit => sprintf "'%s'=%.2f%%", $prefix, $ratio;
                    next THRESHOLD_LOOP;
                }
            }
            # Warnings
            if ( exists $warn{$val} ) {
                if ( $val eq "rollbacks" and $warn{$val} < $rollbacks ) {
                    push @msg_warn => "'$prefix'=$rollbacks";
                    next THRESHOLD_LOOP;
                }
                if ( $val eq "rollback_rate" and $warn{$val} < $rollback_rate ) {
                    push @msg_warn => sprintf("'%s'=%.2ftps", $prefix, $rollback_rate );
                    next THRESHOLD_LOOP;
                }
                if ( $val eq "rollback_ratio" and $warn{$val} < $ratio ) {
                    push @msg_warn => sprintf("'%s'=%.2f%%", $prefix, $ratio );
                    next THRESHOLD_LOOP;
                }
            }
        }
    }

    return status_critical( $me, [ "Commits: $global_commits - Rollbacks: $global_rollbacks", @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, [ "Commits: $global_commits - Rollbacks: $global_rollbacks", @msg_warn ], \@perfdata )
        if scalar @msg_warn > 0;

    return status_ok( $me, ["Commits: $global_commits - Rollbacks: $global_rollbacks"], \@perfdata );

}


=item B<configuration> (8.0+)

Check the most important settings.

Warning and Critical thresholds are ignored.

Specific parameters are :
C<--work_mem>, C<--maintenance_work_mem>, C<--shared_buffers>,C<--wal_buffers>,
C<--checkpoint_segments>, C<--effective_cache_size>, C<--no_check_autovacuum>,
C<--no_check_fsync>, C<--no_check_enable>, C<--no_check_track_counts>.

Required privileges: unprivileged role.

=cut

sub check_configuration {
    my @hosts;
    my @msg_crit;
    my %args = %{ $_[0] };
    my $me   = 'POSTGRES_CONFIGURATION';
    # This service is based on a probe by Marc Cousin (cousinmarc@gmail.com)
    # Limit parameters. Have defaut values
    my $work_mem             = $args{'work_mem'} || 4096; # At least 4MB
    my $maintenance_work_mem = $args{'maintenance_work_mem'} || 65536; # At least 64MB
    my $shared_buffers       = $args{'shared_buffers'} || 16384; # At least 128MB
    my $wal_buffers          = $args{'wal_buffers'} || 64; # At least 512k. Or -1 for 9.1
    my $checkpoint_segments  = $args{'checkpoint_segments'} || 10;
    # At least 1GB. No way a modern server has less than 2GB of ram
    my $effective_cache_size = $args{'effective_cache_size'} || 131072;
    # These will be checked to verify they are still the default values (no
    # parameter, for now) autovacuum, fsync,
    # enable*,track_counts/stats_row_level
    my $no_check_autovacuum   = $args{'no_check_autovacuum'} || 0;
    my $no_check_fsync        = $args{'no_check_fsync'} || 0;
    my $no_check_enable       = $args{'no_check_enable'} || 0;
    my $no_check_track_counts = $args{'no_check_track_counts'} || 0;

    my $sql = "SELECT name,setting FROM pg_settings
        WHERE ( ( name='work_mem' and setting::bigint < $work_mem )
            or ( name='maintenance_work_mem' and setting::bigint < $maintenance_work_mem )
            or ( name='shared_buffers' and setting::bigint < $shared_buffers )
            or ( name='wal_buffers' and ( setting::bigint < $wal_buffers or setting = '-1') )
            or ( name='checkpoint_segments' and setting::bigint < $checkpoint_segments )
            or ( name='effective_cache_size' and setting::bigint < $effective_cache_size )
            or ( name='autovacuum' and setting='off' and $no_check_autovacuum = 0)
            or ( name='fsync' and setting='off' and $no_check_fsync=0  )
            or ( name~'^enable.*' and setting='off' and $no_check_enable=0 and name not in ('enable_partitionwise_aggregate', 'enable_partitionwise_join'))
            or (name='stats_row_level' and setting='off' and $no_check_track_counts=0)
            or (name='track_counts' and setting='off' and $no_check_track_counts=0)
        )";

    # FIXME make one parameter --ignore to rules 'em all.

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "configuration".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'configuration', $PG_VERSION_80 or exit 1;

    my @rc = @{ query( $hosts[0], $sql ) };

DB_LOOP:    foreach my $setting (@rc) {
        push @msg_crit => ( $setting->[0] . "=" . $setting->[1] );
    }

    # All the entries in $result are an error. If the array isn't empty, we
    # return ERROR, and the list of errors
    return status_critical( $me, \@msg_crit )
        if ( @msg_crit > 0 );

    return status_ok( $me, [ "PostgreSQL configuration ok" ] );
}


=item B<connection> (all)

Perform a simple connection test.

No perfdata is returned.

This service ignores critical and warning arguments.

Required privileges: unprivileged role.

=cut

sub check_connection {
    my @rs;
    my @hosts;
    my %args = %{ $_[0] };
    my $me   = 'POSTGRES_CONNECTION';
    my $sql  = q{SELECT now(), version()};

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "connection".',
        -exitval => 127
    ) if @hosts != 1;

    @rs = @{ query( $hosts[0], $sql, undef, undef, \&status_critical ) };

    return status_ok( $me, [ "Connection successful at $rs[0][0], on $rs[0][1]" ] );
}


=item B<custom_query> (all)

Perform the given user query.

Specify the query with C<--query>. The first column will be
used to perform the test for the status if warning and critical are provided.

The warning and critical arguments are optional. They can be of format integer
(default), size or time depending on the C<--type> argument.
Warning and Critical will be raised if they are greater than the first column,
or less if the C<--reverse> option is used.

All other columns will be used to generate the perfdata. Each field name is
used as the name of the perfdata. The field value must contain your perfdata
value and its unit appended to it. You can add as many fields as needed. Eg.:

  SELECT pg_database_size('postgres'),
         pg_database_size('postgres')||'B' AS db_size

Required privileges: unprivileged role (depends on the query).

=cut

sub check_custom_query {
    my %args    = %{ $_[0] };
    my $me      = 'POSTGRES_CUSTOM_QUERY';
    my $sql     = $args{'query'};
    my $type    = $args{'type'} || 'integer';
    my $reverse = $args{'reverse'};
    my $bounded = undef;
    my @rs;
    my @fields;
    my @perfdata;
    my @hosts;
    my @msg_crit;
    my @msg_warn;
    my $c_limit;
    my $w_limit;
    my $perf;
    my $value;

    # FIXME: add warn/crit threshold in perfdata

    # Query must be given
    pod2usage(
        -message => 'FATAL: you must set parameter "--query" with "custom_query" service.',
        -exitval => 127
    ) unless defined $args{'query'} ;

    # Critical and Warning must be given with --type argument
    pod2usage(
        -message => 'FATAL: you must specify critical and warning thresholds with "--type" parameter.',
        -exitval => 127
    ) unless ( not defined $args{'type'} ) or
        ( defined $args{'type'} and $args{'warning'} and $args{'critical'} );


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "custom_query".',
        -exitval => 127
    ) if @hosts != 1;


    # Handle warning and critical type
    if ( $type eq 'size' ) {
        $w_limit = get_size( $args{'warning'} );
        $c_limit = get_size( $args{'critical'} );
    }
    elsif ( $type eq 'time' ) {
        pod2usage(
                -message => "FATAL: critical and warning thresholds only acccepts interval with --type time.",
                -exitval => 127
                ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );

        $w_limit = get_time( $args{'warning'} );
        $c_limit = get_time( $args{'critical'} );
    }
    elsif (defined $args{'warning'} ) {
        pod2usage(
            -message => 'FATAL: given critical and/or warning are not numeric. Please, set "--type" parameter if needed.',
            -exitval => 127
        ) if $args{'warning'} !~ m/^[0-9.]+$/
            or $args{'critical'} !~ m/^[0-9.]+$/;
        $w_limit = $args{'warning'};
        $c_limit = $args{'critical'};
    }

    @rs = @{ query( $hosts[0], $sql, undef, 1 ) };
    @fields = @{ shift @rs };

    return status_unknown( $me, [ 'No row returned by the query!' ] )
        unless defined $rs[0];

    pod2usage(
        -message => 'FATAL: First column of your query is not numeric!',
        -exitval => 127
    ) unless looks_like_number($rs[0][0]);

    DB_LOOP: foreach my $rec ( @rs ) {
        $bounded = $rec->[0] unless $bounded;

        $bounded = $rec->[0] if ( !$reverse and $rec->[0] > $bounded )
            or ( $reverse and $rec->[0] < $bounded );

        $value = shift( @{$rec} );
        shift @fields;

        foreach my $perf ( @$rec ) {
            my ( $val, $uom );
            $perf =~ m{([0-9.]*)(.*)};
            $val = $1 if defined $1;
            $uom = $2 if defined $2;
            push @perfdata => [ shift @fields, $val, $uom ];
        }

        if ( ( defined $c_limit )
            and (
                ( !$reverse and ( $value > $c_limit ) )
                or ( $reverse and ( $value < $c_limit ) )
            )
        ) {
            push @msg_crit => "value: $value";
            next DB_LOOP;
        }

        if ( ( defined $w_limit )
            and (
                ( !$reverse and ( $value > $w_limit ) )
                or ( $reverse  and ( $value < $w_limit ) )
            )
        ) {
            push @msg_warn => "value: $value";
            next DB_LOOP;
        }
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if defined $c_limit and
            ( ( !$reverse and $bounded > $c_limit)
            or ( $reverse and $bounded < $c_limit) );

    return status_warning( $me, [ @msg_warn ], \@perfdata )
        if defined $w_limit and
            ( ( !$reverse and $bounded > $w_limit)
            or ( $reverse and $bounded < $w_limit) );

    return status_ok( $me, [ "Custom query ok" ], \@perfdata );
}

=item B<database_size> (8.1+)

B<Check the variation> of database sizes, and B<return the size> of every
databases.

This service uses the status file (see C<--status-file> parameter).

Perfdata contains the size of each database and their size delta since last call.

Critical and Warning thresholds are optional. They are a list of optional 'label=value'
separated by a comma. It allows to fine tune the alert based on the
absolute C<size> and/or the C<delta> size. Eg.:

    -w 'size=500GB' -c 'size=600GB'
    -w 'delta=1%' -c 'delta=10%'
    -w 'size=500GB,delta=1%' -c 'size=600GB,delta=10GB'

The C<size> label accepts either a raw number or a size and checks the total database size.
The C<delta> label accepts either a raw number, a percentage, or a size.
The aim of the delta parameter is to detect unexpected database size variations.
Delta thresholds are absolute value, and delta percentages are computed against
the previous database size.
A same label must be filled for both warning and critical.

For backward compatibility, if a single raw number or percentage or size is given with no
label, it applies on the size difference for each database since the last execution.
Both threshold bellow are equivalent:

    -w 'delta=1%' -c 'delta=10%'
    -w '1%' -c '10%'

This service supports both C<--dbexclude> and C<--dbinclude> parameters.

Required privileges: unprivileged role.

=cut

sub check_database_size {
    my @msg_crit;
    my @msg_warn;
    my @rs;
    my @perfdata;
    my @hosts;
    my %new_db_sizes;
    my %old_db_sizes;
    my %warn;
    my %crit;
    my %args       = %{ $_[0] };
    my @dbinclude  = @{ $args{'dbinclude'} };
    my @dbexclude  = @{ $args{'dbexclude'} };
    my $me         = 'POSTGRES_DB_SIZE';
    my $db_checked = 0;
    my $sql        = q{SELECT datname, pg_database_size(datname)
        FROM pg_database};

    # Warning and critical are optional, but they are both required if one is given
    pod2usage(
        -message => "FATAL: you must specify both critical and warning thresholds.",
        -exitval => 127
    ) if ( defined $args{'warning'} and not defined $args{'critical'} )
      or ( not defined $args{'warning'} and defined $args{'critical'} );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "database_size".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'database_size', $PG_VERSION_81 or exit 1;

    if ( defined $args{'warning'} ) {
        my $threshods_re = qr/(size|delta)\s*=\s*([^,]+)/i;

        # backward compatibility
        $args{'warning'} = "delta=$args{'warning'}"
            if is_size($args{'warning'}) or ($args{'warning'} =~ m/^([0-9.]+)%?$/);
        $args{'critical'} = "delta=$args{'critical'}"
            if is_size($args{'critical'}) or ($args{'critical'} =~ m/^([0-9.]+)%?$/);

        # Sanity check
        pod2usage(
            -message => "FATAL: wrong format for critical and/or warning thresholds.\n"
                . "See documentation for more information.",
            -exitval => 127
        ) unless $args{'warning'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/
            and $args{'critical'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/ ;

        while ( $args{'warning'} =~ /$threshods_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $warn{$threshold} = $value if $1 and defined $2;
        }

        while ( $args{'critical'} =~ /$threshods_re/g ) {
            my ($threshold, $value) = ($1, $2);
            $crit{$threshold} = $value if $1 and defined $2;
        }

        # Further sanity checks
        pod2usage(
            -message => "FATAL: Size threshold only accept a raw number or a size.\n"
                . "See documentation for more information.",
            -exitval => 127
        ) if (defined $warn{'size'} and not is_size($warn{'size'}))
        or (defined $crit{'size'} and not is_size($crit{'size'}));

        pod2usage(
            -message => "FATAL: you must specify both critical and warning thresholds for size.",
            -exitval => 127
        ) if (defined $warn{'size'} and not defined $crit{'size'})
          or (defined $crit{'size'} and not defined $warn{'size'});

        pod2usage(
            -message => "FATAL: Delta threshold only accept a raw number, a size or a percentage.\n"
                . "See documentation for more information.",
            -exitval => 127
        ) if (defined $warn{'delta'} and not ( is_size($warn{'delta'}) or $warn{'delta'} =~ m/^([0-9.]+)%?$/ ))
          or (defined $crit{'delta'} and not ( is_size($crit{'delta'}) or $crit{'delta'} =~ m/^([0-9.]+)%?$/ ));

        pod2usage(
            -message => "FATAL: you must specify both critical and warning thresholds for delta.",
            -exitval => 127
        ) if (defined $warn{'delta'} and not defined $crit{'delta'})
          or (defined $crit{'delta'} and not defined $warn{'delta'});
    }

    # get old size from status file
    %old_db_sizes = %{ load( $hosts[0], 'db_size', $args{'status-file'} ) || {} };

    @rs = @{ query( $hosts[0], $sql ) };

    DB_LOOP: foreach my $db (@rs) {
        my $delta;
        # $old_db_sizes{ $db->[0] } is the previous DB size
        # $db->[1] is the new DB size

        $new_db_sizes{ $db->[0] } = $db->[1];

        next DB_LOOP if grep { $db->[0] =~ /$_/ } @dbexclude;
        next DB_LOOP if @dbinclude and not grep { $db->[0] =~ /$_/ } @dbinclude;

        $db_checked++;

        unless ( defined $old_db_sizes{ $db->[0] } ) {
            push @perfdata => [ $db->[0], $db->[1], 'B' ];
            next DB_LOOP;
        }

        $delta = $db->[1] - $old_db_sizes{ $db->[0] };

        # Must check threshold for each database
        if ( defined $args{'warning'} ) {
            my $limit;
            my $w_limit;
            my $c_limit;

            # Check against max db size
            if ( defined $crit{'size'} ) {
                $c_limit = get_size( $crit{'size'},  $db->[1] );
                push @msg_crit => sprintf( "%s (size: %s)", $db->[0], to_size($db->[1]) )
                    if $db->[1] >= $c_limit;
            }
            if ( defined $warn{'size'}
                 and defined $c_limit and $db->[1] < $c_limit
            ) {
                $w_limit = get_size( $warn{'size'},  $db->[1] );
                push @msg_warn => sprintf( "%s (size: %s)", $db->[0], to_size($db->[1]) )
                    if $db->[1] >= $w_limit;
            }

            push @perfdata => [ $db->[0], $db->[1], 'B', $w_limit, $c_limit ];

            # Check against delta variations (% or absolute values)
            $c_limit = undef;
            $w_limit = undef;
            if ( defined $crit{'delta'} ) {

                $limit = get_size( $crit{'delta'},  $old_db_sizes{ $db->[0] });
                dprint ("DB $db->[0] new size: $db->[1]  old size $old_db_sizes{ $db->[0] } (delta $delta) critical delta $crit{'delta'} computed limit $limit \n");
                push @msg_crit => sprintf( "%s (delta: %s)", $db->[0], to_size($delta) )
                    if abs($delta) >= $limit;
                $c_limit = "-$limit:$limit";
            }
            if ( defined $warn{'delta'}
                 and defined $c_limit and abs($delta) < $limit
            ) {
                $limit = get_size( $warn{'delta'},  $old_db_sizes{ $db->[0] } );
                dprint ("DB $db->[0] new size: $db->[1]  old size $old_db_sizes{ $db->[0] } (delta $delta) warning  delta $warn{'delta'} computed limit $limit \n");
                push @msg_warn => sprintf( "%s (delta: %s)", $db->[0], to_size($delta) )
                    if abs($delta) >= $limit;
                $w_limit = "-$limit:$limit";
            }

            push @perfdata => [ "$db->[0]_delta", $delta, 'B', $w_limit, $c_limit ];
        }
        else {
            push @perfdata => [ $db->[0], $db->[1], 'B' ];
            push @perfdata => [ "$db->[0]_delta", $delta, 'B' ];
        }
    }

    save $hosts[0], 'db_size', \%new_db_sizes, $args{'status-file'};

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if scalar @msg_warn > 0;

    return status_ok( $me, [ "$db_checked database(s) checked" ], \@perfdata );
}


=item B<extensions_versions> (9.1+)

Check all extensions installed in all databases (including templates)
and raise a critical alert if the current version is not the default
version available on the instance (according to pg_available_extensions).

Typically, it is used to detect forgotten extension upgrades after package
upgrades or a pg_upgrade.

Perfdata returns the number of outdated extensions in each database.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
Schemas are ignored, as an extension cannot be installed more than once
in a database.

This service supports multiple C<--exclude> argument to exclude one or
more extensions from the check. To ignore an extension only in a particular database,
use  'dbname/extension_name' syntax.

Examples:

    --dbexclude 'devdb' --exclude 'testdb/postgis' --exclude 'testdb/postgis_topology'
    --dbinclude 'proddb' --dbinclude 'testdb'  --exclude 'powa'

Required privileges: unprivileged role able to log in all databases

=cut
sub check_extensions_versions {
    my @rs;
    my @perfdata;
    my @msg;
    my @longmsg;
    my @hosts;
    my @all_db;
    my $nb;
    my $me           = 'POSTGRES_CHECK_EXT_VERSIONS';
    my %args         = %{ $_[0] };
    my @dbinclude    = @{ $args{'dbinclude'} };
    my @dbexclude    = @{ $args{'dbexclude'} };
    my $tot_outdated = 0 ;
    my $query = q{SELECT name, default_version, installed_version
                 FROM pg_catalog.pg_available_extensions
                 WHERE installed_version != default_version};

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "extensions_versions".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'extensions_versions', $PG_VERSION_91 or exit 1;

    @all_db = @{ get_all_dbname( $hosts[0], 'all_dbs' ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db (sort @all_db) {
        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        my $outdated = 0;

        # For each record: extension, default, installed
        @rs = @{ query ( $hosts[0], $query, $db ) };

        REC_LOOP: foreach my $ext (sort @rs) {
            foreach my $exclude_re ( @{ $args{'exclude'} } ) {
                next REC_LOOP if $ext->[0] =~ /$exclude_re/ or "$db/$ext->[0]" =~ /$exclude_re/ ;
            }

            $outdated++;
            push @longmsg, "$db.$ext->[0]: $ext->[2] (should be: $ext->[1])";
        }

        dprint("db $db: $outdated outdated ext\n");

        $tot_outdated += $outdated;

        push @perfdata => [ $db, $outdated, undef, undef, 1, 0 ];
    }

    return status_critical( $me, \@msg, \@perfdata, \@longmsg )
        if $tot_outdated > 0;

    return status_ok( $me, \@msg, \@perfdata, \@longmsg );
}


=item B<hit_ratio> (all)

Check the cache hit ratio on the cluster.

This service uses the status file (see C<--status-file> parameter).

Perfdata returns the cache hit ratio per database. Template databases and
databases that do not allow connections will not be checked, nor will the
databases which have never been accessed.

Critical and Warning thresholds are optional. They only accept a percentage.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.

Required privileges: unprivileged role.

=cut

sub check_hit_ratio {
    my @rs;
    my @perfdata;
    my @msg_crit;
    my @msg_warn;
    my @hosts;
    my %db_hitratio;
    my %new_db_hitratio;
    my %args       = %{ $_[0] };
    my @dbinclude  = @{ $args{'dbinclude'} };
    my @dbexclude  = @{ $args{'dbexclude'} };
    my $me         = 'POSTGRES_HIT_RATIO';
    my $db_checked = 0;
    my $sql        = q{SELECT d.datname, blks_hit, blks_read
        FROM pg_stat_database sd
          JOIN pg_database d ON d.oid = sd.datid
        WHERE d.datallowconn AND NOT d.datistemplate
        ORDER BY datname};

    # Warning and critical must be %.
    if ( defined $args{'warning'} and defined $args{'critical'} ) {
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept percentages.",
            -exitval => 127
        ) unless $args{'warning'} =~ m/^([0-9.]+)%$/
            and $args{'critical'} =~ m/^([0-9.]+)%$/;

        $args{'warning'}  = substr $args{'warning'}, 0, -1;
        $args{'critical'} = substr $args{'critical'}, 0, -1;
    }


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "hit_ratio".',
        -exitval => 127
    ) if @hosts != 1;


    %db_hitratio = %{ load( $hosts[0], 'db_hitratio', $args{'status-file'} ) || {} };

    @rs = @{ query( $hosts[0], $sql ) };

    DB_LOOP: foreach my $db (@rs) {
        my $ratio;
        my $hit_delta;
        my $read_delta;
        my @perfdata_value;

        $new_db_hitratio{ $db->[0] } = [ $db->[1], $db->[2], 'NaN' ];

        next DB_LOOP if grep { $db->[0] =~ /$_/ } @dbexclude;
        next DB_LOOP if @dbinclude and not grep { $db->[0] =~ /$_/ } @dbinclude;

        $db_checked++;

        next DB_LOOP unless defined $db_hitratio{ $db->[0] };

        $hit_delta  = $new_db_hitratio{ $db->[0] }[0] - $db_hitratio{ $db->[0] }[0];
        $read_delta = $new_db_hitratio{ $db->[0] }[1] - $db_hitratio{ $db->[0] }[1];

        # Metrics moved since last run
        if ( $hit_delta + $read_delta > 0 ) {
            $ratio = 100 * $hit_delta / ( $hit_delta + $read_delta );
            # rounding the fractional part to 2 digits
            $ratio = int($ratio*100+0.5)/100;
            $new_db_hitratio{ $db->[0] }[2] = $ratio;

            @perfdata_value = ( $db->[0], $ratio, '%' );
        }
        # Without activity since last run, use previous hit ratio.
        # This should not happen as the query itself hits/reads.
        elsif ( $db->[1] + $db->[2] > 0 ) {
            $ratio = $db_hitratio{ $db->[0] }[2];
            @perfdata_value = ( $db->[0], $ratio, '%' );
        }
        # This database has no reported activity yet
        else {
            $ratio='NaN';
            $new_db_hitratio{ $db->[0] }[2] = 'NaN';
            @perfdata_value = ( $db->[0], 'NaN', '%' );
        }

        push @perfdata_value => ( $args{'warning'}, $args{'critical'} )
            if defined $args{'critical'};

        push @perfdata => \@perfdata_value;

        if ( defined $args{'critical'} ) {
            if ( $ratio < $args{'critical'} ) {
                push @msg_crit => sprintf "%s: %s%%", $db->[0], $ratio;
                next DB_LOOP;
            }

            if ( defined $args{'warning'} and $ratio < $args{'warning'} ) {
                push @msg_warn => sprintf "%s: %s%%", $db->[0], $ratio;
            }
        }
    }

    save $hosts[0], 'db_hitratio', \%new_db_hitratio, $args{'status-file'};

    if ( defined $args{'critical'} ) {
        return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
            if scalar @msg_crit;

        return status_warning( $me, \@msg_warn, \@perfdata )
            if scalar @msg_warn;
    }

    return status_ok( $me, [ "$db_checked database(s) checked" ], \@perfdata );
}


=item B<hot_standby_delta> (9.0)

Check the data delta between a cluster and its hot standbys.

You must give the connection parameters for two or more clusters.

Perfdata returns the data delta in bytes between the master and each hot
standby cluster listed.

Critical and Warning thresholds are optional. They can take one or two values
separated by a comma. If only one value given, it applies to both received and
replayed data.
If two values are given, the first one applies to received data, the second one
to replayed ones. These thresholds only accept a size (eg. 2.5G).

This service raises a Critical if it doesn't find exactly ONE valid master
cluster (ie. critical when 0 or 2 and more masters).

Required privileges: unprivileged role.

=cut

sub check_hot_standby_delta {
    my @perfdata;
    my @msg;
    my @msg_crit;
    my @msg_warn;
    my $w_limit_received;
    my $c_limit_received;
    my $w_limit_replayed;
    my $c_limit_replayed;
    my @hosts;
    my %args            = %{ $_[0] };
    my $master_location = '';
    my $num_clusters    = 0;
    my $wal_size        = hex('ff000000');
    my $me              = 'POSTGRES_HOT_STANDBY_DELTA';
    # we need to coalesce on pg_last_xlog_receive_location because it returns
    # NULL during WAL Shipping
    my %queries = (
        $PG_VERSION_90 => q{
        SELECT (NOT pg_is_in_recovery())::int,
            CASE pg_is_in_recovery()
                WHEN 't' THEN coalesce(
                    pg_last_xlog_receive_location(),
                    pg_last_xlog_replay_location()
                )
                ELSE pg_current_xlog_location()
            END,
            CASE pg_is_in_recovery()
                WHEN 't' THEN pg_last_xlog_replay_location()
                ELSE NULL
            END
        },
        $PG_VERSION_100 => q{
        SELECT (NOT pg_is_in_recovery())::int,
            CASE pg_is_in_recovery()
                WHEN 't' THEN coalesce(
                    pg_last_wal_receive_lsn(),
                    pg_last_wal_replay_lsn()
                )
                ELSE pg_current_wal_lsn()
            END,
            CASE pg_is_in_recovery()
                WHEN 't' THEN pg_last_wal_replay_lsn()
                ELSE NULL
            END
    });

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give two or more hosts with service "hot_standby_delta".',
        -exitval => 127
    ) if @hosts < 2;

    foreach my $host ( @hosts ) {
        is_compat $host, 'hot_standby_delta', $PG_VERSION_90 or exit 1;
    }

    # Fetch LSNs
    foreach my $host (@hosts) {
        $host->{'rs'} = \@{ query_ver( $host, %queries )->[0] };
        $num_clusters += $host->{'rs'}[0];
        $master_location = $host->{'rs'}[1] if $host->{'rs'}[0];
    }

    # Check that all clusters have the same major version.
    foreach my $host ( @hosts ) {
        return status_critical( $me,
            ["PostgreSQL major versions differ amongst clusters ($hosts[0]{'version'} vs. $host->{'version'})."]
        ) if substr($hosts[0]{'version_num'}, 0, -2)
            != substr($host->{'version_num'}, 0, -2);
    }

    return status_critical( $me, ['No cluster in production.'] ) if $num_clusters == 0;

    return status_critical( $me, ['More than one cluster in production.'] )
        if $num_clusters != 1;

    if ( defined $args{'critical'} ) {
        ($w_limit_received, $w_limit_replayed) = split /,/, $args{'warning'};
        ($c_limit_received, $c_limit_replayed) = split /,/, $args{'critical'};

        if (!defined($w_limit_replayed)) {
            $w_limit_replayed = $w_limit_received;
        }
        if (!defined($c_limit_replayed)) {
            $c_limit_replayed = $c_limit_received;
        }

        $w_limit_received = get_size( $w_limit_received );
        $c_limit_received = get_size( $c_limit_received );
        $w_limit_replayed = get_size( $w_limit_replayed );
        $c_limit_replayed = get_size( $c_limit_replayed );
    }

    $wal_size = 4294967296 if $hosts[0]{'version_num'} >= $PG_VERSION_93;

    # We recycle this one to count the number of slaves
    $num_clusters = 0;

    $master_location =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
    $master_location = ( $wal_size * hex($1) ) + hex($2);

    # Compute deltas
    foreach my $host (@hosts) {
        next if $host->{'rs'}[0];
        my ($a, $b) = split(/\//, $host->{'rs'}[1]);
        $host->{'receive_delta'} = $master_location - ( $wal_size * hex($a) ) - hex($b);

        ($a, $b) = split(/\//, $host->{'rs'}[2]);
        $host->{'replay_delta'} = $master_location - ( $wal_size * hex($a) ) - hex($b);

        $host->{'name'} =~ s/ db=.*$//;

        push @perfdata => ([
            "receive delta $host->{'name'}",
            $host->{'receive_delta'} > 0 ? $host->{'receive_delta'}:0, 'B',
            $w_limit_received, $c_limit_received
        ],
        [
            "replay delta $host->{'name'}",
            $host->{'replay_delta'} > 0 ? $host->{'replay_delta'}:0, 'B',
            $w_limit_replayed, $c_limit_replayed
        ]);

        if ( defined $args{'critical'} ) {

            if ($host->{'receive_delta'} > $c_limit_received) {
                push @msg_crit, "critical receive lag: "
                    . to_size($host->{'receive_delta'}) . " for $host->{'name'}";
                next;
            }

            if ($host->{'replay_delta'} > $c_limit_replayed) {
                push @msg_crit, "critical replay lag: "
                    . to_size($host->{'replay_delta'}) . " for $host->{'name'}";
                next;
            }

            if ($host->{'receive_delta'} > $w_limit_received) {
                push @msg_warn, "warning receive lag: "
                    . to_size($host->{'receive_delta'}) . " for $host->{'name'}";
                next;
            }

            if ($host->{'replay_delta'} > $w_limit_replayed) {
                push @msg_warn, "warning replay lag: "
                    . to_size($host->{'replay_delta'}) . " for $host->{'name'}";
                next;
            }
        }

        $num_clusters++;
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if @msg_warn > 0;

    return status_ok( $me, [ "$num_clusters Hot standby checked" ], \@perfdata );
}


=item B<is_hot_standby> (9.0+)

Checks if the cluster is in recovery and accepts read only queries.

This service ignores critical and warning arguments.

No perfdata is returned.

Required privileges: unprivileged role.

=cut

sub check_is_hot_standby {
    my @rs;
    my @hosts;
    my %args          = %{ $_[0] };
    my $me            = 'POSTGRES_IS_HOT_STANDBY';
    my %queries       = (
        $PG_VERSION_90 => q{SELECT pg_is_in_recovery()}
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "is_hot_standby".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'is_hot_standby', $PG_VERSION_90 or exit 1;
    @rs = @{ query_ver( $hosts[0], %queries )->[0] };

    return status_critical( $me, [ "Cluster is not hot standby" ] ) if $rs[0] eq "f";
    return status_ok( $me, [ "Cluster is hot standby" ] );
}


=item B<is_master> (all)

Checks if the cluster accepts read and/or write queries. This state is reported
as "in production" by pg_controldata.

This service ignores critical and warning arguments.

No perfdata is returned.

Required privileges: unprivileged role.

=cut

sub check_is_master {
    my @rs;
    my @hosts;
    my %args          = %{ $_[0] };
    my $me            = 'POSTGRES_IS_MASTER';
    # For PostgreSQL 9.0+, the "pg_is_in_recovery()" function is used, for
    # previous versions the ability to connect is enough.
    my %queries       = (
        $PG_VERSION_74  => q{ SELECT false },
        $PG_VERSION_90 => q{ SELECT pg_is_in_recovery() }
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "is_master".',
        -exitval => 127
    ) if @hosts != 1;

    @rs = @{ query_ver( $hosts[0], %queries )->[0] };

    return status_critical( $me, [ "Cluster is not master" ] ) if $rs[0] eq "t";
    return status_ok( $me, [ "Cluster is master" ] );
}

=item B<invalid_indexes> (8.2+)

Check if there are invalid indexes in a database.

A critical alert is raised if an invalid index is detected.

This service supports both C<--dbexclude>  and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

This service supports a C<--exclude REGEX>  parameter to exclude indexes
matching a regular expression. The regular expression applies to
"database.schema_name.index_name". This enables you to filter either on a
relation name for all schemas and databases, on a qualified named
index (schema + index) for all databases or on a qualified named
index in only one database.

You can use multiple C<--exclude REGEX>  parameters.

Perfdata will return the number of invalid indexes per database.

A list of invalid indexes will be returned after the
perfdata. This list contains the fully qualified index name. If
excluded index is set, the number of exclude indexes is returned.

Required privileges: unprivileged role able to log in all databases.

=cut

sub check_invalid_indexes {
    my @perfdata;
    my @longmsg;
    my @rs;
    my @hosts;
    my @all_db;
    my $total_idx   = 0; # num of tables checked, without excluded ones
    my $total_extbl = 0; # num of excluded tables
    my $c_count     = 0;
    my %args        = %{ $_[0] };
    my @dbinclude   = @{ $args{'dbinclude'} };
    my @dbexclude   = @{ $args{'dbexclude'} };
    my $me          = 'POSTGRES_INVALID_INDEXES';
    my $query       = q{
    SELECT current_database(), nsp.nspname AS schemaname, cls.relname, idx.indisvalid
        FROM pg_class cls
            join pg_namespace nsp on nsp.oid = cls.relnamespace
            join pg_index idx on idx.indexrelid = cls.oid
        WHERE
            cls.relkind = 'i'
        AND nsp.nspname not like 'pg_toast%'
        AND nsp.nspname NOT IN ('information_schema', 'pg_catalog');
    };

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give one (and only one) host with service "invalid_indexes".',
        -exitval => 127
    ) if @hosts != 1;

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db ( sort @all_db ) {

        my @rc;
        my $nb_idx      = 0;
        my $idx_invalid = 0;

        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        @rc = @{ query( $hosts[0], $query, $db ) };

        INVALIDIDX_LOOP: foreach my $invalid (@rc) {
            foreach my $exclude_re ( @{ $args{'exclude'} } ) {
                if ("$invalid->[0].$invalid->[1].$invalid->[2]" =~ m/$exclude_re/){
                    $total_extbl++;
                    next INVALIDIDX_LOOP ;
                }
            }
            if ($invalid->[3] eq "f") {
                # long message info :
                push @longmsg => sprintf "Invalid index = %s.%s.%s ; ",
                    $invalid->[0], $invalid->[1], $invalid->[2];
                $idx_invalid++;
            }
            $nb_idx++;
        }
        $total_idx += $nb_idx;
        $c_count += $idx_invalid;
        push @perfdata => ["invalid index in $db", $idx_invalid ];
    }
    push @longmsg => sprintf "%i index(es) exclude from check", $total_extbl
        if $total_extbl > 0;

    # we use the critical count for the **total** number of invalid index
    return status_critical( $me,
        [ "$c_count/$total_idx index(es) invalid" ],
        \@perfdata, \@longmsg
    ) if $c_count > 0;

    return status_ok( $me, [ "No invalid index" ], \@perfdata, \@longmsg );
}


=item B<is_replay_paused> (9.1+)

Checks if the replication is paused. The service will return UNKNOWN if
executed on a master server.

Thresholds are optional. They must be specified as interval. OK will always be
returned if the standby is not paused, even if replication delta time hits the
thresholds.

Critical or warning are raised if last reported replayed timestamp is greater
than given threshold AND some data received from the master are not applied
yet.  OK will always be returned if the standby is paused, or if the standby
has already replayed everything from master and until some write activity
happens on the master.

Perfdata returned:
  * paused status (0 no, 1 yes, NaN if master)
  * lag time (in second)
  * data delta with master (0 no, 1 yes)

Required privileges: unprivileged role.

=cut

sub check_is_replay_paused {
    my @perfdata;
    my @rs;
    my @hosts;
    my $w_limit = -1;
    my $c_limit = -1;
    my %args    = %{ $_[0] };
    my $me      = 'POSTGRES_REPLICATION_PAUSED';
    my %queries = (
        $PG_VERSION_91 => q{
        SELECT pg_is_in_recovery()::int AS is_in_recovery,
        CASE pg_is_in_recovery()
            WHEN 't' THEN pg_is_xlog_replay_paused()::int
            ELSE 0::int
        END AS is_paused,
        CASE pg_is_in_recovery()
            WHEN 't' THEN extract('epoch' FROM now()-pg_last_xact_replay_timestamp())::int
            ELSE NULL::int
        END AS lag,
        CASE
            WHEN pg_is_in_recovery() AND pg_last_xlog_replay_location() <> pg_last_xlog_receive_location()
                THEN 1::int
            WHEN pg_is_in_recovery() THEN 0::int
            ELSE NULL
        END AS delta},
        $PG_VERSION_100 => q{
        SELECT pg_is_in_recovery()::int AS is_in_recovery,
        CASE pg_is_in_recovery()
            WHEN 't' THEN pg_is_wal_replay_paused()::int
            ELSE 0::int
        END AS is_paused,
        CASE pg_is_in_recovery()
            WHEN 't' THEN extract('epoch' FROM now()-pg_last_xact_replay_timestamp())::int
            ELSE NULL::int
        END AS lag,
        CASE
            WHEN pg_is_in_recovery() AND pg_last_wal_replay_lsn() <> pg_last_wal_receive_lsn()
                THEN 1::int
            WHEN pg_is_in_recovery() THEN 0::int
            ELSE NULL
        END AS delta}
    );

    if ( defined $args{'warning'} and defined $args{'critical'} ) {
        # warning and critical must be interval if provided.
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept interval.",
            -exitval => 127
        ) unless ( is_time( $args{'warning'} ) and  is_time( $args{'critical'} ) );

        $c_limit = get_time $args{'critical'};
        $w_limit = get_time $args{'warning'};
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "is_replay_paused".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], "is_replay_paused", $PG_VERSION_91 or exit 1;

    @rs = @{ query_ver( $hosts[0], %queries )->[0] };

    return status_unknown( $me,
        [ "Server is not standby." ],
        [ [ 'is_paused', 'NaN' ],
          [ 'lag_time', 'NaN', 's' ],
          [ 'has_data_delta', 'NaN', 's' ] ]
    ) if not $rs[0];

    push @perfdata, [ "is_paused", $rs[1] ];
    push @perfdata, [ "lag_time", $rs[2], "s" ];
    push @perfdata, [ "has_data_delta", $rs[3] ];

    # Always return ok if replay is not paused
    return status_ok( $me, [ ' replay is not paused' ], \@perfdata ) if not $rs[1];

    # Do we have thresholds?
    if ( $c_limit != -1 ) {
        return status_critical( $me,
            [' replay lag time: ' . to_interval( $rs[2] ) ],
            \@perfdata
        ) if $rs[3] and $rs[2] > $c_limit;

        return status_warning( $me,
            [' replay lag time: ' . to_interval( $rs[2] ) ],
            \@perfdata
        ) if $rs[3] and $rs[2] > $w_limit;
    }

    return status_ok( $me, [ ' replay is paused.' ], \@perfdata );
}


# Agnostic check vacuum or analyze sub
# FIXME: we can certainly do better about temp tables
sub check_last_maintenance {
    my $rs;
    my $c_limit;
    my $w_limit;
    my @perfdata;
    my @msg_crit;
    my @msg_warn;
    my @msg;
    my @hosts;
    my @all_db;
    my %counts;
    my %new_counts;
    my $dbchecked  = 0;
    my $type       = $_[0];
    my %args       = %{ $_[1] };
    my @dbinclude  = @{ $args{'dbinclude'} };
    my @dbexclude  = @{ $args{'dbexclude'} };
    my $me         = 'POSTGRES_LAST_' . uc($type);
    my %queries    = (
        # 1st field: oldest known maintenance on a table
        #            -inf if a table never had maintenance
        #             NaN if nothing found
        # 2nd field: total number of maintenance
        # 3nd field: total number of auto-maintenance
        # 4th field: hash(insert||update||delete) to detect write
        #            activity between two run and avoid useless alerts
        #
        # 8.2 does not have per-database activity stats. We must aggregate
        # from pg_stat_user_tables
        $PG_VERSION_82 => qq{
            SELECT coalesce(min(
                extract(epoch FROM current_timestamp -
                    greatest(last_${type}, last_auto${type})
                )), 'NaN'::float),
                NULL, NULL,
                sum(hashtext(n_tup_ins::text
                    ||n_tup_upd::text
                    ||n_tup_del::text))
            FROM pg_stat_user_tables
            WHERE schemaname NOT LIKE 'pg_temp_%'
        },
        # Starting with 8.3, we can check database activity from
        # pg_stat_database
        $PG_VERSION_83 => qq{
            SELECT coalesce(min(
                extract(epoch FROM current_timestamp -
                    greatest(last_${type}, last_auto${type})
                )), 'NaN'::float),
                NULL, NULL,
                (
                  SELECT md5(tup_inserted::text||tup_updated::text||tup_deleted::text)
                  FROM pg_catalog.pg_stat_database
                  WHERE datname = current_database()
                )
            FROM pg_stat_user_tables
            WHERE schemaname NOT LIKE 'pg_temp_%'
              AND schemaname NOT LIKE 'pg_toast_temp_%'
        },
        $PG_VERSION_91 => qq{
            SELECT coalesce(min(
                       coalesce(extract(epoch FROM
                         current_timestamp -
                                 greatest(last_${type}, last_auto${type})),
                        '-infinity'::float)),
                   'NaN'::float),
                coalesce(sum(${type}_count), 0) AS ${type}_count,
                coalesce(sum(auto${type}_count), 0) AS auto${type}_count,
                (
                  SELECT md5(tup_inserted::text||tup_updated::text||tup_deleted::text)
                  FROM pg_catalog.pg_stat_database
                  WHERE datname = current_database()
                )
            FROM pg_stat_user_tables
            WHERE schemaname NOT LIKE 'pg_temp_%'
              AND schemaname NOT LIKE 'pg_toast_temp_%'
        }
    );

    # warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );

    $c_limit = get_time $args{'critical'};
    $w_limit = get_time $args{'warning'};

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => "FATAL: you must give only one host with service \"last_$type\".",
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], "last_$type", $PG_VERSION_82 or exit 1;

    # check required GUCs
    if ($hosts[0]->{'version_num'} < $PG_VERSION_83) {
        is_guc $hosts[0], 'stats_start_collector', 'on' or exit 1;
        is_guc $hosts[0], 'stats_row_level', 'on' or exit 1;
    }
    else {
        is_guc $hosts[0], 'track_counts', 'on' or exit 1;
    }

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    %counts = %{ load( $hosts[0], "${type}_counts", $args{'status-file'} ) || {} };

LOOP_DB: foreach my $db (@all_db) {
        my @perf;
        my $rs;

        next LOOP_DB if grep { $db =~ /$_/ } @dbexclude;
        next LOOP_DB if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        $dbchecked++;

        $rs = query_ver( $hosts[0], %queries, $db )->[0];
        $db =~ s/=//g;

        push @perfdata => [ $db, $rs->[0], 's', $w_limit, $c_limit ];

        $new_counts{$db} = [ $rs->[1], $rs->[2] ];

        if ( exists $counts{$db} ) {

            if ($hosts[0]->{'version_num'} >= $PG_VERSION_91 ) {
                my $delta      = $rs->[1] - $counts{$db}[0];
                my $delta_auto = $rs->[2] - $counts{$db}[1];

                push @perfdata => (
                    [ "$db $type", $delta ],
                    [ "$db auto$type", $delta_auto ]
                );
            }

            # avoid alerts if no write activity since last call
            if ( defined $counts{$db}[2] and $counts{$db}[2] eq $rs->[3] ) {
                # keep old hashed status for this database
                $new_counts{$db}[2] = $counts{$db}[2];
                next LOOP_DB;
            }
        }

        if ( $rs->[0] =~ /^-inf/i or $rs->[0] >= $c_limit ) {
            push @msg_crit => "$db: " . to_interval($rs->[0]);
            next LOOP_DB;
        }

        if ( $rs->[0] >= $w_limit ) {
            push @msg_warn => "$db: " . to_interval($rs->[0]);
            next LOOP_DB;
        }

        # iif everything is OK, save the current hashed status for this database
        $new_counts{$db}[2] = $rs->[3];
    }

    save $hosts[0], "${type}_counts", \%new_counts, $args{'status-file'};

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if scalar @msg_warn > 0;

    return status_ok( $me, [ "$dbchecked database(s) checked" ], \@perfdata );
}


=item B<last_analyze> (8.2+)

Check on each databases that the oldest C<analyze> (from autovacuum or not) is
not older than the given threshold.

This service uses the status file (see C<--status-file> parameter) with
PostgreSQL 9.1+.

Perfdata returns oldest C<analyze> per database in seconds. With PostgreSQL
9.1+, the number of [auto]analyses per database since last call is also
returned.

Critical and Warning thresholds only accept an interval (eg. 1h30m25s)
and apply to the oldest execution of analyse.

Tables that were never analyzed, or whose analyze date was lost due to a crash,
will raise a critical alert.

B<NOTE>: this service does not raise alerts if the database had strictly
no writes since last call. In consequence, a read-only database can have
its oldest analyze reported in perfdata way after your thresholds, but not
raise any alerts.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

Required privileges: unprivileged role able to log in all databases.

=cut

sub check_last_analyze {
    return check_last_maintenance( 'analyze', @_ );
}


=item B<last_vacuum> (8.2+)

Check that the oldest vacuum (from autovacuum or otherwise) in each database
in the cluster is not older than the given threshold.

This service uses the status file (see C<--status-file> parameter) with
PostgreSQL 9.1+.

Perfdata returns oldest vacuum per database in seconds. With PostgreSQL
9.1+, it also returns the number of [auto]vacuums per database since last
execution.

Critical and Warning thresholds only accept an interval (eg. 1h30m25s)
and apply to the oldest vacuum.

Tables that were never vacuumed, or whose vacuum date was lost due to a crash,
will raise a critical alert.

B<NOTE>: this service does not raise alerts if the database had strictly
no writes since last call. In consequence, a read-only database can have
its oldest vacuum reported in perfdata way after your thresholds, but not
raise any alerts.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

Required privileges: unprivileged role able to log in all databases.

=cut

sub check_last_vacuum {
    return check_last_maintenance( 'vacuum', @_ );
}


=item B<locks> (all)

Check the number of locks on the hosts.

Perfdata returns the number of locks, by type.

Critical and Warning thresholds accept either a raw number of locks or a
percentage. For percentage, it is computed using the following limits
for 7.4 to 8.1:

  max_locks_per_transaction * max_connections

for 8.2+:

  max_locks_per_transaction * (max_connections + max_prepared_transactions)

for 9.1+, regarding lockmode :

  max_locks_per_transaction * (max_connections + max_prepared_transactions)
or max_pred_locks_per_transaction * (max_connections + max_prepared_transactions)

Required privileges: unprivileged role.

=cut

sub check_locks {
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my %args          = %{ $_[0] };
    my $total_locks   = 0;
    my $total_pred_locks   = 0;
    my $waiting_locks = 0;
    my $me            = 'POSTGRES_LOCKS';
    my %queries       = (
        $PG_VERSION_74 => q{
            SELECT count(l.granted), ref.mode,
                current_setting('max_locks_per_transaction')::integer
                * current_setting('max_connections')::integer, 0, ref.granted
            FROM (
                SELECT 'AccessShareLock',                't'::boolean
                UNION SELECT 'RowShareLock',             't'
                UNION SELECT 'RowExclusiveLock',         't'
                UNION SELECT 'ShareUpdateExclusiveLock', 't'
                UNION SELECT 'ShareLock',                't'
                UNION SELECT 'ShareRowExclusiveLock',    't'
                UNION SELECT 'ExclusiveLock',            't'
                UNION SELECT 'AccessExclusiveLock',      't'
                UNION SELECT 'AccessShareLock',          'f'
                UNION SELECT 'RowShareLock',             'f'
                UNION SELECT 'RowExclusiveLock',         'f'
                UNION SELECT 'ShareUpdateExclusiveLock', 'f'
                UNION SELECT 'ShareLock',                'f'
                UNION SELECT 'ShareRowExclusiveLock',    'f'
                UNION SELECT 'ExclusiveLock',            'f'
                UNION SELECT 'AccessExclusiveLock',      'f'
            ) ref (mode, granted)
            LEFT JOIN pg_locks l
                ON (ref.mode, ref.granted) = (l.mode, l.granted)
            GROUP BY 2,3,4,5
            ORDER BY ref.granted, ref.mode
        },
        $PG_VERSION_82 => q{
            SELECT count(l.granted), ref.mode,
                current_setting('max_locks_per_transaction')::integer * (
                    current_setting('max_prepared_transactions')::integer
                    + current_setting('max_connections')::integer), 0, ref.granted
            FROM (SELECT * FROM ( VALUES
                ('AccessShareLock',          't'::boolean),
                ('RowShareLock',             't'),
                ('RowExclusiveLock',         't'),
                ('ShareUpdateExclusiveLock', 't'),
                ('ShareLock',                't'),
                ('ShareRowExclusiveLock',    't'),
                ('ExclusiveLock',            't'),
                ('AccessExclusiveLock',      't'),
                ('AccessShareLock',          'f'),
                ('RowShareLock',             'f'),
                ('RowExclusiveLock',         'f'),
                ('ShareUpdateExclusiveLock', 'f'),
                ('ShareLock',                'f'),
                ('ShareRowExclusiveLock',    'f'),
                ('ExclusiveLock',            'f'),
                ('AccessExclusiveLock',      'f')
                ) lockmode (mode, granted)
            ) ref
            LEFT JOIN pg_locks l
                ON (ref.mode, ref.granted) = (l.mode, l.granted)
            GROUP BY 2,3,4,5
            ORDER BY ref.granted, ref.mode
        },
        $PG_VERSION_91 => q{
            SELECT count(l.granted), ref.mode,
                current_setting('max_locks_per_transaction')::integer * (
                    current_setting('max_prepared_transactions')::integer
                    + current_setting('max_connections')::integer),
                current_setting('max_pred_locks_per_transaction')::integer * (
                    current_setting('max_prepared_transactions')::integer
                    + current_setting('max_connections')::integer), ref.granted
            FROM (SELECT * FROM ( VALUES
                ('AccessShareLock',          't'::boolean),
                ('RowShareLock',             't'),
                ('RowExclusiveLock',         't'),
                ('ShareUpdateExclusiveLock', 't'),
                ('ShareLock',                't'),
                ('ShareRowExclusiveLock',    't'),
                ('ExclusiveLock',            't'),
                ('AccessExclusiveLock',      't'),
                ('AccessShareLock',          'f'),
                ('RowShareLock',             'f'),
                ('RowExclusiveLock',         'f'),
                ('ShareUpdateExclusiveLock', 'f'),
                ('ShareLock',                'f'),
                ('ShareRowExclusiveLock',    'f'),
                ('ExclusiveLock',            'f'),
                ('AccessExclusiveLock',      'f'),
                ('SIReadLock',               't')
                ) lockmode (mode, granted)
            ) ref
            LEFT JOIN pg_locks l
                ON (ref.mode, ref.granted) = (l.mode, l.granted)
            GROUP BY 2,3,4,5
            ORDER BY ref.granted, ref.mode
        }
    );

    # warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    # warning and critical must be raw or %.
    pod2usage(
        -message => "FATAL: critical and warning thresholds only accept raw numbers or %.",
        -exitval => 127
    ) unless $args{'warning'}  =~ m/^([0-9.]+)%?$/
        and  $args{'critical'} =~ m/^([0-9.]+)%?$/;


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "locks".',
        -exitval => 127
    ) if @hosts != 1;


    @rs = @{ query_ver $hosts[0], %queries };

    $args{'predcritical'} = $args{'critical'};
    $args{'predwarning'} = $args{'warning'};

    $args{'critical'} = int($1 * $rs[0][2]/100) if $args{'critical'} =~ /^([0-9.]+)%$/;
    $args{'warning'}  = int($1 * $rs[0][2]/100) if $args{'warning'}  =~ /^([0-9.]+)%$/;
    $args{'predcritical'} = int($1 * $rs[0][3]/100) if $args{'predcritical'} =~ /^([0-9.]+)%$/;
    $args{'predwarning'}  = int($1 * $rs[0][3]/100) if $args{'predwarning'}  =~ /^([0-9.]+)%$/;

    map {
        $total_locks += $_->[0] if $_->[1] ne 'SIReadLock';
        $total_pred_locks += $_->[0] if $_->[1] eq 'SIReadLock';
        if ($_->[4] eq 't') {
            if ($_->[1] ne 'SIReadLock') {
                push @perfdata =>
                    [ $_->[1], $_->[0], undef, $args{'warning'}, $args{'critical'} ];
            } else {
                push @perfdata =>
                    [ $_->[1], $_->[0], undef, $args{'predwarning'}, $args{'predcritical'} ];
            }
        }
        else {
            $waiting_locks += $_->[0];
            push @perfdata =>
                [ "Waiting $_->[1]", $_->[0], undef, $args{'warning'}, $args{'critical'} ];
        }
    } @rs;

    push @msg => "$total_locks locks, $total_pred_locks predicate locks, $waiting_locks waiting locks";

    return status_critical( $me, \@msg, \@perfdata )
        if $total_locks >= $args{'critical'} or ( $hosts[0]->{'version_num'} >= $PG_VERSION_91 and $total_pred_locks >= $args{'predcritical'} );

    return status_warning( $me, \@msg, \@perfdata )
        if $total_locks >= $args{'warning'} or ( $hosts[0]->{'version_num'} >= $PG_VERSION_91 and $total_pred_locks >= $args{'predwarning'});

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<longest_query> (all)

Check the longest running query in the cluster.

Perfdata contains the max/avg/min running time and the number of queries per
database.

Critical and Warning thresholds only accept an interval.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.

It also supports argument C<--exclude REGEX> to exclude queries matching the
given regular expression from the check.

Above 9.0, it also supports C<--exclude REGEX> to filter out application_name.

You can use multiple C<--exclude REGEX> parameters.

Required privileges: an unprivileged role only checks its own queries;
a pg_monitor (10+) or superuser (<10) role is required to check all queries.

=cut

sub check_longest_query {
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my $c_limit;
    my $w_limit;
    my %args          = %{ $_[0] };
    my @dbinclude     = @{ $args{'dbinclude'} };
    my @dbexclude     = @{ $args{'dbexclude'} };
    my $me            = 'POSTGRES_LONGEST_QUERY';
    my $longest_query = 0;
    my $nb_query      = 0;
    my %stats         = ();
    my %queries       = (
       $PG_VERSION_74 => q{SELECT d.datname,
                COALESCE(elapsed, -1),
                COALESCE(query, '')
            FROM pg_database AS d
            LEFT JOIN (
                SELECT datname, current_query AS query,
                    extract('epoch' FROM
                        date_trunc('second', current_timestamp-query_start)
                    ) AS elapsed
                FROM pg_stat_activity
                WHERE current_query NOT LIKE '<IDLE>%'
            ) AS s ON (d.datname=s.datname)
            WHERE d.datallowconn
        },
        $PG_VERSION_90 => q{SELECT d.datname,
                COALESCE(elapsed, -1),
                COALESCE(query, ''),
                application_name
            FROM pg_database AS d
            LEFT JOIN (
                SELECT datname, current_query AS query,
                    extract('epoch' FROM
                        date_trunc('second', current_timestamp-query_start)
                    ) AS elapsed,
                    application_name
                FROM pg_stat_activity
                WHERE current_query NOT LIKE '<IDLE>%'
            ) AS s ON (d.datname=s.datname)
            WHERE d.datallowconn
        },
        $PG_VERSION_92 => q{SELECT d.datname,
                COALESCE(elapsed, 0),
                COALESCE(query, ''),
                application_name
            FROM pg_database AS d
            LEFT JOIN (
                SELECT datname, query,
                    extract('epoch' FROM
                        date_trunc('second', current_timestamp-state_change)
                    ) AS elapsed,
                    application_name
                FROM pg_stat_activity
                WHERE state = 'active'
            ) AS s ON (d.datname=s.datname)
            WHERE d.datallowconn
        }
    );

    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );


    $c_limit = get_time( $args{'critical'} );
    $w_limit = get_time( $args{'warning'} );


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "longest_query".',
        -exitval => 127
    ) if @hosts != 1;


    @rs = @{ query_ver( $hosts[0], %queries ) };

    REC_LOOP: foreach my $r (@rs) {

        # exclude/include on db name
        next REC_LOOP if grep { $r->[0] =~ /$_/ } @dbexclude;
        next REC_LOOP if @dbinclude and not grep { $r->[0] =~ /$_/ } @dbinclude;

        # exclude on query text
        foreach my $exclude_re ( @{ $args{'exclude'} } ) {
            next REC_LOOP if $r->[2] =~ /$exclude_re/;
            next REC_LOOP if defined($r->[3]) && $r->[3]  =~ /$exclude_re/;
        }

        $stats{$r->[0]} = {
            'num' => 0,
            'max' => -1,
            'avg' => 0,
        } unless exists $stats{$r->[0]};

        next REC_LOOP unless $r->[2] ne '';

        $longest_query = $r->[1] if $r->[1] > $longest_query;
        $nb_query++;

        $stats{$r->[0]}{'num'}++;
        $stats{$r->[0]}{'max'} = $r->[1] if $stats{$r->[0]}{'max'} < $r->[1];
        $stats{$r->[0]}{'avg'} = (
            $stats{$r->[0]}{'avg'} * ($stats{$r->[0]}{'num'} -1) + $r->[1])
            / $stats{$r->[0]}{'num'};
    }

    DB_LOOP: foreach my $db (keys %stats) {

        unless($stats{$db}{'max'} > -1) {
            $stats{$db}{'max'} = 'NaN';
            $stats{$db}{'avg'} = 'NaN';
        }

        push @perfdata, (
            [ "$db max", $stats{$db}{'max'}, 's', $w_limit, $c_limit ],
            [ "$db avg", $stats{$db}{'avg'}, 's', $w_limit, $c_limit ],
            [ "$db #queries", $stats{$db}{'num'} ]
        );

        if ( $stats{$db}{'max'} > $c_limit ) {
            push @msg => "$db: ". to_interval($stats{$db}{'max'});
            next DB_LOOP;
        }

        if ( $stats{$db}{'max'} > $w_limit ) {
            push @msg => "$db: ". to_interval($stats{$db}{'max'});
        }
    }

    return status_critical( $me, \@msg, \@perfdata )
        if $longest_query > $c_limit;

    return status_warning( $me, \@msg, \@perfdata )
        if $longest_query > $w_limit;

    return status_ok( $me, [ "$nb_query running querie(s)" ], \@perfdata );
}


=item B<max_freeze_age> (all)

Checks oldest database by transaction age.

Critical and Warning thresholds are optional. They accept either a raw number
or percentage for PostgreSQL 8.2 and more. If percentage is given, the
thresholds are computed based on the "autovacuum_freeze_max_age" parameter.
100% means that some table(s) reached the maximum age and will trigger an
autovacuum freeze. Percentage thresholds should therefore be greater than 100%.

Even with no threshold, this service will raise a critical alert if a database
has a negative age.

Perfdata returns the age of each database.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.

Required privileges: unprivileged role.

=cut

sub check_max_freeze_age {
    my @rs;
    my @perfdata;
    my @msg;
    my @msg_crit;
    my @msg_warn;
    my @hosts;
    my $c_limit;
    my $w_limit;
    my $oldestdb;
    my $oldestage     = -1;
    my %args          = %{ $_[0] };
    my @dbinclude     = @{ $args{'dbinclude'} };
    my @dbexclude     = @{ $args{'dbexclude'} };
    my $me            = 'POSTGRES_MAX_FREEZE_AGE';
    my %queries       = (
       $PG_VERSION_74 => q{SELECT datname, age(datfrozenxid)
            FROM pg_database
            WHERE datname <> 'template0'
       },
        $PG_VERSION_82 => q{SELECT datname, age(datfrozenxid),
                current_setting('autovacuum_freeze_max_age')
            FROM pg_database
            WHERE datname <> 'template0'
        }
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "max_freeze_age".',
        -exitval => 127
    ) if @hosts != 1;

    # warning and critical must be raw or %.
    if ( defined $args{'warning'} and defined $args{'critical'} ) {
        # warning and critical must be raw
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept raw numbers or % (for 8.2+).",
            -exitval => 127
        ) unless $args{'warning'}  =~ m/^([0-9]+)%?$/
            and  $args{'critical'} =~ m/^([0-9]+)%?$/;

        $w_limit = $args{'warning'};
        $c_limit = $args{'critical'};

        set_pgversion($hosts[0]);

        pod2usage(
            -message => "FATAL: only raw thresholds are compatible with PostgreSQL 8.1 and below.",
            -exitval => 127
        ) if $hosts[0]->{'version_num'} < $PG_VERSION_82
            and ($args{'warning'} =~ m/%\s*$/ or $args{'critical'} =~ m/%\s*$/);
    }

    @rs = @{ query_ver( $hosts[0], %queries ) };

    if ( scalar @rs and defined $args{'critical'} ) {
        $c_limit = int($1 * $rs[0][2]/100) if $args{'critical'} =~ /^([0-9.]+)%$/;
        $w_limit = int($1 * $rs[0][2]/100) if $args{'warning'}  =~ /^([0-9.]+)%$/;
    }

    REC_LOOP: foreach my $r (@rs) {
        my @perf;

        next REC_LOOP if grep { $r->[0] =~ /$_/ } @dbexclude;
        next REC_LOOP if @dbinclude and not grep { $r->[0] =~ /$_/ } @dbinclude;

        if ($oldestage < $r->[1]) {
            $oldestdb = $r->[0];
            $oldestage = $r->[1];
        }

        @perf = ( $r->[0], $r->[1] );

        push @perf => ( undef, $w_limit, $c_limit ) if defined $c_limit;

        push @perfdata => [ @perf ];

        if ( $r->[1] < 0 ) {
            push @msg_crit => "$r->[0] has a negative age" ;
            next REC_LOOP;
        }

        if ( defined $c_limit ) {
            if ( $r->[1] > $c_limit ) {
                push @msg_crit => "$r->[0]";
                next REC_LOOP;
            }

            push @msg_warn => "$r->[0]"
                if defined $w_limit and $r->[1] > $w_limit;
        }
    }

    return status_critical( $me, [
            'Critical: '. join(',', @msg_crit)
            . (scalar @msg_warn? ' Warning: '. join(',', @msg_warn):'')
        ], \@perfdata
    ) if scalar @msg_crit;

    return status_warning( $me,
        [ 'Warning: '. join(',', @msg_warn) ], \@perfdata
    ) if scalar @msg_warn;

    return status_ok( $me, [ "oldest database is $oldestdb with age of $oldestage" ], \@perfdata );
}


=item B<minor_version> (all)

Check if the cluster is running the most recent minor version of PostgreSQL.

Latest versions of PostgreSQL can be fetched from PostgreSQL official
website if check_pgactivity has access to it, or must be given as a parameter.

Without C<--critical> or C<--warning> parameters, this service attempts
to fetch the latest version numbers online. A critical alert is raised if the
minor version is not the most recent.

You can optionally set the path to your prefered retrieval tool using
the C<--path> parameter (eg. C<--path '/usr/bin/wget'>). Supported programs are:
GET, wget, curl, fetch, lynx, links, links2.

If you do not want to (or cannot) query the PostgreSQL website,
provide the expected versions using either C<--warning> OR
C<--critical>, depending on which return value you want to raise.

The given string must contain one or more MINOR versions separated by anything
but a '.'. For instance, the following parameters are all equivalent:

  --critical "10.1 9.6.6 9.5.10 9.4.15 9.3.20 9.2.24 9.1.24 9.0.23 8.4.22"
  --critical "10.1, 9.6.6, 9.5.10, 9.4.15, 9.3.20, 9.2.24, 9.1.24, 9.0.23, 8.4.22"
  --critical "10.1,9.6.6,9.5.10,9.4.15,9.3.20,9.2.24,9.1.24,9.0.23,8.4.22"
  --critical "10.1/9.6.6/9.5.10/9.4.15/9.3.20/9.2.24/9.1.24/9.0.23/8.4.22"

Any other value than 3 numbers separated by dots (before version 10.x)
or 2 numbers separated by dots (version 10 and above) will be ignored.
If the running PostgreSQL major version is not found, the service raises an
unknown status.

Perfdata returns the numerical version of PostgreSQL.

Required privileges: unprivileged role; access to http://www.postgresql.org
required to download version numbers.

=cut

sub check_minor_version {
    my @perfdata;
    my @msg;
    my %latest_versions;
    my $rss;
    my @hosts;
    my $major_version;
    my %args    = %{ $_[0] };
    my $me      = 'POSTGRES_MINOR_VERSION';
    my $timeout = get_time($args{'timeout'});
    my $url     = 'http://www.postgresql.org/versions.rss';

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "minor_version".',
        -exitval => 127
    ) if @hosts != 1;

    set_pgversion($hosts[0]);

    if (not defined $args{'warning'}
        and not defined $args{'critical'}
    ) {
        # These methods come from check_postgres,
        # by Greg Sabino Mullane <greg@endpoint.com>,
        # licenced under BSD
        our %get_methods = (
            'GET'    => "GET -t $timeout -H 'Pragma: no-cache' $url",
            'wget'   => "wget --quiet --timeout=$timeout --no-cache -O - $url",
            'curl'   => "curl --silent --location --max-time $timeout -H 'Pragma: no-cache' $url",
            'fetch'  => "fetch -q -T $timeout -o - $url",
            'lynx'   => "lynx --connect-timeout=$timeout --dump $url",
            'links'  => 'links -dump $url',
            'links2' => 'links2 -dump $url'
        );

        # Force the fetching method
        if ($args{'path'}) {
            my $meth = basename $args{'path'};

            pod2usage(
                -message => "FATAL: \"$args{'path'}\" is not a valid program.",
                -exitval => 127
            ) unless -x $args{'path'};

            pod2usage(
                -message => "FATAL: \"$args{'path'}\" is not a supported program.",
                -exitval => 127
            ) unless $meth =~ 'GET|wget|curl|fetch|lynx|links|links2';

            # fetch the latest versions via $path
            $rss = qx{$get_methods{$meth} 2>/dev/null};
        }
        else {
            # Fetch the latest versions
            foreach my $exe (values %get_methods) {
                $rss = qx{$exe 2>/dev/null};

                last if $rss =~ 'PostgreSQL latest versions';
            }
        }

        return status_unknown( $me, [ 'Could not fetch PostgreSQL latest versions' ] )
            unless $rss;

        # Versions until 9.6
        $latest_versions{"$1.$2"} = [$1 * 10000 + $2 * 100 + $3, "$1.$2.$3"]
            while ($rss =~ m/<title>(\d+)\.(\d+)\.(\d+)/g  && $1<10);
        # Versions from 10
        $latest_versions{"$1"} = [$1 * 10000 + $2, "$1.$2"]
            while ($rss =~ m/<title>(\d+)\.(\d+)/g && $1>=10);

    }
    else {
        pod2usage(
            -message => 'FATAL: you must provide a warning OR a critical threshold for service minor_version!',
            -exitval => 127
        ) if defined $args{'critical'} and defined $args{'warning'};

        my $given_version = defined $args{'critical'} ?
            $args{'critical'}
            : $args{'warning'};

        while ( $given_version =~ m/(\d+)\.(\d+)\.(\d*)/g ) {
            $latest_versions{"$1.$2"} = [$1 * 10000 + $2 * 100 + $3, "$1.$2.$3"] if $1<10 ; # v9.6.5=90605
        }
        while ( $given_version =~ m/(\d+)\.(\d+)/g ) {
            $latest_versions{"$1"} = [$1 * 10000 + $2, "$1.$2"] if $1>=10 ;  # v10.1 = 100001, v11.0=110000
        }
    }
    if ( $hosts[0]{'version_num'} < 100000 ) {
      #eg 90605 for 9.6.5 -> major is 9.6
      $hosts[0]{'version'} =~ '^(\d+\.\d+).*$';
      $major_version = $1;
    } else {
      # eg 100001 for 10.1  -> major is 10
      $major_version = int($hosts[0]{'version_num'}/10000) ;
    }
    dprint ("major version: $major_version\n");

    unless ( defined $latest_versions{$major_version} ) {
        push @msg => "Unknown major PostgreSQL version $major_version";
        return status_unknown( $me, \@msg );
    }

    push @perfdata => [ 'version', $hosts[0]{'version_num'}, 'PGNUMVER' ];

    if ( $hosts[0]{'version_num'} != $latest_versions{$major_version}[0] ) {
        push @msg => "PostgreSQL version ". $hosts[0]{'version'}
            ." (should be $latest_versions{$major_version}[1])";

        return status_warning( $me, \@msg, \@perfdata ) if defined $args{'warning'};
        return status_critical( $me, \@msg, \@perfdata );
    }

    push @msg => "PostgreSQL version ". $hosts[0]{'version'};

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<oldest_2pc> (8.1+)

Check the oldest I<two-phase commit transaction> (aka. prepared transaction) in
the cluster.

Perfdata contains the max/avg age time and the number of prepared
transactions per databases.

Critical and Warning thresholds only accept an interval.

Required privileges: unprivileged role.

=cut

sub check_oldest_2pc {
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my $c_limit;
    my $w_limit;
    my $me          = 'POSTGRES_OLDEST_2PC';
    my $oldest_2pc = 0;
    my $nb_2pc      = 0;
    my %stats       = ();
    my $query         = q{SELECT transaction, gid,
            coalesce(extract('epoch' FROM
                    date_trunc('second', current_timestamp-prepared)
                ), -1),
            owner, d.datname
        FROM pg_database AS d
        LEFT JOIN pg_prepared_xacts AS x
            ON d.datname=x.database
        WHERE d.datallowconn
    };


    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );


    $c_limit = get_time $args{'critical'};
    $w_limit = get_time $args{'warning'};


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "oldest_2pc".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'postgres_oldest_2pc', $PG_VERSION_81 or exit 1;


    @rs = @{ query( $hosts[0], $query ) };

    REC_LOOP: foreach my $r (@rs) {

        $stats{$r->[4]} = {
            'num' => 0,
            'max' => -1,
            'avg' => 0,
        } unless exists $stats{$r->[4]};

        $oldest_2pc = $r->[2] if $r->[2] > $oldest_2pc;

        $stats{$r->[4]}{'num'}++ if $r->[0];
        $stats{$r->[4]}{'max'} = $r->[2] if $stats{$r->[4]}{'max'} < $r->[2];
        $stats{$r->[4]}{'avg'} = (
            $stats{$r->[4]}{'avg'} * ($stats{$r->[4]}{'num'} -1) + $r->[2])
            / $stats{$r->[4]}{'num'} if $stats{$r->[4]}{'num'};
    }

    DB_LOOP: foreach my $db (sort keys %stats) {

        $nb_2pc += $stats{$db}{'num'};

        unless($stats{$db}{'max'} > -1) {
            $stats{$db}{'max'} = 'NaN';
            $stats{$db}{'avg'} = 'NaN';
        }

        push @perfdata, (
            [ "$db max", $stats{$db}{'max'}, 's', $w_limit, $c_limit ],
            [ "$db avg", $stats{$db}{'avg'}, 's', $w_limit, $c_limit ],
            [ "$db # prep. xact", $stats{$db}{'num'} ]
        );

        if ( $stats{$db}{'max'} > $c_limit ) {
            push @msg => "oldest 2pc on $db: ". to_interval($stats{$db}{'max'});
            next DB_LOOP;
        }

        if ( $stats{$db}{'max'} > $w_limit ) {
            push @msg => "oldest 2pc on $db: ". to_interval($stats{$db}{'max'});
        }
    }

    unshift @msg => "$nb_2pc prepared transaction(s)";

    return status_critical( $me, \@msg, \@perfdata )
        if $oldest_2pc > $c_limit;

    return status_warning( $me, \@msg, \@perfdata )
        if $oldest_2pc > $w_limit;

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<oldest_idlexact> (8.3+)

Check the oldest I<idle> transaction.

Perfdata contains the max/avg age and the number of idle transactions
per databases.

Critical and Warning thresholds only accept an interval.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.

Above 9.2, it supports C<--exclude> to filter out connections. Eg., to
filter out pg_dump and pg_dumpall, set this to 'pg_dump,pg_dumpall'.

Required privileges: an unprivileged role checks only its own queries;
a pg_monitor (10+) or superuser (<10) role is required to check all queries.

=cut

sub check_oldest_idlexact {
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my $c_limit;
    my $w_limit;
    my %args        = %{ $_[0] };
    my @dbinclude   = @{ $args{'dbinclude'} };
    my @dbexclude   = @{ $args{'dbexclude'} };
    my $me          = 'POSTGRES_OLDEST_IDLEXACT';
    my $oldest_idle = 0;
    my $nb_idle     = 0;
    my %stats       = ( );
    my %queries     = (
        $PG_VERSION_83 => q{SELECT d.datname,
            coalesce(extract('epoch' FROM
                    date_trunc('second', current_timestamp-xact_start)
                ), -1)
            FROM pg_database AS d
            LEFT JOIN pg_stat_activity AS a ON (a.datid = d.oid AND current_query = '<IDLE> in transaction')},
        $PG_VERSION_92 => q{SELECT d.datname,
            coalesce(extract('epoch' FROM
                    date_trunc('second', current_timestamp-xact_start)
                ), -1)
            FROM pg_database AS d
            LEFT JOIN pg_stat_activity AS a ON (a.datid = d.oid AND state='idle in transaction')
        }
    );

    # Exclude some apps
    if(defined $args{'exclude'}[0]){
        $queries{$PG_VERSION_92}.=" WHERE a.application_name NOT IN ('".join("','", split(',', $args{'exclude'}[0]))."')";
    }

    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    pod2usage(
        -message => "FATAL: critical and warning thresholds only acccepts interval.",
        -exitval => 127
    ) unless ( is_time( $args{'warning'} ) and is_time( $args{'critical'} ) );


    $c_limit = get_time $args{'critical'};
    $w_limit = get_time $args{'warning'};


    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "oldest_idlexact".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'oldest_idlexact', $PG_VERSION_83 or exit 1;


    @rs = @{ query_ver( $hosts[0], %queries ) };

    REC_LOOP: foreach my $r (@rs) {

        $stats{$r->[0]} = {
            'num' => 0,
            'max' => -1,
            'avg' => 0,
        } unless exists $stats{$r->[0]};

        $oldest_idle = $r->[1] if $r->[1] > $oldest_idle;

        $stats{$r->[0]}{'num'}++ if $r->[1] > -1;
        $stats{$r->[0]}{'max'} = $r->[1] if $stats{$r->[0]}{'max'} < $r->[1];
        $stats{$r->[0]}{'avg'} = (
            $stats{$r->[0]}{'avg'} * ($stats{$r->[0]}{'num'} -1) + $r->[1])
            / $stats{$r->[0]}{'num'} if $stats{$r->[0]}{'num'};
    }

    DB_LOOP: foreach my $db (sort keys %stats) {

        next DB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next DB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        $nb_idle += $stats{$db}{'num'};

        unless($stats{$db}{'max'} > -1) {
            $stats{$db}{'max'} = 'NaN';
            $stats{$db}{'avg'} = 'NaN';
        }

        push @perfdata, (
            [ "$db max", $stats{$db}{'max'}, 's', $w_limit, $c_limit ],
            [ "$db avg", $stats{$db}{'avg'}, 's', $w_limit, $c_limit ],
            [ "$db # idle xact", $stats{$db}{'num'} ]
        );

        if ( $stats{$db}{'max'} > $c_limit ) {
            push @msg => "oldest idle xact on $db: ". to_interval($stats{$db}{'max'});
            next DB_LOOP;
        }

        if ( $stats{$db}{'max'} > $w_limit ) {
            push @msg => "oldest idle xact on $db: ". to_interval($stats{$db}{'max'});
        }
    }

    unshift @msg => "$nb_idle idle transaction(s)";

    return status_critical( $me, \@msg, \@perfdata )
        if $oldest_idle > $c_limit;

    return status_warning( $me, \@msg, \@perfdata )
        if $oldest_idle > $w_limit;

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<oldest_xmin> (8.4+)

Check the xmin I<horizon> from distinct sources of xmin retention.

Per default, Perfdata outputs the oldest known xmin age for each database among
running queries, opened or idle transactions, pending prepared transactions,
replication slots and walsender. For versions prior to 9.4, only C<2pc> source
of xmin retention is checked.

Using C<--detailed>, Perfdata contains the oldest xmin and maximum age for the
following source of xmin retention: C<query> (a running query), C<active_xact>
(an opened transaction currently executing a query), C<idle_xact> (an opened
transaction being idle), C<2pc> (a pending prepared transaction), C<repslot> (a
replication slot) and C<walwender> (a WAL sender replication process), for each
connectable database.  If a source doesn't retain any transaction for a
database, NaN is returned.  For versions prior to 9.4, only C<2pc> source of
xmin retention is available, so other sources won't appear in the perfdata.
Note that xmin retention from walsender is only set if C<hot_standby_feedback>
is enabled on remote standby.

Critical and Warning thresholds are optional. They only accept a raw number of
transaction.

This service supports both C<--dbexclude>" and C<--dbinclude>" parameters.

Required privileges: a pg_read_all_stats (10+) or superuser (<10) role is
required to check pg_stat_replication.  2PC, pg_stat_activity, and replication
slots don't require special privileges.

=cut

sub check_oldest_xmin {
    my @rs;
    my @perfdata;
    my @msg;
    my @msg_crit;
    my @msg_warn;
    my @hosts;
    my $detailed;
    my $c_limit;
    my $w_limit;
    my %oldest_xmin; # track oldest xmin and its kind for each database
    my %args      = %{ $_[0] };
    my $me        = 'POSTGRES_OLDEST_XMIN';
    my @dbinclude = @{ $args{'dbinclude'} };
    my @dbexclude = @{ $args{'dbexclude'} };
    my %queries   = (
        # 8.4 is the first supported version as we rely on window functions to
        # get the oldest xmin.  Only 2PC has transaction information available
        $PG_VERSION_84 => q{
        WITH ordered AS (
          SELECT '2pc' AS kind,
          d.datname,
          -- xid type doesn't have range operators as the value will wraparound.
          -- Instead, rely on age() function and row_number() window function
          -- to get the oldest xid found.
          row_number() OVER (
            PARTITION BY d.datname
            ORDER BY age(transaction) DESC NULLS LAST
          ) rownum, age(transaction) AS age,
          transaction AS xmin
          FROM (SELECT transaction, database FROM pg_prepared_xacts
            UNION ALL SELECT NULL, NULL
          ) sql(transaction, datname)
          -- we use this JOIN condition to make sure that we'll always have a
          -- full record for all (connectable) databases
          JOIN pg_database d ON d.datname = coalesce(sql.datname, d.datname)
          WHERE d.datallowconn
        )
        SELECT datname, kind, age, xmin FROM ordered
        WHERE rownum = 1
        },
        # backend_xmin and backend_xid added to pg_stat_activity,
        # backend_xmin added to pg_stat_replication,
        # replication slots introduced
        $PG_VERSION_94 => q{
        WITH raw AS (
          -- regular backends
          SELECT
          CASE WHEN xact_start = query_start
            THEN 'query'
            ELSE
              CASE WHEN state = 'idle in transaction'
                THEN 'idle_xact'
                ELSE 'active_xact'
              END
          END AS kind,
          datname,
          coalesce(backend_xmin, backend_xid) AS xmin
          FROM pg_stat_activity
          -- exclude ourselves, as a blocked xmin in another database would be
          -- exposed in the database we're connecting too, which may otherwise
          -- not have the same xmin
          WHERE pid != pg_backend_pid()
          UNION ALL (
            -- 2PC
            SELECT '2pc' AS kind,
            database AS datname,
            transaction AS xmin
            FROM  pg_prepared_xacts
          ) UNION ALL (
            -- replication slots
            SELECT 'repslot' AS kind,
            database AS datname,
            xmin AS xmin
            FROM  pg_replication_slots
          ) UNION ALL (
            -- walsenders
            SELECT 'walsender' AS kind,
            NULL AS datname,
            backend_xmin AS xmin
            FROM pg_stat_replication
          )
        ),
        ordered AS (
          SELECT kind, datname,
          -- xid type doesn't have range operators as the value will wraparound.
          -- Instead, rely on age() function and row_number() window function
          -- to get the oldest xid found.
          row_number() OVER (
            PARTITION BY kind, datname
            ORDER BY age(xmin) DESC NULLS LAST
          ) rownum, age(xmin) AS age, xmin
          FROM raw
        )
        SELECT f.datname, f.kind, o.age, o.xmin
        FROM ordered AS o
        RIGHT JOIN (
          SELECT d.datname, v.kind
          FROM pg_catalog.pg_database d,
          (VALUES
            ( 'query'       ),
            ( 'idle_xact'   ),
            ( 'active_xact' ),
            ( '2pc'         ),
            ( 'repslot'     ),
            ( 'walsender'   )
          ) v(kind)
          WHERE d.datallowconn
        ) f ON o.datname = f.datname
           AND o.kind = f.kind
        WHERE coalesce(o.rownum, 1) = 1
        }
    );

    # Either both warning and critical are required or none.
    pod2usage(
        -message => "FATAL: you must specify both critical and warning thresholds or none of them.",
        -exitval => 127
    ) unless (    defined $args{'warning'} and     defined $args{'critical'})
        or   (not defined $args{'warning'} and not defined $args{'critical'});

    if ( defined $args{'critical'} ) {

        $c_limit  = $args{'critical'};
        $w_limit  = $args{'warning'};

        # warning and critical must be raw.
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept raw number of transactions.",
            -exitval => 127
        ) unless $args{'warning'}  =~ m/^([0-9.]+)$/
            and $args{'critical'} =~ m/^([0-9.]+)$/;
    }

    $detailed = $args{'detailed'};

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "oldest_xmin".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'oldest_xmin', $PG_VERSION_84 or exit 1;

    @rs = @{ query_ver( $hosts[0], %queries ) };

    REC_LOOP: foreach my $r (@rs) {

        next REC_LOOP if @dbexclude and     grep { $r->[0] =~ /$_/ } @dbexclude;
        next REC_LOOP if @dbinclude and not grep { $r->[0] =~ /$_/ } @dbinclude;

        map { $_ = 'NaN' if $_ eq ''} @{$r}[2..3];

        if ($detailed) {
            push @perfdata => (
                ["$r->[0]_$r->[1]_age", $r->[2]],
                ["$r->[0]_$r->[1]_xmin", $r->[3]]
            );
        }
        else {
            if ( exists $oldest_xmin{$r->[0]} ) {
                $oldest_xmin{$r->[0]} = [ $r->[1], $r->[2] ]
                    if $oldest_xmin{$r->[0]}[1] eq 'NaN'
                    or $r->[2] > $oldest_xmin{$r->[0]}[1];
            }
            else {
                $oldest_xmin{$r->[0]} = [ $r->[1], $r->[2] ];
            }
        }

        if (defined $c_limit) {
            if ($r->[2] ne 'NaN' and $r->[2] > $c_limit) {
                push @msg_crit => "$r->[0]_$r->[1]_age";
                next REC_LOOP;
            }

            push @msg_warn => "$r->[0]_$r->[1]_age"
                if ($r->[2] ne 'NaN' and $r->[2] > $w_limit);
        }
    }

    if (not $detailed) {
        foreach my $k (keys %oldest_xmin) {
            push @perfdata => (
                ["${k}_age", $oldest_xmin{$k}[1]]
            );

            push @msg, "Oldest xmin in $k from ". $oldest_xmin{$k}[0]
                if $oldest_xmin{$k}[1] ne 'NaN';
        }
    }

    return status_critical( $me, [
        'Critical: '. join(',', @msg_crit)
        . (scalar @msg_warn? ' Warning: '. join(',', @msg_warn):''),
        @msg
    ], \@perfdata ) if scalar @msg_crit;

    return status_warning( $me,
        [ 'Warning: '. join(',', @msg_warn), @msg ], \@perfdata
    ) if scalar @msg_warn;

    return status_ok( $me, \@msg, \@perfdata );
}


=item B<pg_dump_backup>

Check the age and size of backups.

This service uses the status file (see C<--status-file> parameter).

The C<--path> argument contains the location to the backup folder. The
supported format is a glob pattern matching every folder or file that you need
to check.

The C<--pattern> is required, and must contain a regular expression matching
the backup file name, extracting the database name from the first matching
group.

Optionally, a C<--global-pattern> option can be supplied to check for an
additional global file.

Examples:

To monitor backups like:

    /var/lib/backups/mydb-20150803.dump
    /var/lib/backups/otherdb-20150803.dump
    /var/lib/backups/mydb-20150804.dump
    /var/lib/backups/otherdb-20150804.dump

you must set:

    --path    '/var/lib/backups/*'
    --pattern '(\w+)-\d+.dump'

If the path contains the date, like this:

   /var/lib/backups/2015-08-03-daily/mydb.dump
   /var/lib/backups/2015-08-03-daily/otherdb.dump

then you can set:

    --path    '/var/lib/backups/*/*.dump'
    --pattern '/\d+-\d+-\d+-daily/(.*).dump'

For compatibility with pg_back (https://github.com/orgrim/pg_back),
you should use:

   --path '/path/*{dump,sql}'
   --pattern '(\w+)_[0-9-_]+.dump'
   --global-pattern 'pg_global_[0-9-_]+.sql'

The C<--critical> and C<--warning> thresholds are optional. They accept a list
of 'metric=value' separated by a comma. Available metrics are C<oldest> and
C<newest>, respectively the age of the oldest and newest backups, and C<size>,
which must be the maximum variation of size since the last check, expressed as
a size or a percentage. C<mindeltasize>, expressed in B, is the minimum
variation of size needed to raise an alert.

This service supports the C<--dbinclude> and C<--dbexclude> arguments, to
respectively test for the presence of include or exclude files.

The argument C<--exclude> enables you to exclude files younger than an
interval. This is useful to ignore files from a backup in progress. Eg., if
your backup process takes 2h, set this to '125m'.

Perfdata returns the age of the oldest and newest backups, as well as the size
of the newest backups.

Required privileges: unprivileged role; the system user needs read access
on the directory containing the dumps (but not on the dumps themselves).

=cut

sub check_pg_dump_backup {

    my @rs;
    my @stat;
    my @dirfiles;
    my @perfdata;
    my @msg_crit;
    my @msg_warn;
    my %db_sizes;
    my %firsts;
    my %lasts;
    my %crit;
    my %warn;
    my $mtime;
    my $size;
    my $me             = 'POSTGRES_PGDUMP_BACKUP';
    my $sql            = 'SELECT datname FROM pg_database';
    my $now            = time();
    my @hosts          = @{ parse_hosts %args };
    my @dbinclude      = @{ $args{'dbinclude'} };
    my @dbexclude      = @{ $args{'dbexclude'} };
    my $min_age        = 0;
    my $backup_path    = $args{'path'};
    my $pattern        = $args{'pattern'};
    my $thresholds_re  = qr/(oldest|newest|size|mindeltasize)\s*=\s*(\d+[^,]*)/i;
    my $global_pattern = $args{'global-pattern'};

    pod2usage(
        -message => "FATAL: you must specify a pattern for filenames",
        -exitval => 127
    ) unless $pattern;

    pod2usage(
        -message => "FATAL: you must specify a backup path",
        -exitval => 127
    ) unless $backup_path;

    pod2usage(
        -message => 'FATAL: you must give one (and only one) host with service "pg_dump_backup".',
        -exitval => 127
    ) if @hosts != 1;

    pod2usage(
        -message => "FATAL: to use a size threshold, a status-file is required",
        -exitval => 127
    ) unless $args{'status-file'};

    # warning and critical must be raw
    pod2usage(
        -message => "FATAL: critical and warning thresholds only accept a list of 'label=value' separated by comma.\n"
            . "See documentation for more information.",
        -exitval => 127
    ) unless ( not defined $args{'warning'} )
        or (
            $args{'warning'}      =~ m/^$thresholds_re(\s*,\s*$thresholds_re)*$/
            and $args{'critical'} =~ m/^$thresholds_re(\s*,\s*$thresholds_re)*$/
        );

    $min_age = get_time( $args{'exclude'}[0] ) if defined $args{'exclude'}[0];

    while ( $args{'warning'} and $args{'warning'} =~ /$thresholds_re/g ) {
        my ( $threshold, $value ) = ($1, $2);

        if( $threshold eq "oldest" or $threshold eq "newest" ) {
            pod2usage(
                -message => "FATAL: threshold for the oldest or newest backup age must be an interval: $threshold=$value",
                -exitval => 127
            ) unless is_time($value);

            $value = get_time($value);
        }

        $warn{$threshold} = $value if $1 and defined $2;
    }

    while ( $args{'critical'} and $args{'critical'} =~ /$thresholds_re/g ) {
        my ($threshold, $value) = ($1, $2);

        if( $threshold eq "oldest" or $threshold eq "newest" ) {
            pod2usage(
                -message => "FATAL: threshold for the oldest or newest backup age must be an interval: $threshold=$value",
                -exitval => 127
            ) unless is_time($value);

            $value = get_time($value);
        }

        $crit{$threshold} = $value if $1 and defined $2;
    }

    # Stat files in the backup directory
    @dirfiles = glob $backup_path;

    foreach my $file ( @dirfiles ) {
        my $filename = $file;
        my $dbname;
        my $mtime;
        my $size;

        ( undef, undef, undef, undef, undef, undef, undef, $size,
            undef, $mtime ) = stat $file;

        next if $now - $mtime < $min_age;

        if ( $global_pattern and $filename =~ $global_pattern ) {
            $firsts{'globals_objects'} = [ $mtime, $size ] if not exists $firsts{'globals_objects'}
                or $firsts{'globals_objects'}[0] > $mtime;
            $lasts{'globals_objects'}  = [ $mtime, $size ] if not exists $lasts{'globals_objects'}
                or $lasts{'globals_objects'}[0] < $mtime;
        }

        next unless $filename =~ $pattern and defined $1;

        $dbname = $1;

        $firsts{$dbname} = [ $mtime, $size ] if not exists $firsts{$dbname}
            or $firsts{$dbname}[0] > $mtime;

        $lasts{$dbname} = [ $mtime, $size ] if not defined $lasts{$dbname}
            or $lasts{$dbname}[0] < $mtime;
    }

    if ( scalar @dbinclude ) {
        push @rs => [ $_ ] foreach @dbinclude;
    }
    else {
        # Check against databases queried from pg_database
        @rs = @{ query( $hosts[0], $sql ) };
    }

    # If global_pattern is defined, add them to the list to check
    push @rs => [ "globals_objects" ] if $global_pattern;

    %db_sizes = %{ load( $hosts[0], 'pg_dump_backup', $args{'status-file'} ) || {} }
        if exists $warn{'size'} or exists $crit{'size'};

    ALLDB: foreach my $row ( @rs ) {
        my $db = $row->[0];
        my @perf_newest;
        my @perf_oldest;
        my @perf_delta;
        my @perf_size;
        my $last_age;
        my $first_age;

        next if grep { $db =~ /$_/ } @dbexclude;
        next if @dbinclude and not grep { $db =~ /$_/ } @dbinclude
            and $db ne 'globals_objects';

        if ( not exists $lasts{$db}[0] ) {
            push @msg_crit => sprintf("'%s_oldest'=NaNs", $db);
            push @msg_crit => sprintf("'%s_newest'=NaNs", $db);
            push @msg_crit => sprintf("'%s_size'=NaNB", $db);
            @perf_oldest = ( "${db}_oldest", 'NaN', 's' );
            @perf_newest = ( "${db}_newest", 'NaN', 's' );
            @perf_size   = ( "${db}_size",   'NaN', 'B' );
            @perf_delta  = ( "${db}_delta",  'NaN', 'B' );
            push @perfdata => ( \@perf_newest, \@perf_oldest, \@perf_size, \@perf_delta );
            next;
        }

        $last_age  = $now - $lasts{$db}[0];
        $first_age = $now - $firsts{$db}[0];

        @perf_oldest = ( "${db}_oldest", $first_age,     's' );
        @perf_newest = ( "${db}_newest", $last_age,      's' );
        @perf_size   = ( "${db}_size",   $lasts{$db}[1], 'B' );
        @perf_delta  = exists $db_sizes{$db} ?
            ( "${db}_delta",  $lasts{$db}[1] - $db_sizes{$db}[1], 'B' )
            : ( "${db}_delta",  0, 'B' );

        if ( exists $warn{'newest'} or exists $crit{'newest'} ) {
            my $c_limit = $crit{'newest'};
            my $w_limit = $warn{'newest'};

            push @perf_newest => ( defined $w_limit ? $w_limit : undef );
            push @perf_newest => ( defined $c_limit ? $c_limit : undef );

            if ( defined $c_limit and $last_age > $c_limit ) {
                push @msg_crit => sprintf("'%s_newest'=%s", $db,
                    to_interval( $last_age )
                );
            }
            elsif ( defined $w_limit and $last_age > $w_limit ) {
                push @msg_warn => sprintf("'%s_newest'=%s", $db,
                    to_interval( $last_age )
                );
            }
        }

        if ( exists $warn{'oldest'} or exists $crit{'oldest'} ) {
            my $c_limit = $crit{'oldest'};
            my $w_limit = $warn{'oldest'};

            push @perf_oldest => ( defined $w_limit ? $w_limit : undef );
            push @perf_oldest => ( defined $c_limit ? $c_limit : undef );

            if ( defined $c_limit and $first_age > $c_limit ) {
                push @msg_crit => sprintf("'%s_oldest'=%s", $db,
                    to_interval( $first_age )
                );
            }
            elsif ( defined $w_limit and $first_age > $w_limit ) {
                push @msg_warn => sprintf( "'%s_oldest'=%s", $db,
                    to_interval( $first_age )
                );
            }
        }

        if ( exists $warn{'size'} or exists $crit{'size'} ) {
            next ALLDB unless exists $db_sizes{$db};

            my $w_delta = get_size( $warn{'size'}, $db_sizes{$db}[1] )
                if exists $warn{'size'};
            my $c_delta = get_size( $crit{'size'}, $db_sizes{$db}[1] )
                if exists $crit{'size'};
            my $delta = abs( $lasts{$db}[1] - $db_sizes{$db}[1] );

            push @perf_delta => ( defined $w_delta ? $w_delta: undef );
            push @perf_delta => ( defined $c_delta ? $c_delta: undef );

            my $w_mindeltasize = 0;
            my $c_mindeltasize = 0;
            $w_mindeltasize = $warn{'mindeltasize'} if exists $warn{'mindeltasize'};
            $c_mindeltasize = $crit{'mindeltasize'} if exists $crit{'mindeltasize'};

            if ( defined $c_delta and $delta > $c_delta and $delta >= $c_mindeltasize ) {
                push @msg_crit => sprintf("'%s_delta'=%dB", $db, $lasts{$db}[1]);
            }
            elsif ( defined $w_delta and $delta > $w_delta and $delta >= $w_mindeltasize ) {
                push @msg_warn => sprintf("'%s_delta'=%dB", $db, $lasts{$db}[1]);
            }
        }

        push @perfdata => ( \@perf_newest, \@perf_oldest, \@perf_size, \@perf_delta );
    }

    save $hosts[0], 'pg_dump_backup', \%lasts, $args{'status-file'}
        if $args{'status-file'};

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit;

    return status_warning( $me, \@msg_warn, \@perfdata )
        if scalar @msg_warn;

    return status_ok( $me, [], \@perfdata );
}

=item B<pga_version>

Check if this script is running the given version of check_pgactivity.
You must provide the expected version using either C<--warning> OR
C<--critical>.

No perfdata is returned.

Required privileges: none.

=cut

sub check_pga_version {
    my @rs;
    my @hosts;
    my %args = %{ $_[0] };
    my $me   = 'PGACTIVITY_VERSION';
    my $msg  = "check_pgactivity $VERSION %s, Perl %vd";

    pod2usage(
        -message => 'FATAL: you must provide a warning or a critical threshold for service pga_version!',
        -exitval => 127
    ) if (defined $args{'critical'} and defined $args{'warning'})
        or (not defined $args{'critical'} and not defined $args{'warning'});

    pod2usage(
        -message => "FATAL: given version does not look like a check_pgactivity version!",
        -exitval => 127
    ) if ( defined $args{'critical'} and $args{'critical'} !~ m/^\d\.\d+(?:_?(?:dev|beta|rc)\d*)?$/ )
        or (defined $args{'warning'} and $args{'warning'} !~ m/^\d\.\d+(?:_?(?:dev|beta|rc)\d*)?$/ );

    return status_critical( $me,
        [ sprintf($msg, "(should be $args{'critical'}!)", $^V) ]
    ) if defined $args{'critical'} and $VERSION ne $args{'critical'};

    return status_warning( $me,
        [ sprintf($msg, "(should be $args{'warning'}!)", $^V) ]
    ) if defined $args{'warning'} and $VERSION ne $args{'warning'};

    return status_ok( $me, [ sprintf($msg, "", $^V) ] );
}

=item B<pgdata_permission> (8.2+)

Check that the instance data directory rights are 700, and belongs
to the system user currently running postgresql.

The check on rights works on all Unix systems.

Checking the user only works on Linux systems (it uses /proc to avoid
dependencies). Before 9.3, you need to provide the expected owner using the
C<--uid> argument, or the owner will not be checked.

Required privileges:
 <11:superuser
 v11: user with pg_monitor or pg_read_all_setting
The system user must also be able to read the folder containing
PGDATA: B<the service has to be executed locally on the monitored server.>

=cut
sub check_pgdata_permission {
    my $me        = 'POSTGRES_CHECK_PGDATA_PERMISSION';
    my %args      = %{ $_[0] };
    my $criticity = 0; # 0=ok, 1=warn, 2=critical
    my $pg_uid    = $args{'uid'};
    my @rs;
    my @msg;
    my @longmsg;
    my @stat;
    my @hosts;
    my $mode;
    my $perm;
    my $query;
    my $dir_uid;
    my $stats_age;
    my $data_directory;

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "pgdata_permission".',
        -exitval => 127
    ) if scalar @hosts != 1;

    is_compat $hosts[0], 'pgdata_permission', $PG_VERSION_82 or exit 1;

    # Get the data directory
    $query = q{
        SELECT setting FROM pg_settings WHERE name='data_directory'
    };

    @rs = @{ query( $hosts[0], $query ) };

    $data_directory = $rs[0][0];

    return status_unknown( $me,
        [ "Postgresql returned this PGDATA: $data_directory, but I cannot access it: $!" ]
    ) unless @stat = stat( $data_directory );

    $mode    = $stat[2];
    $dir_uid = $stat[4];
    $perm = sprintf( "%04o", $mode & 07777 );

    # starting with v11, PGDATA can be 0750.
    if ($perm eq '0700'
        or (check_compat($hosts[0], $PG_VERSION_110) and $perm eq '0750')
    ) {
        push @msg, ( "Permission is ok on $data_directory" );
    }
    else {
        $criticity = 2;
        push @msg, ( "Permission is $perm on $data_directory" );
    }

    # Now look at who this directory belongs to, and if it matches the user running
    # the instance.
    if ( defined $args{'uid'} ) {
         $pg_uid = getpwnam( $args{'uid'} );
    }
    # Simplest way is to get the current backend pid and see who it belongs to
    elsif ( $^O =~ /linux/i and $hosts[0]{'version_num'} >= $PG_VERSION_93 ) {
        my $pg_line_uid;
        my @rs_tmp;

        # first query is a ugly hack to bypass query() control about number of
        # columns expected.
        $query = qq{ select ' ';
            select pg_backend_pid() as pid
            \\gset
            \\setenv PID :pid
            \\! cat /proc/\$PID/status
        };
        @rs = @{ query ( $hosts[0], $query ) };

        # takes part of the ugly hack to bypass query() control.
        shift @rs;

        # As the usual separators are not there, we only get a big record
        # separated with \n
        # Find the record beginning with Uid and get the third column
        # (containing EUID)
        @rs_tmp      = split( /\n/, $rs[0][0] );
        $pg_line_uid = ( grep (/^Uid/, @rs_tmp) )[0];
        $pg_uid      = ( split("\t", $pg_line_uid) )[2];
    }

    if ( not defined $pg_uid ) {
        push @longmsg, ( "Cannot determine UID of user running postgres. Try to use '--uid'?" );
    }
    elsif ( $pg_uid ne $dir_uid ) {
        $criticity = 2;
        push @msg, ( "User running Postgres ($pg_uid) doesn't own $data_directory ($dir_uid)" );
    }
    else {
        push @msg, ( "Owner of $data_directory is ($pg_uid)" );
    }

    return status_warning( $me, \@msg, undef, \@longmsg )  if $criticity == 1;
    return status_critical( $me, \@msg, undef, \@longmsg ) if $criticity;
    return status_ok( $me, \@msg, undef, \@longmsg );
}



=item B<replication_slots> (9.4+)

Check the number of WAL files retained and spilled files for each replication
slots.

Perfdata returns the number of WAL kept for each slot and the number of spilled
files in pg_replslot for each logical replication slot. Since v13, if
C<max_slot_wal_keep_size> is greater or equal to 0, perfdata reports the size
of WAL to produce before each slot becomes C<unreserved> or C<lost>. Note that
this size can become negative if the WAL status for the limited time where the
slot becomes C<unreserved>. It is set to zero as soon as the last checkpoint
finished and the status becomes C<lost>.

This service needs superuser privileges to obtain the number of spill files or
returns 0 in last resort.

Critical and Warning thresholds are optional. They accept either a raw number
(for backward compatibility, only wal threshold will be used) or a list of
'wal=value' and/or 'spilled=value' and/or 'remaining=size'. Respectively number
of kept wal files, number of spilled files in pg_replslot for each logical slot
and remaining bytes before a slot becomes C<unreserved> or C<lost>.

Moreover, with v13 and after, the service raises a warning alert if a slot
becomes C<unreserved>. It raises a critical alert if the slot becomes C<lost>.

Required privileges:
 v9.4: unprivileged role, or superuser to monitor spilled files for logical replication
 v11+: unprivileged user with GRANT EXECUTE on function pg_ls_dir(text)

Here is somes examples:

    -w 'wal=50,spilled=20' -c 'wal=100,spilled=40'
    -w 'spilled=20,remaining=160MB' -c 'spilled=40,remaining=48MB'

=cut

sub check_replication_slots {
    my $me   = 'POSTGRES_REPLICATION_SLOTS';
    my %args = %{ $_[0] };
    my @msg_crit;
    my @msg_warn;
    my @longmsg;
    my @perfdata;
    my @hosts;
    my @rs;
    my @perf_wal_limits;
    my @perf_spilled_limits;
    my @perf_remaining_limits;
    my %warn;
    my %crit;
    my %queries = (
        # 1st field: slot name
        # 2nd field: slot type
        # 3rd field: number of WAL kept because of the slot
        # 4th field: number of spill files for logical replication
        # 5th field: wal status for this slot (v13+)
        # 6th field: remaining safe bytes before max_slot_wal_keep_size (v13+)
       $PG_VERSION_94 => q{
        WITH wal_size AS (
           SELECT current_setting('wal_block_size')::int * setting::int AS val
           FROM pg_settings
           WHERE name = 'wal_segment_size' -- usually 2048 (blocks)
        )
        SELECT slot_name, slot_type, replslot_wal_keep,
              count(slot_file) as replslot_files, -- 0 if not superuser
              NULL, NULL
        FROM
          (SELECT slot.slot_name,
                  CASE WHEN slot_file <> 'state' THEN 1 END AS slot_file,
                  slot_type,
          COALESCE(
                 floor(
                      CASE WHEN pg_is_in_recovery()
                      THEN (
                        pg_xlog_location_diff(pg_last_xlog_receive_location(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_xlog_location_diff(restart_lsn, '0/0') % s.val)
                      ) / s.val
                      ELSE (
                        pg_xlog_location_diff(pg_current_xlog_location(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_xlogfile_name_offset(restart_lsn)).file_offset
                      ) / s.val
                      END
                 ),0
             ) as replslot_wal_keep
            FROM pg_replication_slots slot
            -- trick when user is not superuser
            LEFT JOIN (
              SELECT slot2.slot_name,
                    pg_ls_dir('pg_replslot/'||slot2.slot_name) as slot_file
                FROM pg_replication_slots slot2
                WHERE current_setting('is_superuser')::bool
            ) files(slot_name,slot_file) ON slot.slot_name=files.slot_name
        CROSS JOIN wal_size s
         ) as d
        GROUP BY slot_name,slot_type,replslot_wal_keep},

       $PG_VERSION_100 => q{
        WITH wal_size AS (
           SELECT current_setting('wal_block_size')::int * setting::int AS val
           FROM pg_settings
           WHERE name = 'wal_segment_size' -- usually 2048 (blocks)
        )
        SELECT slot_name, slot_type, replslot_wal_keep,
              count(slot_file) AS spilled_files, -- 0 if not superuser
              NULL, NULL
        FROM
          (SELECT slot.slot_name,
                  CASE WHEN slot_file <> 'state' THEN 1 END AS slot_file,
                  slot_type,
          COALESCE(
                 floor(
                      CASE WHEN pg_is_in_recovery()
                      THEN (
                        pg_wal_lsn_diff(pg_last_wal_receive_lsn(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_wal_lsn_diff(restart_lsn, '0/0') % s.val)
                      ) / s.val
                      ELSE (
                        pg_wal_lsn_diff(pg_current_wal_lsn(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_walfile_name_offset(restart_lsn)).file_offset
                      ) / s.val
                      END
                 ),0
             ) as replslot_wal_keep
            FROM pg_replication_slots slot
            -- trick when user is not superuser
            LEFT JOIN (
              SELECT slot2.slot_name,
                    pg_ls_dir('pg_replslot/'||slot2.slot_name) as slot_file
                FROM pg_replication_slots slot2
                WHERE current_setting('is_superuser')::bool
            ) files(slot_name,slot_file) ON slot.slot_name=files.slot_name
        CROSS JOIN wal_size s
         ) as d
        GROUP BY slot_name,slot_type,replslot_wal_keep},

        $PG_VERSION_110 => q{
         WITH wal_size AS (
            SELECT setting::int AS wal_segment_size -- unit: B (often 16777216)
            FROM pg_settings
            WHERE name = 'wal_segment_size'
         )
         SELECT slot_name, slot_type, replslot_wal_keep,
               count(slot_file) AS spilled_files, -- 0 if not superuser
               NULL, NULL
         FROM
           (SELECT slot.slot_name,
                   CASE WHEN slot_file <> 'state' THEN 1 END AS slot_file,
                   slot_type,
           COALESCE(
                  floor(
                      CASE WHEN pg_is_in_recovery()
                      THEN (
                        pg_wal_lsn_diff(pg_last_wal_receive_lsn(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_wal_lsn_diff(restart_lsn, '0/0') % s.wal_segment_size)
                      ) / s.wal_segment_size
                      ELSE (
                        pg_wal_lsn_diff(pg_current_wal_lsn(), slot.restart_lsn)
                        -- this is needed to account for whole WAL retention and
                        -- not only size retention
                        + (pg_walfile_name_offset(restart_lsn)).file_offset
                      ) / s.wal_segment_size
                      END
                  ),0
              ) as replslot_wal_keep
             FROM pg_replication_slots slot
             -- trick when user is not superuser
             LEFT JOIN (
               SELECT slot2.slot_name,
                     pg_ls_dir('pg_replslot/'||slot2.slot_name) as slot_file
                 FROM pg_replication_slots slot2
                 WHERE current_setting('is_superuser')::bool) files(slot_name,slot_file)
             ON  slot.slot_name=files.slot_name
         CROSS JOIN wal_size s
          ) as d
         GROUP BY slot_name,slot_type,replslot_wal_keep},

        $PG_VERSION_130 => q{
         WITH wal_sz AS (
            SELECT setting::int AS v -- unit: B (often 16777216)
            FROM pg_settings
            WHERE name = 'wal_segment_size'
         ),
         slot_sz AS (
            SELECT setting::int AS v -- unit: MB
            FROM pg_settings
            WHERE name = 'max_slot_wal_keep_size'
         )
         SELECT slot_name, slot_type, replslot_wal_keep,
               count(slot_file) AS spilled_files, -- 0 if not superuser
               wal_status, remaining_sz
         FROM (
            SELECT slot.slot_name,
                CASE WHEN slot_file <> 'state' THEN 1 END AS slot_file,
                slot_type,
                CASE WHEN slot.wal_status = 'lost'
                THEN 0
                ELSE
                    COALESCE(
                        floor(
                            CASE WHEN pg_is_in_recovery()
                            THEN (
                                pg_wal_lsn_diff(pg_last_wal_receive_lsn(), slot.restart_lsn)
                                -- this is needed to account for whole WAL retention and
                                -- not only size retention
                                + (pg_wal_lsn_diff(restart_lsn, '0/0') % wal_sz.v)
                            ) / wal_sz.v
                            ELSE (
                                pg_wal_lsn_diff(pg_current_wal_lsn(), slot.restart_lsn)
                                -- this is needed to account for whole WAL retention and
                                -- not only size retention
                                + (pg_walfile_name_offset(restart_lsn)).file_offset
                            ) / wal_sz.v
                            END
                        ), 0
                    )
                END AS replslot_wal_keep,
                slot.wal_status,
                CASE WHEN slot_sz.v >= 0
                    THEN slot.safe_wal_size
                    ELSE NULL
                END AS remaining_sz
            FROM pg_replication_slots slot
            -- trick when user is not superuser
            LEFT JOIN (
                SELECT slot2.slot_name,
                    pg_ls_dir('pg_replslot/'||slot2.slot_name) as slot_file
                FROM pg_replication_slots slot2
                WHERE current_setting('is_superuser')::bool) files(slot_name,slot_file
            ) ON slot.slot_name = files.slot_name
            CROSS JOIN wal_sz
            CROSS JOIN slot_sz
         ) as d
         GROUP BY slot_name, slot_type, replslot_wal_keep,
            wal_status, remaining_sz}
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "replication_slots".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'replication_slots', $PG_VERSION_94 or exit 1;

    # build warn/crit thresholds
    if ( defined $args{'warning'} ) {
        my $threshods_re = qr/(wal|spilled|remaining)\s*=\s*(?:([^,]+))/i;

        if ($args{'warning'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/
            and $args{'critical'} =~ m/^$threshods_re(\s*,\s*$threshods_re)*$/
        ) {
            while ( $args{'warning'} =~ /$threshods_re/g ) {
                my ($threshold, $value) = ($1, $2);

                if ($threshold eq 'remaining') {
                    $warn{$threshold} = get_size $value;
                }
                else {
                    pod2usage(
                        -message => "FATAL: $threshold accept a raw number\n",
                        -exitval => 127
                    ) unless $value =~ m/^([0-9]+)$/;

                    $warn{$threshold} = $value;
                }
            }

            while ( $args{'critical'} =~ /$threshods_re/g ) {
                my ($threshold, $value) = ($1, $2);

                if ($threshold eq 'remaining') {
                    $crit{$threshold} = get_size $value;
                }
                else {
                    pod2usage(
                        -message => "FATAL: $threshold accept a raw number\n",
                        -exitval => 127
                    ) unless $value =~ m/^([0-9]+)$/;

                    $crit{$threshold} = $value;
                }
            }
        }

        # For backward compatibility
        elsif ($args{'warning'}  =~ m/^([0-9]+)$/
               and $args{'critical'} =~ m/^([0-9]+)$/
        ) {
            $warn{'wal'} = $args{'warning'};
            $crit{'wal'} = $args{'critical'};
        }

        else {
            pod2usage(
                -message => "FATAL: critical and warning thresholds only accept:\n"
                    . "- raw numbers for backward compatibility to set wal threshold.\n"
                    . "- a list 'wal=value' and/or 'spilled=value' and/or remaining=size separated by comma.\n"
                    . "See documentation for more information.",
                -exitval => 127
            )
        }

        pod2usage(
            -message => "FATAL: \"remaining=size\" can only be set for PostgreSQL 13 and after.",
            -exitval => 127
        ) if $hosts[0]->{'version_num'} < $PG_VERSION_130
         and ( exists $warn{'remaining'} or exists $crit{'remaining'} );
    }

    @perf_wal_limits = ( $warn{'wal'}, $crit{'wal'} )
        if defined $warn{'wal'} or defined $crit{'wal'};
    @perf_spilled_limits = ( $warn{'spilled'}, $crit{'spilled'} )
        if defined $warn{'spilled'} or defined $crit{'spilled'};
    @perf_remaining_limits = ( $warn{'remaining'}, $crit{'remaining'} )
        if defined $warn{'remaining'} or defined $crit{'remaining'};

    @rs = @{ query_ver( $hosts[0], %queries ) };

SLOTS_LOOP: foreach my $row (@rs) {

        push @perfdata => [ "$row->[0]_wal", $row->[2],'File', @perf_wal_limits ]
            unless $row->[4] and $row->[4] eq 'lost';

        # add number of spilled files if logical replication slot
        push @perfdata => [ "$row->[0]_spilled", $row->[3], 'File', @perf_spilled_limits ]
            if $row->[1] eq 'logical';

        # add remaining safe bytes if available
        push @perfdata => [ "$row->[0]_remaining", $row->[5], '', @perf_remaining_limits ]
            if $row->[5];

        # alert on number of WAL kept
        if ( defined $crit{'wal'} and $row->[2] > $crit{'wal'} ) {
            push @msg_crit, "$row->[0] wal files : $row->[2]";
            push @longmsg => sprintf("Slot: %s wal files = %s above crit threshold %s",
                $row->[0], $row->[2], $crit{'wal'}
            );
        }
        elsif ( defined $warn{'wal'} and $row->[2] > $warn{'wal'} ) {
              push @msg_warn, "$row->[0] wal files : $row->[2]";
              push @longmsg => sprintf("Slot: %s wal files = %s above warn threshold %s",
                  $row->[0], $row->[2], $warn{'wal'}
              );
        }

        # alert on number of spilled files for logical replication
        if ( defined $crit{'spilled'} and $row->[3] > $crit{'spilled'} ) {
            push @msg_crit, "$row->[0] spilled files : $row->[3]";
            push @longmsg => sprintf("Slot: %s spilled files = %s above critical threshold %s",
                $row->[0], $row->[3], $crit{'spilled'}
            );
        }
        elsif ( defined $warn{'spilled'} and $row->[3] > $warn{'spilled'} ) {
              push @msg_warn, "$row->[0] spilled files : $row->[3]";
              push @longmsg => sprintf("Slot: %s spilled files = %s above warning threshold %s",
                  $row->[0], $row->[3], $warn{'spilled'}
              );
        }

        # alert on wal status
        push @msg_warn, "$row->[0] unreserved"
            if $row->[4] and $row->[4] eq 'unreserved';

        push @msg_crit, "$row->[0] lost"
            if $row->[4] and $row->[4] eq 'lost';

        # do not test remaining bytes if no value available from query
        next unless $row->[5];

        # alert on remaining safe bytes
        if ( defined $crit{'remaining'} and $row->[5] < $crit{'remaining'} ) {
            push @msg_crit, sprintf("slot %s not safe", $row->[0]);
            push @longmsg => sprintf("Remaining %s of WAL for slot %s",
                to_size($row->[5]), $row->[0]
            );
        }
        elsif ( defined $warn{'remaining'} and $row->[5] < $warn{'remaining'} ) {
            push @msg_warn, sprintf("slot %s not safe", $row->[0]);
            push @longmsg => sprintf("Remaining %s of WAL for slot %s",
                to_size($row->[5]), $row->[0]
            );
        }
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata, \@longmsg )
        if scalar @msg_crit > 0;

    return status_warning( $me, [ @msg_warn ], \@perfdata, \@longmsg )
        if scalar @msg_warn > 0;

    return status_ok( $me, [ "Replication slots OK" ], \@perfdata, \@longmsg );
}

=item B<settings> (9.0+)

Check if the current settings have changed since they were stored in the
service file.

The "known" settings are recorded during the very first call of the service.
To update the known settings after a configuration change, call this service
again with the argument C<--save>.

No perfdata.

Critical and Warning thresholds are ignored.

A Critical is raised if at least one parameter changed.

Required privileges: unprivileged role.

=cut

sub check_settings {
    my $me  = 'POSTGRES_SETTINGS';
    my @long_msg;
    my @hosts;
    my @rs;
    my %settings;
    my %new_settings;
    my $pending_count = 0;
    my %args  = %{ $_[0] };
    my %queries = (
        $PG_VERSION_90 => q{
        SELECT coalesce(r.rolname, '*'), coalesce(d.datname, '*'),
          unnest(s.setconfig) AS setting,
          false AS pending_restart
        FROM pg_db_role_setting s
        LEFT JOIN pg_database d ON d.oid=s.setdatabase
        LEFT JOIN pg_roles r ON r.oid=s.setrole
        UNION ALL
        SELECT '*', '*', name||'='||current_setting(name),false
        FROM pg_settings
       },
        $PG_VERSION_95 => q{
        SELECT coalesce(r.rolname, '*'), coalesce(d.datname, '*'),
          unnest(s.setconfig) AS setting,
          false AS pending_restart
        FROM pg_db_role_setting s
        LEFT JOIN pg_database d ON d.oid=s.setdatabase
        LEFT JOIN pg_roles r ON r.oid=s.setrole
        UNION ALL
        SELECT '*', '*', name||'='||current_setting(name), pending_restart
        FROM pg_settings
       }
    );

    @hosts = @{ parse_hosts %args };

    is_compat $hosts[0], 'settings', $PG_VERSION_90 or exit 1;

    @rs = @{ query_ver( $hosts[0], %queries ) };

    %settings = %{ load( $hosts[0], 'settings', $args{'status-file'} ) || {} };

    # Save settings on the very first call
    $args{'save'} = 1 unless %settings;

PARAM_LOOP: foreach my $row (@rs) {
        my ( $rolname, $datname, $setting, $pending ) = @$row;
        my ( $name, $val ) = split /=/ => $setting, 2;
        my $prefix = "$rolname\@$datname";
        my $msg = "$setting";

        if ( $pending eq "t" ) {
            $pending_count++;
            push @long_msg => "$name is pending restart !";
        }

        $msg = "$prefix: $setting" unless $prefix eq '*@*';

        $new_settings{$name}{$prefix} = $val;

        push @long_msg => $msg unless exists $settings{$name}{$prefix};

        push @long_msg => $msg if exists $settings{$name}{$prefix}
                                  and $val ne $settings{$name}{$prefix};

        delete $settings{$name}{$prefix};

    }

    # Gather remaining settings that has not been processed
    foreach my $s ( keys %settings ) {
        foreach my $p ( keys %{ $settings{$s} } ) {
            my $prefix = ( $p eq "*@*"? ":" : " $p:" );

            push @long_msg => "missing$prefix $s=$settings{$s}{$p}";
        }
    }

    if ( $args{'save'} ) {
        save $hosts[0], 'settings', \%new_settings, $args{'status-file'};
        return status_ok( $me, [ "Setting saved" ] )
    }

    return status_warning( $me, [ 'Setting changed and pending restart!' ], undef, \@long_msg )
        if $pending_count > 0;

    return status_warning( $me, [ 'Setting changed!' ], undef, \@long_msg )
        if scalar @long_msg;

    return status_ok( $me, [ "Setting OK" ] );
}

=item B<sequences_exhausted> (7.4+)

Check all sequences assigned to a column (the smallserial, serial and bigserial
types), and raise an alarm if the column or sequences gets too close to the
maximum value.

Perfdata returns the sequences that trigger the alert.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

Critical and Warning thresholds accept a percentage of the sequence filled.

Required privileges: unprivileged role able to log in all databases

=cut
sub check_sequences_exhausted {
    my $me       = 'POSTGRES_CHECK_SEQ_EXHAUSTED';
    my @rs;
    my @rs2;
    my @perfdata;
    my @msg;
    my @longmsg;
    my @hosts;
    my %args     = %{ $_[0] };
    my $stats_age;
    my @all_db;
    my @dbinclude = @{ $args{'dbinclude'} };
    my @dbexclude = @{ $args{'dbexclude'} };
    my $criticity=0; # 0=ok, 1=warn, 2=critical



    if ( not defined $args{'warning'} or not defined $args{'critical'} ) {
        # warning and critical are mandatory.
        pod2usage(
            -message => "FATAL: you must specify critical and warning thresholds.",
            -exitval => 127
        );
    }
    unless ( $args{'warning'}  =~ m/^([0-9.]+)%$/
          and  $args{'critical'} =~ m/^([0-9.]+)%$/)
    {
        # Warning and critical must be %.
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept %.",
            -exitval => 127
        );
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "sequences_exhausted".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'sequences_exhausted', $PG_VERSION_82 or exit 1;

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db (sort @all_db) {
        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;
        dprint ("Searching for exhausted dequences in $db \n");

        my %sequences;

        # We have two code paths: one for < 10.0 and one for >=10.S
        if (check_compat $hosts[0], $PG_VERSION_82, $PG_VERSION_96)
        {
            # Search path is emptied so that ::regclass gives us full paths
            my $query  = q{
SET search_path TO 'pg_catalog';
SELECT t.typname,
       seq.oid::regclass as name,
       pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) || '.' || pg_catalog.quote_ident(a.attname)
FROM pg_class seq
INNER JOIN pg_catalog.pg_depend d ON d.objid=seq.oid
INNER JOIN pg_catalog.pg_class c  ON c.oid=d.refobjid
INNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
INNER JOIN pg_catalog.pg_attribute a ON (
         a.attrelid=c.oid AND
         a.attnum=d.refobjsubid)
INNER JOIN pg_catalog.pg_type t ON a.atttypid=t.oid
WHERE seq.relkind='S'
 AND d.classid='pg_catalog.pg_class'::pg_catalog.regclass
 AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass
 AND d.deptype='a'};
            # We got an array: for each record, type (int2, int4, int8), sequence name, and the column name
            @rs = @{ query ( $hosts[0], $query, $db ) };
            dprint ("DB  $db : found ".(scalar @rs )." sequences \n" );

            next ALLDB_LOOP if ( scalar @rs ) <= 0 ;

            # Now a second query: get last_value for all sequences. There is no way to not generate a query here
            my @query_elements;
            foreach my $record(@rs) {
                # dprint ("Looking at sequence  $record->[1] \n") ;
                my $seqname=$record->[1];
                my $protected_seqname=$seqname;
                $protected_seqname=~s/'/''/g; # Protect quotes
                push @query_elements,("SELECT '$protected_seqname',last_value,min_value,max_value,increment_by FROM $seqname");
            }
            my $query_elements=join("\nUNION ALL\n",@query_elements);

            # We got a second array: for each record, sequence, last value and max value (in this sequence)
            @rs2 = @{ query ( $hosts[0], $query_elements, $db ) };

            # To make things easier, we store all of this in a hash table with all sequences, merging these two queries
            foreach my $record(@rs) {
                $sequences{$record->[1]}->{TYPE}=$record->[0];
                $sequences{$record->[1]}->{COLNAME}=$record->[2];
            }
            foreach my $record(@rs2) {
                $sequences{$record->[0]}->{LASTVALSEQ}=$record->[1];
                $sequences{$record->[0]}->{MINVALSEQ}=$record->[2];
                $sequences{$record->[0]}->{MAXVALSEQ}=$record->[3];
                $sequences{$record->[0]}->{INCREMENTBY}=$record->[4];
            }
        }
        else
        {
            # Version 10.0 and bigger: we now have pg_sequence and functions to
            # get the info directly
            my $query = q{
SET search_path TO 'pg_catalog';
SELECT
    t.typname,
    seq.seqrelid::regclass AS sequencename,
    pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) || '.' || pg_catalog.quote_ident(a.attname),
    CASE
        WHEN has_sequence_privilege(seq.seqrelid, 'SELECT,USAGE'::text) THEN pg_sequence_last_value(seq.seqrelid::regclass)
        ELSE NULL::bigint
    END AS last_value,
    seq.seqmin AS min_value,
    seq.seqmax AS max_value,
    seq.seqincrement AS increment_by
   FROM pg_sequence seq
   INNER JOIN pg_catalog.pg_depend d ON d.objid=seq.seqrelid
   INNER JOIN pg_catalog.pg_class c  ON c.oid=d.refobjid
   INNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   INNER JOIN pg_catalog.pg_attribute a ON ( a.attrelid=c.oid
                                             AND a.attnum=d.refobjsubid)
   INNER JOIN pg_catalog.pg_type t ON a.atttypid=t.oid
   WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
     AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass
     AND d.deptype='a'};
            # We get an array: for each record, type (int2, int4, int8),
            # sequence name, column name, last, min, max, increment
            @rs = @{ query ( $hosts[0], $query, $db ) };
            foreach my $record (@rs)
            {
                $sequences{$record->[1]}->{TYPE}=$record->[0];
                $sequences{$record->[1]}->{COLNAME}=$record->[2];
                $sequences{$record->[1]}->{LASTVALSEQ}=$record->[3];
                $sequences{$record->[1]}->{MINVALSEQ}=$record->[4];
                $sequences{$record->[1]}->{MAXVALSEQ}=$record->[5];
                $sequences{$record->[1]}->{INCREMENTBY}=$record->[6];
            }
        }
        # Calculate real max value:
        # We take into accound negative incrementby. If incrementby <0, use minvalseq
        foreach my $seq (keys %sequences)
        {
            my $max_value;
            # We don't make the difference between positive and negative limits
            # It's only 1 of difference, so there is no point…
            if ($sequences{$seq}->{TYPE} eq 'int2') {
                $max_value=32767;
            }
            elsif ($sequences{$seq}->{TYPE} eq 'int4') {
                $max_value=2147483647;
            }
            elsif ($sequences{$seq}->{TYPE} eq 'int8') {
                $max_value=9223372036854775807;
            }
            else
            {
                # We're not going to try to guess. This is not a serial, trust
                # the dba/developer
                delete $sequences{$seq};
                next;
            }

        if ($sequences{$seq}->{LASTVALSEQ} eq '') {
                # Skip sequences having lastvalue not initialized
                delete $sequences{$seq};
                next;
            }

            my $max_val_seq;
            if ($sequences{$seq}->{INCREMENTBY}>=0) {
                $max_val_seq=$sequences{$seq}->{MAXVALSEQ};
                $sequences{$seq}->{ABSVALSEQ}=$sequences{$seq}->{LASTVALSEQ};
            } else {
                $max_val_seq=-$sequences{$seq}->{MINVALSEQ};# Reverse the sign
                $sequences{$seq}->{ABSVALSEQ}=-$sequences{$seq}->{LASTVALSEQ};
            }
            # The real maximum value is the smallest of both
            my $real_max_value=$max_val_seq<=$max_value?$max_val_seq:$max_value;
            $sequences{$seq}->{REALMAXVALUE}=$real_max_value;
        }
        # We have inverted values for the reverse-order sequences. We don't
        # have to think about it anymore.

        foreach my $seq(keys %sequences) {
            # First, get all info
            my $real_max_value=$sequences{$seq}->{REALMAXVALUE};
            my $usable_amount=$real_max_value - $sequences{$seq}->{MINVALSEQ} + 1;
            my $lim_warning=$usable_amount-get_size($args{'warning'},$usable_amount);
            my $lim_critical=$usable_amount-get_size($args{'critical'},$usable_amount);
            my $how_much_left=$real_max_value-$sequences{$seq}->{ABSVALSEQ};
            my $seq_desc="$db." . $sequences{$seq}->{COLNAME};
            my $long_seq_desc="$db.$seq(owned by " . $sequences{$seq}->{COLNAME} . ')';
            my $seq_criticity=0;
            if ($how_much_left<=$lim_critical) {
                $seq_criticity=2;
            }
            elsif ($how_much_left<=$lim_warning) {
                $seq_criticity=1;
            }
            if ($seq_criticity>=1) {
                push @perfdata => [ $seq_desc, $how_much_left, undef,
                                    $lim_warning, $lim_critical, 0, $sequences{$seq}->{REALMAXVALUE} ];
                push @longmsg, "$long_seq_desc $how_much_left values left";
                $criticity=$criticity>$seq_criticity?$criticity:$seq_criticity; # Take biggest of the criticities
            }
        }
    }
    return status_warning( $me, \@msg, \@perfdata, \@longmsg ) if $criticity == 1;
    return status_critical( $me, \@msg, \@perfdata, \@longmsg ) if $criticity == 2;
    return status_ok( $me, \@msg, \@perfdata );
}

=item B<stat_snapshot_age> (9.5+)

Check the age of the statistics snapshot (statistics collector's statistics).
This probe helps to detect a frozen stats collector process.

Perfdata returns the statistics snapshot age.

Critical and Warning thresholds accept a raw number of seconds.

Required privileges: unprivileged role.

=cut
sub check_stat_snapshot_age {
    my $me       = 'POSTGRES_STAT_SNAPSHOT_AGE';
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my %args     = %{ $_[0] };
    my $stats_age;

    my $query  = q{ SELECT extract(epoch from (now() - pg_stat_get_snapshot_timestamp())) AS age };

    if ( defined $args{'warning'} ) {
        # warning and critical are mandatory.
        pod2usage(
            -message => "FATAL: you must specify critical and warning thresholds.",
            -exitval => 127
        ) unless defined $args{'warning'} and defined $args{'critical'} ;

        # warning and critical must be raw or %.
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept raw numbers.",
            -exitval => 127
        ) unless $args{'warning'}  =~ m/^([0-9.]+)/
            and  $args{'critical'} =~ m/^([0-9.]+)/;
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "stat_snapshot_age".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'stat_snapshot_age', $PG_VERSION_95 or exit 1;

    @rs = @{ query ( $hosts[0], $query ) };

    # Get statistics age in seconds
    $stats_age = $rs[0][0];

    push @perfdata => [ "statistics_age", $stats_age, undef ];

    if ( defined $args{'warning'} ) {
        my $w_limit = $args{'warning'};
        my $c_limit = $args{'critical'};

        push @{ $perfdata[0] } => ( $w_limit, $c_limit );

        return status_critical( $me, \@msg, \@perfdata ) if $stats_age >= $c_limit;
        return status_warning( $me, \@msg, \@perfdata )  if $stats_age >= $w_limit;
    }

    return status_ok( $me, \@msg, \@perfdata );
}

=item B<streaming_delta> (9.1+)

Check the data delta between a cluster and its standbys in streaming
replication.

Optional argument C<--slave> allows you to specify some slaves that MUST be
connected. This argument can be used as many times as desired to check multiple
slave connections, or you can specify multiple slaves connections at one time,
using comma separated values. Both methods can be used in a single call. The
provided values must be of the form "APPLICATION_NAME IP".
Both following examples will check for the presence of two slaves:

  --slave 'slave1 192.168.1.11' --slave 'slave2 192.168.1.12'
  --slave 'slave1 192.168.1.11','slave2 192.168.1.12'

This service supports a C<--exclude REGEX> parameter to exclude every result
matching a regular expression on application_name or IP address fields.

You can use multiple C<--exclude REGEX>  parameters.

Perfdata returns the data delta in bytes between the master and every standbies
found, the number of standbies connected and the number of excluded standbies.

Critical and Warning thresholds are optional. They can take one or two values
separated by a comma. If only one value is supplied, it applies to both flushed
and replayed data. If two values are supplied, the first one applies to flushed
data, the second one to replayed data. These thresholds only accept a size
(eg. 2.5G).

Required privileges: unprivileged role.

=cut

sub check_streaming_delta {
    my @perfdata;
    my @msg;
    my @msg_crit;
    my @msg_warn;
    my @rs;
    my $w_limit_flushed;
    my $c_limit_flushed;
    my $w_limit_replayed;
    my $c_limit_replayed;
    my @hosts;
    my %slaves;
    my %args            = %{ $_[0] };
    my @exclude         = @{ $args{'exclude'} };
    my $excluded        = 0;
    my $wal_size        = hex('ff000000');
    my $me              = 'POSTGRES_STREAMING_DELTA';
    my $master_location = '';
    my $num_clusters    = 0;
    my %queries         = (
        $PG_VERSION_100 => q{SELECT application_name, client_addr, pid,
            sent_lsn, write_lsn, flush_lsn, replay_lsn,
            CASE pg_is_in_recovery() WHEN true THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END
            FROM pg_stat_replication
        WHERE state NOT IN ('startup', 'backup')},
        $PG_VERSION_92 => q{SELECT application_name, client_addr, pid,
            sent_location, write_location, flush_location, replay_location,
            CASE pg_is_in_recovery() WHEN true THEN pg_last_xlog_receive_location() ELSE pg_current_xlog_location() END
            FROM pg_stat_replication
        WHERE state NOT IN ('startup', 'backup')},
        $PG_VERSION_91 => q{SELECT application_name, client_addr, procpid,
            sent_location, write_location, flush_location, replay_location,
            CASE pg_is_in_recovery() WHEN true THEN pg_last_xlog_receive_location() ELSE pg_current_xlog_location() END
            FROM pg_stat_replication
        WHERE state NOT IN ('startup', 'backup')}
    );
    # FIXME this service should check for given slaves in opts!

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "streaming_delta".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'streaming_delta', $PG_VERSION_91 or exit 1;

    $wal_size = 4294967296 if $hosts[0]{'version_num'} >= $PG_VERSION_93;

    if ( scalar @{ $args{'slave'} } ) {
        $slaves{$_} = 0 foreach ( split ( /,/, join ( ',', @{ $args{'slave'} } ) ) );
    }

    @rs = @{ query_ver( $hosts[0], %queries ) };

    return status_unknown( $me, ['No slaves connected'], \@perfdata )
        unless scalar @rs;

    $rs[0][7] =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
    $master_location = ( $wal_size * hex($1) ) + hex($2);

    if ( defined $args{'critical'} ) {

        ($w_limit_flushed, $w_limit_replayed) = split /,/, $args{'warning'};
        ($c_limit_flushed, $c_limit_replayed) = split /,/, $args{'critical'};

        if (!defined($w_limit_replayed)) {
            $w_limit_replayed = $w_limit_flushed;
        }
        if (!defined($c_limit_replayed)) {
            $c_limit_replayed = $c_limit_flushed;
        }

        $w_limit_flushed = get_size( $w_limit_flushed );
        $c_limit_flushed = get_size( $c_limit_flushed );
        $w_limit_replayed = get_size( $w_limit_replayed );
        $c_limit_replayed = get_size( $c_limit_replayed );
    }

    # Compute deltas
    foreach my $host (@rs) {
        my $send_delta;
        my $write_delta;
        my $flush_delta;
        my $replay_delta;
        my $name;

        if ( grep { $host->[0] =~ m/$_/ or $host->[1] =~ m/$_/ } @exclude ) {
            $excluded++;
            next;
        }

        $num_clusters++;

        $host->[3] =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
        $send_delta = $master_location - ( $wal_size * hex($1) ) - hex($2);

        $host->[4] =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
        $write_delta = $master_location - ( $wal_size * hex($1) ) - hex($2);

        $host->[5] =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
        $flush_delta = $master_location - ( $wal_size * hex($1) ) - hex($2);

        $host->[6] =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
        $replay_delta = $master_location - ( $wal_size * hex($1) ) - hex($2);

        $name = "$host->[0]\@$host->[1]";

        push @perfdata => (
            [ "sent delta $name",    $send_delta,   "B" ],
            [ "wrote delta $name",   $write_delta,  "B" ],
            [ "flushed delta $name", $flush_delta,  "B", $w_limit_flushed, $c_limit_flushed ],
            [ "replay delta $name",  $replay_delta, "B", $w_limit_replayed, $c_limit_replayed ],
            [ "pid $name", $host->[2] ]
        );

        $slaves{"$host->[0] $host->[1]"} = 1;

        if ( defined $args{'critical'} ) {

            if ($flush_delta > $c_limit_flushed) {
                push @msg_crit, "critical flush lag: " . to_size($flush_delta)
                    . " for $name";
                next;
            }

            if ($replay_delta > $c_limit_replayed) {
                push @msg_crit, "critical replay lag: " . to_size($replay_delta)
                    . " for $name";
                next;
            }

            if ($flush_delta > $w_limit_flushed) {
                push @msg_warn, "warning flush lag: ". to_size($flush_delta)
                    . " for $name";
                next;
            }

            if ($replay_delta > $w_limit_replayed) {
                push @msg_warn, "warning replay lag: " . to_size($replay_delta)
                    . " for $name";
                next;
            }
        }
    }

    push @perfdata => [ '# of excluded slaves', $excluded ];
    push @perfdata => [ '# of slaves', scalar @rs || 0 ];

    while ( my ( $host, $connected ) = each %slaves ) {
        unshift @msg_crit => "$host not connected" unless $connected;
    }

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if @msg_warn > 0;

    return status_ok( $me, [ "$num_clusters slaves checked" ], \@perfdata );
}


=item B<table_unlogged> (9.5+)

Check if tables are changed to unlogged. In 9.5, you can switch between logged
and unlogged.

Without C<--critical>  or C<--warning> parameters, this service attempts to
fetch all unlogged tables.

A critical alert is raised if an unlogged table is detected.

This service supports both C<--dbexclude>  and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

This service supports a C<--exclude REGEX>  parameter to exclude relations
matching a regular expression. The regular expression applies to
"database.schema_name.relation_name". This enables you to filter either on a
relation name for all schemas and databases, on a qualified named relation
(schema + relation) for all databases or on a qualified named relation in
only one database.

You can use multiple C<--exclude REGEX>  parameters.

Perfdata will return the number of unlogged tables per database.

A list of the unlogged tables will be returned after the
perfdata. This list contains the fully qualified table name. If
C<--exclude REGEX> is set, the number of excluded tables is returned.

Required privileges: unprivileged role able to log in all databases,
or at least those in C<--dbinclude>.

=cut
sub check_table_unlogged {
    my @perfdata;
    my @longmsg;
    my @rs;
    my @hosts;
    my @all_db;
    my $total_tbl   = 0; # num of tables checked, without excluded ones
    my $total_extbl = 0; # num of excluded tables
    my $c_count     = 0;
    my %args        = %{ $_[0] };
    my @dbinclude   = @{ $args{'dbinclude'} };
    my @dbexclude   = @{ $args{'dbexclude'} };
    my $me          = 'POSTGRES_TABLE_UNLOGGED';
    my $query       = q{
        SELECT current_database(), nsp.nspname AS schemaname, cls.relname,
               cls.relpersistence
        FROM pg_class cls
            join pg_namespace nsp on nsp.oid = cls.relnamespace
        WHERE
            cls.relkind = 'r'
            AND nsp.nspname not like 'pg_toast%'
            AND nsp.nspname NOT IN ('information_schema', 'pg_catalog')
    };

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give one (and only one) host with service "table_unlogged".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'table_unlogged', $PG_VERSION_95 or exit 1;

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db (sort @all_db) {
        my @rc;

        my $nb_tbl       = 0;
        my $tbl_unlogged = 0;

        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        @rc = @{ query( $hosts[0], $query, $db ) };

        UNLOGGED_LOOP: foreach my $unlogged (@rc) {

            foreach my $exclude_re ( @{ $args{'exclude'} } ) {
                if ("$unlogged->[0].$unlogged->[1].$unlogged->[2]" =~ m/$exclude_re/){
                    $total_extbl++;
                    next UNLOGGED_LOOP ;
                }

            }

            # unlogged tables count
            if ($unlogged->[3] eq "u") {
                # long message info :
                push @longmsg => sprintf "%s.%s.%s (unlogged);",
                    $unlogged->[0], $unlogged->[1], $unlogged->[2];

                $tbl_unlogged++;
            }

            $nb_tbl++;
        }

        $total_tbl += $nb_tbl;
        $c_count += $tbl_unlogged;
        push @perfdata => [ "table unlogged in $db", $tbl_unlogged ];
    }

    push @longmsg => sprintf "%i excluded table(s) from check", $total_extbl
        if $total_extbl > 0;

    # we use the critical count for the **total** number of unlogged tables
    return status_critical( $me,
        [ "$c_count/$total_tbl table(s) unlogged" ],
        \@perfdata, \@longmsg
    ) if $c_count > 0;

    return status_ok( $me, [ "No unlogged table" ], \@perfdata, \@longmsg );
}

=item B<table_bloat>

Estimate bloat on tables.

Warning and critical thresholds accept a comma-separated list of either
raw number(for a size), size (eg. 125M) or percentage. The thresholds apply to
B<bloat> size, not object size. If a percentage is given, the threshold will
apply to the bloat size compared to the table + TOAST size.
If multiple threshold values are passed, check_pgactivity will choose the
largest (bloat size) value.

This service supports both C<--dbexclude> and C<--dbinclude> parameters.
The 'postgres' database and templates are always excluded.

This service supports a C<--exclude REGEX> parameter to exclude relations
matching the given regular expression. The regular expression applies to
"database.schema_name.relation_name". This enables you to filter either on a
relation name for all schemas and databases, on a qualified named relation
(schema + relation) for all databases or on a qualified named relation in
only one database.

You can use multiple C<--exclude REGEX> parameters.

B<Warning>: With a non-superuser role, this service can only check the tables
that the given role is granted to read!

Perfdata will return the number of tables matching the warning and critical
thresholds, per database.

A list of the bloated tables will be returned after the
perfdata. This list contains the fully qualified bloated table name, the
estimated bloat size, the table size and the bloat percentage.

Required privileges: superuser (<10) able to log in all databases, or at least
those in C<--dbinclude>; superuser (<10);
on PostgreSQL 10+, a user with the role pg_monitor suffices,
provided that you grant SELECT on the system table pg_statistic
to the pg_monitor role, in each database of the cluster:
C<GRANT SELECT ON pg_statistic TO pg_monitor;>

=cut

sub check_table_bloat {
    my @perfdata;
    my @longmsg;
    my @rs;
    my @hosts;
    my @all_db;
    my $total_tbl = 0; # num of table checked, without excluded ones
    my $w_count   = 0;
    my $c_count   = 0;
    my %args      = %{ $_[0] };
    my @dbinclude = @{ $args{'dbinclude'} };
    my @dbexclude = @{ $args{'dbexclude'} };
    my $me        = 'POSTGRES_TABLE_BLOAT';
    my %queries   = (
        # The base for the following queries come from:
        #   https://github.com/ioguix/pgsql-bloat-estimation
        #
        # Changes:
        # * use pg_statistic instead of pg_stats for performance
        # * as pg_namespace is not useful in subquery "s", move it as the very last join

        # Text types header is 4, page header is 20 and block size 8192 for 7.4.
        # page header is 24 and GUC block_size appears for 8.0
        $PG_VERSION_74 =>  q{
          SELECT current_database(), ns.nspname AS schemaname, tblname, bs*tblpages AS real_size,
            NULL, NULL, NULL, (tblpages-est_num_pages)*bs AS bloat_size,
            CASE WHEN tblpages - est_num_pages > 0
              THEN 100 * (tblpages - est_num_pages)/tblpages::float
              ELSE 0
            END AS bloat_ratio, is_na
          FROM (
            SELECT
              ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_num_pages, tblpages,
              bs, tblid, relnamespace, tblname, heappages, toastpages, is_na
            FROM (
              SELECT
                ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
                  - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
                  - CASE WHEN tpl_data_size::numeric%ma = 0 THEN ma ELSE tpl_data_size::numeric%ma END
                ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + coalesce(toast.relpages, 0)) AS tblpages, heappages,
                coalesce(toast.relpages, 0) AS toastpages, s.reltuples,
                coalesce(toast.reltuples, 0) AS toasttuples, bs, page_hdr, tblid, s.relnamespace, tblname, is_na
              FROM (
                SELECT
                  tbl.oid AS tblid, tbl.relnamespace, tbl.relname AS tblname, tbl.reltuples,
                  tbl.relpages AS heappages, tbl.reltoastrelid,
                  CASE WHEN cluster_version.v > 7
                    THEN current_setting('block_size')::numeric
                    ELSE 8192::numeric
                  END AS bs,
                  CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
                  CASE WHEN cluster_version.v > 7
                    THEN 24
                    ELSE 20
                  END AS page_hdr,
                  CASE WHEN cluster_version.v > 7 THEN 27 ELSE 23 END
                      + CASE WHEN MAX(coalesce(stanullfrac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
                      + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
                  sum( (1-coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 1024) ) AS tpl_data_size,
                  max( CASE WHEN att.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
                FROM pg_attribute att
                  JOIN pg_class tbl ON att.attrelid = tbl.oid
                  JOIN pg_statistic s ON s.starelid=tbl.oid
                    AND s.staattnum=att.attnum
                  CROSS JOIN ( SELECT substring(current_setting('server_version') FROM '#"[0-9]+#"%' FOR '#')::integer ) AS cluster_version(v)
                WHERE att.attnum > 0 AND NOT att.attisdropped
                  AND tbl.relkind = 'r'
                GROUP BY 1,2,3,4,5,6,7,8,9, cluster_version.v, tbl.relhasoids
              ) as s LEFT JOIN pg_class toast ON s.reltoastrelid = toast.oid
            ) as s2
          ) AS s3 JOIN pg_namespace AS ns ON ns.oid = s3.relnamespace
          WHERE NOT is_na
          ORDER BY ns.nspname,s3.tblname},
        # Variable block size, page header is 24 and text types header is 1 or 4 for 8.3+
        $PG_VERSION_82 =>  q{
          SELECT current_database(), ns.nspname AS schemaname, tblname, bs*tblpages AS real_size,
            (tblpages-est_tblpages)*bs AS extra_size,
            CASE WHEN tblpages - est_tblpages > 0
              THEN 100 * (tblpages - est_tblpages)/tblpages::float
              ELSE 0
            END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size,
            CASE WHEN tblpages - est_tblpages_ff > 0
              THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
              ELSE 0
            END AS bloat_ratio, is_na
          FROM (
            SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
              ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
              tblpages, fillfactor, bs, tblid, relnamespace, tblname, heappages, toastpages, is_na
            FROM (
              SELECT
                ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
                  - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
                  - CASE WHEN tpl_data_size::numeric%ma = 0 THEN ma ELSE tpl_data_size::numeric%ma END
                ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + coalesce(toast.relpages, 0)) AS tblpages,
                heappages, coalesce(toast.relpages, 0) AS toastpages,
                s.reltuples, coalesce(toast.reltuples, 0) AS toasttuples,
                bs, page_hdr, tblid, s.relnamespace, tblname, fillfactor, is_na
              FROM (
                SELECT
                  tbl.oid AS tblid, tbl.relnamespace, tbl.relname AS tblname, tbl.reltuples, tbl.reltoastrelid,
                  tbl.relpages AS heappages,
                  coalesce(substring(
                    array_to_string(tbl.reloptions, ' ')
                    FROM '%fillfactor=#"__#"%' FOR '#')::smallint, 100) AS fillfactor,
                  current_setting('block_size')::numeric AS bs,
                  CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
                  24 AS page_hdr,
                  CASE WHEN current_setting('server_version_num')::integer < 80300 THEN 27 ELSE 23 END
                    + CASE WHEN MAX(coalesce(s.stanullfrac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
                    + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
                  sum( (1-coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 1024) ) AS tpl_data_size,
                  bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na
                FROM pg_attribute AS att
                  JOIN pg_class AS tbl ON att.attrelid = tbl.oid
                  JOIN pg_statistic s
                    ON s.starelid = tbl.oid AND s.staattnum=att.attnum
                  LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
                WHERE att.attnum > 0 AND NOT att.attisdropped
                  AND tbl.relkind = 'r'
                GROUP BY 1,2,3,4,5,6,7,8,9,10, tbl.relhasoids
                ORDER BY 2,3
              ) as s LEFT JOIN pg_class toast ON s.reltoastrelid = toast.oid
            ) as s2
          ) AS s3 JOIN pg_namespace AS ns ON ns.oid = s3.relnamespace
          WHERE NOT is_na
          ORDER BY ns.nspname,s3.tblname},
        # Exclude inherited stats
        $PG_VERSION_90 =>  q{
          SELECT current_database(), nspname AS schemaname, tblname, bs*tblpages AS real_size,
            (tblpages-est_tblpages)*bs AS extra_size,
            CASE WHEN tblpages - est_tblpages > 0
              THEN 100 * (tblpages - est_tblpages)/tblpages::float
              ELSE 0
            END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size,
            CASE WHEN tblpages - est_tblpages_ff > 0
              THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
              ELSE 0
            END AS bloat_ratio, is_na
          FROM (
            SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
              ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
              tblpages, fillfactor, bs, tblid, relnamespace, tblname, heappages, toastpages, is_na
            FROM (
              SELECT
                ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
                  - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
                  - CASE WHEN tpl_data_size::numeric%ma = 0 THEN ma ELSE tpl_data_size::numeric%ma END
                ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + coalesce(toast.relpages, 0)) AS tblpages,
                heappages, coalesce(toast.relpages, 0) AS toastpages, s.reltuples,
                coalesce(toast.reltuples, 0) toasttuples, bs, page_hdr, tblid, s.relnamespace, tblname, fillfactor, is_na
              FROM (
                SELECT
                  tbl.oid AS tblid, tbl.relnamespace, tbl.relname AS tblname, tbl.reltoastrelid, tbl.reltuples,
                  tbl.relpages AS heappages,
                  coalesce(substring(
                    array_to_string(tbl.reloptions, ' ')
                    FROM '%fillfactor=#"__#"%' FOR '#')::smallint, 100) AS fillfactor,
                  current_setting('block_size')::numeric AS bs,
                  CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
                  24 AS page_hdr,
                  23 + CASE WHEN MAX(coalesce(s.stanullfrac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
                    + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
                  sum( (1-coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 1024) ) AS tpl_data_size,
                  bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na
                FROM pg_attribute AS att
                  JOIN pg_class AS tbl ON att.attrelid = tbl.oid
                  JOIN pg_statistic AS s ON s.starelid = tbl.oid AND s.stainherit=false AND s.staattnum=att.attnum
                WHERE att.attnum > 0 AND NOT att.attisdropped
                  AND tbl.relkind = 'r'
                GROUP BY 1,2,3,4,5,6,7,8,9, tbl.relhasoids
                ORDER BY 2,3
              ) as s LEFT JOIN pg_class toast ON s.reltoastrelid = toast.oid
            ) as s2
          ) AS s3 JOIN pg_namespace AS ns ON ns.oid = s3.relnamespace
          WHERE NOT is_na
          ORDER BY ns.nspname,s3.tblname},
        # relhasoids has disappeared, performance improvements
        $PG_VERSION_120 => q{
        SELECT current_database(), ns.nspname, tblname, bs*tblpages AS real_size,
          (tblpages-est_tblpages)*bs AS extra_size,
          CASE WHEN tblpages - est_tblpages > 0
            THEN 100 * (tblpages - est_tblpages)/tblpages::float
            ELSE 0
          END AS extra_ratio, fillfactor,
          CASE WHEN tblpages - est_tblpages_ff > 0
            THEN (tblpages-est_tblpages_ff)*bs
            ELSE 0
          END AS bloat_size,
          CASE WHEN tblpages - est_tblpages_ff > 0
            THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
            ELSE 0
          END AS bloat_ratio, is_na
        FROM (
          SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
            ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
            tblpages, fillfactor, bs, tblid, relnamespace, tblname, heappages, toastpages, is_na
          FROM (
            SELECT
            ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
              - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
              - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
            ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
            toastpages, reltuples, toasttuples, bs, page_hdr, tblid, relnamespace, tblname, fillfactor, is_na
            FROM (
              SELECT
                tbl.oid AS tblid, tbl.relnamespace, tbl.relname AS tblname, tbl.reltuples,
                tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
                coalesce(toast.reltuples, 0) AS toasttuples,
                coalesce(substring(
                  array_to_string(tbl.reloptions, ' ')
                    FROM 'fillfactor=([0-9]+)')::smallint, 100
                ) AS fillfactor,
                current_setting('block_size')::numeric AS bs,
                CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
                24 AS page_hdr,
                23 + CASE WHEN MAX(coalesce(s.stanullfrac,0)) > 0 THEN ( 7 + count(s.staattnum) ) / 8 ELSE 0::int END
                 + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
                sum( (1-coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 0) ) AS tpl_data_size,
                bool_or(att.atttypid = 'pg_catalog.name'::regtype)
                  OR sum(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> count(s.staattnum) AS is_na
              FROM pg_attribute AS att
              JOIN pg_class AS tbl ON att.attrelid = tbl.oid
              LEFT JOIN pg_statistic AS s ON s.starelid = tbl.oid AND s.stainherit = false AND s.staattnum = att.attnum
              LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
              WHERE NOT att.attisdropped AND tbl.relkind = 'r'
              GROUP BY 1,2,3,4,5,6,7,8,9,10
              ORDER BY 2,3
            ) AS s
          ) AS s2
        ) AS s3
        JOIN pg_namespace AS ns ON ns.oid = s3.relnamespace
        WHERE NOT is_na
        ORDER BY ns.nspname, s3.tblname },
    );

    # Warning and critical are mandatory.
    pod2usage(
        -message => "FATAL: you must specify critical and warning thresholds.",
        -exitval => 127
    ) unless defined $args{'warning'} and defined $args{'critical'} ;

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "table_bloat".',
        -exitval => 127
    ) if @hosts != 1;

    @all_db = @{ get_all_dbname( $hosts[0] ) };

    # Iterate over all db
    ALLDB_LOOP: foreach my $db (sort @all_db) {
        my @rc;
        # handle max, avg and count for size and percentage, per relkind
        my $nb_tbl      = 0;
        my $tbl_bloated = 0;

        next ALLDB_LOOP if grep { $db =~ /$_/ } @dbexclude;
        next ALLDB_LOOP if @dbinclude and not grep { $db =~ /$_/ } @dbinclude;

        @rc = @{ query_ver( $hosts[0], %queries, $db ) };

        BLOAT_LOOP: foreach my $bloat (@rc) {

            foreach my $exclude_re ( @{ $args{'exclude'} } ) {
                next BLOAT_LOOP if "$bloat->[0].$bloat->[1].$bloat->[2]" =~ m/$exclude_re/;
            }

            my $w_limit = 0;
            my $c_limit = 0;

            # We need to compute effective thresholds on each object,
            # as the value can be given in percentage
            # The biggest calculated size will be used.
            foreach my $cur_warning (split /,/, $args{'warning'}) {
                my $size = get_size( $cur_warning, $bloat->[3] );
                $w_limit = $size if $size > $w_limit;
            }
            foreach my $cur_critical (split /,/, $args{'critical'}) {
                my $size = get_size( $cur_critical, $bloat->[3] );
                $c_limit = $size if $size > $c_limit;
            }

            if ( $bloat->[7] > $w_limit ) {
                $tbl_bloated++;
                $w_count++;
                $c_count++ if $bloat->[7] > $c_limit;

                push @longmsg => sprintf "%s.%s.%s %s/%s (%.2f%%);",
                    $bloat->[0], $bloat->[1], $bloat->[2],
                    to_size($bloat->[7]), to_size($bloat->[3]), $bloat->[8];
            }

            $nb_tbl++;
        }

        $total_tbl += $nb_tbl;

        push @perfdata => [ "table bloated in $db", $tbl_bloated ];
    }

    # We use the warning count for the **total** number of bloated tables
    return status_critical( $me,
        [ "$w_count/$total_tbl table(s) bloated" ],
        \@perfdata, [ @longmsg ]
    ) if $c_count > 0;

    return status_warning( $me,
        [ "$w_count/$total_tbl table(s) bloated" ],
        \@perfdata, [ @longmsg ]
    ) if $w_count > 0;

    return status_ok( $me, [ "Table bloat ok" ], \@perfdata );
}


=item B<temp_files> (8.1+)

Check the number and size of temp files.

This service uses the status file (see C<--status-file> parameter) for 9.2+.

Perfdata returns the number and total size of temp files found in
C<pgsql_tmp> folders. They are aggregated by database until 8.2, then
by tablespace (see GUC temp_tablespaces).

Starting with 9.2, perfdata returns as well the number of temp files per
database since last run, the total size of temp files per database since last
run and the rate at which temp files were generated.

Critical and Warning thresholds are optional. They accept either a number
of file (raw value), a size (unit is B<mandatory> to define a size) or both
values separated by a comma.

Thresholds are applied on current temp files being created AND the number/size
of temp files created since last execution.

Required privileges:
 <10: superuser
 v10: an unprivileged role is possible but it will not monitor databases
that it cannot access, nor live temp files
 v11: an unprivileged role is possible but must be granted EXECUTE
on functions pg_ls_dir(text), pg_read_file(text), pg_stat_file(text, boolean);
the same restrictions than on v10 will still apply
 v12+: a role with pg_monitor privilege.

=cut

sub check_temp_files {
    my $me  = 'POSTGRES_TEMP_FILES';
    my $now = time();
    my $w_flimit;
    my $c_flimit;
    my $w_limit;
    my $c_limit;
    my @perf_flimits;
    my @perf_limits;
    my $obj = 'database(s)';
    my @msg_crit;
    my @msg_warn;
    my @perfdata;
    my @hosts;
    my @rs;
    my %prev_temp_files;
    my %new_temp_files;
    my %args = %{ $_[0] };

    my %queries   = (
        # WARNING: these queries might have a race condition between pg_ls_dir
        # and pg_stat_file!  temp folders are per database
        $PG_VERSION_81 => q{
          SELECT 'live', agg.datname, sum(CASE WHEN agg.tmpfile <> '' THEN 1 ELSE 0 END),
            sum(CASE
              WHEN agg.tmpfile <> '' THEN (pg_stat_file(agg.dir||'/'||agg.tmpfile)).size
              ELSE 0 END)
          FROM (
            SELECT t3.datname, t3.spcname, t3.dbroot||'/'||t3.dbcont AS dir,
              CASE gs.i WHEN 1 THEN pg_ls_dir(t3.dbroot||'/'||t3.dbcont) ELSE '' END AS tmpfile
            FROM (
              SELECT d.datname, t2.spcname, t2.tblroot||'/'||t2.tblcont AS dbroot, pg_ls_dir(t2.tblroot||'/'||t2.tblcont) AS dbcont
              FROM (
                SELECT t.spcname, t.tblroot, pg_ls_dir(tblroot) AS tblcont
                FROM (
                  SELECT spc.spcname, 'pg_tblspc/'||spc.oid AS tblroot
                  FROM pg_tablespace AS spc
                  WHERE spc.spcname !~ '^pg_'
                  UNION ALL
                  SELECT 'pg_default', 'base' AS dir
                ) AS t
              ) AS t2
              JOIN pg_database d ON d.oid=t2.tblcont
            ) AS t3, (SELECT generate_series(1,2) AS i) AS gs
            WHERE t3.dbcont='pgsql_tmp'
          ) AS agg
          GROUP BY 1,2
        },
        # Temp folders are per tablespace
        $PG_VERSION_83 => q{
          SELECT 'live', agg.spcname, sum(CASE WHEN agg.tmpfile <> '' THEN 1 ELSE 0 END),
            sum(CASE
              WHEN agg.tmpfile <> '' THEN (pg_stat_file(agg.dir||'/'||agg.tmpfile)).size
              ELSE 0 END)
          FROM (
            SELECT gs.i, sr.oid, sr.spcname, sr.dir,
              CASE WHEN gs.i = 1 THEN pg_ls_dir(sr.dir) ELSE '' END AS tmpfile
            FROM (
              SELECT spc.oid, spc.spcname, 'pg_tblspc/'||spc.oid||'/pgsql_tmp' AS dir, pg_ls_dir('pg_tblspc/'||spc.oid) AS sub
              FROM (
                SELECT oid, spcname
                FROM pg_tablespace WHERE spcname !~ '^pg_'
              ) AS spc
              UNION ALL
              SELECT 0, 'pg_default', 'base/pgsql_tmp' AS dir, 'pgsql_tmp' AS sub
              FROM pg_ls_dir('base') AS l
              WHERE l='pgsql_tmp'
            ) sr, (SELECT generate_series(1,2) AS i) AS gs
            WHERE sr.sub='pgsql_tmp'
          ) agg
          GROUP BY 1,2
        },
        # Add sub folder PG_9.x_* to pg_tblspc
        $PG_VERSION_90 => q{
          SELECT 'live', agg.spcname, sum(CASE WHEN agg.tmpfile <> '' THEN 1 ELSE 0 END),
            sum(CASE
                WHEN agg.tmpfile <> '' THEN (pg_stat_file(agg.dir||'/'||agg.tmpfile)).size
                ELSE 0 END)
          FROM (
            SELECT ls.oid, ls.spcname, ls.dir||'/'||ls.sub AS dir,
                CASE gs.i WHEN 1 THEN '' ELSE pg_ls_dir(dir||'/'||ls.sub) END AS tmpfile
            FROM (
              SELECT sr.oid, sr.spcname, 'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir,
                pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub
              FROM (
                SELECT spc.oid, spc.spcname, pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root,
                    trim( trailing E'\n ' FROM pg_read_file('PG_VERSION', 0, 100)) as v
                FROM (
                  SELECT oid, spcname
                  FROM pg_tablespace WHERE spcname !~ '^pg_'
                ) AS spc
              ) sr
              WHERE sr.spc_root ~ ('^PG_'||sr.v)
              UNION ALL
              SELECT 0, 'pg_default', 'base' AS dir, 'pgsql_tmp' AS sub
              FROM pg_ls_dir('base') AS l
              WHERE l='pgsql_tmp'
            ) AS ls,
            (SELECT generate_series(1,2) AS i) AS gs
            WHERE ls.sub = 'pgsql_tmp'
          ) agg
          GROUP BY 1,2
        },
        # Add stats from pg_stat_database
        $PG_VERSION_92 => q{
          SELECT 'live', agg.spcname, sum(CASE WHEN agg.tmpfile <> '' THEN 1 ELSE 0 END),
            sum(CASE
                WHEN agg.tmpfile <> '' THEN (pg_stat_file(agg.dir||'/'||agg.tmpfile)).size
                ELSE 0 END)
          FROM (
            SELECT ls.oid, ls.spcname, ls.dir||'/'||ls.sub AS dir,
                CASE gs.i WHEN 1 THEN '' ELSE pg_ls_dir(dir||'/'||ls.sub) END AS tmpfile
            FROM (
              SELECT sr.oid, sr.spcname, 'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir,
                pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub
              FROM (
                SELECT spc.oid, spc.spcname, pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root,
                    trim( trailing E'\n ' FROM pg_read_file('PG_VERSION')) as v
                FROM (
                  SELECT oid, spcname
                  FROM pg_tablespace WHERE spcname !~ '^pg_'
                ) AS spc
              ) sr
              WHERE sr.spc_root ~ ('^PG_'||sr.v)
              UNION ALL
              SELECT 0, 'pg_default', 'base' AS dir, 'pgsql_tmp' AS sub
              FROM pg_ls_dir('base') AS l
              WHERE l='pgsql_tmp'
            ) AS ls,
            (SELECT generate_series(1,2) AS i) AS gs
            WHERE ls.sub = 'pgsql_tmp'
          ) agg
          GROUP BY 1,2
          UNION ALL
          SELECT 'db', d.datname, s.temp_files, s.temp_bytes
          FROM pg_database AS d
          JOIN pg_stat_database AS s ON s.datid=d.oid
          WHERE datallowconn
        },
        # Specific query to handle superuser and non-superuser roles in
        # PostgreSQL 10 the WHERE current_setting('is_superuser')::bool clause
        # does all the magic Also, the previous query was not working with
        # PostgreSQL 10
        $PG_VERSION_100 => q{
          SELECT 'live', agg.spcname, count(agg.tmpfile),
                 SUM(COALESCE((pg_stat_file(agg.dir||'/'||agg.tmpfile, true)).size, 0)) AS SIZE
            FROM ( SELECT ls.oid, ls.spcname AS spcname,
                          ls.dir||'/'||ls.sub AS dir,
                          pg_ls_dir(ls.dir||'/'||ls.sub) AS tmpfile
                     FROM ( SELECT sr.oid, sr.spcname,
                                   'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir,
                                   pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub
                              FROM ( SELECT spc.oid, spc.spcname,
                                            pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root,
                                            trim(TRAILING e'\n ' FROM pg_read_file('PG_VERSION')) AS v
                                       FROM ( SELECT oid, spcname
                                                FROM pg_tablespace
                                               WHERE spcname !~ '^pg_' ) AS spc ) sr
                             WHERE sr.spc_root ~ ('^PG_'||sr.v)
                             UNION ALL
                 SELECT 0, 'pg_default', 'base' AS dir, 'pgsql_tmp' AS sub
                               FROM pg_ls_dir('base') AS l
                              WHERE l='pgsql_tmp'
              ) AS ls
                 ) AS agg
           WHERE current_setting('is_superuser')::bool
           GROUP BY 1, 2
          UNION ALL
          SELECT 'db', d.datname, s.temp_files, s.temp_bytes
            FROM pg_database AS d
            JOIN pg_stat_database AS s ON s.datid=d.oid
        },
	# Use pg_ls_tmpdir with PostgreSQL 12
	# The technic to bypass function execution for non-superuser roles used in
	# the query PG_VERSION_100 does not work anymore since commit b8d7f053c5c in
	# PostgreSQL. From now on, this probe requires at least a pg_monitor role to
	# perform with PostgreSQL >= 12.
        $PG_VERSION_120 => q{
          SELECT 'live', agg.spcname, count(agg.name),
                 SUM(agg.size) AS SIZE
            FROM (
            SELECT ts.spcname,
                   tmp.name,
                   tmp.size
              FROM pg_tablespace ts,
              LATERAL pg_catalog.pg_ls_tmpdir(ts.oid) tmp
                 WHERE spcname <> 'pg_global'
                 ) AS agg
           GROUP BY 1, 2
          UNION ALL
          SELECT 'db', d.datname, s.temp_files, s.temp_bytes
            FROM pg_database AS d
            JOIN pg_stat_database AS s ON s.datid=d.oid;
        },
    );

    @hosts = @{ parse_hosts %args };

    is_compat $hosts[0], 'temp_files', $PG_VERSION_81 or exit 1;

    $obj = 'tablespace(s)' if $hosts[0]{'version_num'} >= $PG_VERSION_83;
    $obj = 'tablespace(s)/database(s)' if $hosts[0]{'version_num'} >= $PG_VERSION_92;

    pod2usage(
        -message => 'FATAL: you must give only one host with service "temp_files".',
        -exitval => 127
    ) if @hosts != 1;

    if ( defined $args{'warning'} and defined $args{'critical'} ) {

        while ( $args{'warning'} =~ m/(?:(\d+)([kmgtpez]?b)?)/ig ) {
            if ( defined $2 ) {
                $w_limit = get_size("$1$2");
            }
            else {
                $w_flimit = $1;
            }
        }

        while ( $args{'critical'} =~ m/(?:(\d+)([kmgtpez]?b)?)/ig ) {
            if ( defined $2 ) {
                $c_limit = get_size("$1$2");
            }
            else {
                $c_flimit = $1;
            }
        }

        pod2usage(
            -message => 'FATAL: you must give the number file thresholds '
                .'for both warning AND critical if used with service "temp_files".',
            -exitval => 127
        ) if (defined $w_flimit and not defined $c_flimit)
            or (not defined $w_flimit and defined $c_flimit);

        pod2usage(
            -message => 'FATAL: you must give the total size thresholds '
                .'for both warning AND critical if used with service "temp_files".',
            -exitval => 127
        ) if (defined $w_limit and not defined $c_limit)
            or (not defined $w_limit and defined $c_limit);

        @perf_flimits = ( $w_flimit, $c_flimit ) if defined $w_flimit;
        @perf_limits  = ( $w_limit, $c_limit ) if defined $w_limit;
    }

    %prev_temp_files = %{ load( $hosts[0], 'temp_files', $args{'status-file'} ) || {} };

    @rs = @{ query_ver( $hosts[0], %queries ) };

DB_LOOP: foreach my $stat (@rs) {
        my $frate;
        my $brate;
        my $last_check;
        my $last_number;
        my $last_size;
        my $diff_number;
        my $diff_size;

        if ( $stat->[0] eq 'live' ) {
            push @perfdata => [ "# files in $stat->[1]", $stat->[2], 'File', @perf_flimits ];
            push @perfdata => [ "Total size in $stat->[1]", $stat->[3], 'B', @perf_limits ];

            if ( defined $c_limit) {
                if ( $stat->[3] > $c_limit ) {
                    push @msg_crit => sprintf("%s (%s file(s)/%s)",
                        $stat->[1], $stat->[2], to_size($stat->[3])
                    );
                    next DB_LOOP;
                }

                push @msg_warn => sprintf("%s (%s file(s)/%s)",
                    $stat->[1], $stat->[2], to_size($stat->[3])
                ) if $stat->[3] > $w_limit;
            }

            if ( defined $c_flimit) {
                if ( $stat->[2] > $c_flimit ) {
                    push @msg_crit => sprintf("%s (%s file(s)/%s)",
                        $stat->[1], $stat->[2], to_size($stat->[3])
                    );
                    next DB_LOOP;
                }

                push @msg_warn => sprintf("%s (%s file(s)/%s)",
                    $stat->[1], $stat->[2], to_size($stat->[3])
                ) if $stat->[2] > $w_flimit;
            }

            next DB_LOOP;
        }

        $new_temp_files{ $stat->[1] } = [ $now, $stat->[2], $stat->[3] ];
        next DB_LOOP unless defined $prev_temp_files{ $stat->[1] };

        $last_check  = $prev_temp_files{ $stat->[1] }[0];
        $last_number = $prev_temp_files{ $stat->[1] }[1];
        $last_size   = $prev_temp_files{ $stat->[1] }[2];
        $diff_number = $stat->[2] - $last_number;
        $diff_size   = $stat->[3] - $last_size;

        $frate = 60 * $diff_number / ($now - $last_check);
        $brate = 60 * $diff_size   / ($now - $last_check);

        push @perfdata => [ "$stat->[1]", $frate, 'Fpm' ];
        push @perfdata => [ "$stat->[1]", $brate, 'Bpm' ];
        push @perfdata => [ "$stat->[1]", $diff_number, 'Files', @perf_flimits ];
        push @perfdata => [ "$stat->[1]", $diff_size, 'B', @perf_limits ];

        if ( defined $c_limit) {
            if ( $diff_size > $c_limit ) {
                push @msg_crit => sprintf("%s (%s file(s)/%s)",
                    $stat->[1], $diff_number, to_size($diff_size)
                );
                next DB_LOOP;
            }

            push @msg_warn => sprintf("%s (%s file(s)/%s)",
                $stat->[1], $diff_number, to_size($diff_size)
            ) if $diff_size > $w_limit;
        }

        if ( defined $c_flimit) {
            if ( $diff_number > $c_flimit ) {
                push @msg_crit => sprintf("%s (%s file(s)/%s)",
                    $stat->[1], $diff_number, to_size($diff_size)
                );
                next DB_LOOP;
            }

            push @msg_warn => sprintf("%s (%s file(s)/%s)",
                $stat->[1], $diff_number, to_size($diff_size)
            ) if $diff_number > $w_flimit;
        }
    }

    save $hosts[0], 'temp_files', \%new_temp_files, $args{'status-file'};

    return status_critical( $me, [ @msg_crit, @msg_warn ], \@perfdata )
        if scalar @msg_crit > 0;

    return status_warning( $me, \@msg_warn, \@perfdata ) if scalar @msg_warn > 0;

    return status_ok( $me, [ scalar(@rs) . " $obj checked" ], \@perfdata );
}


=item B<uptime> (8.1+)

Returns time since postmaster start ("uptime", from 8.1),
since configuration reload (from 8.4),
and since shared memory initialization (from 10).

Please note that the uptime is unaffected when the postmaster resets
all its children (for example after a kill -9 on a process or a failure).

From 10+, the 'time since shared memory init' aims at detecting this situation:
in fact we use the age of the oldest non-client child process (usually
checkpointer, writer or startup). This needs pg_monitor access to read
pg_stat_activity.

Critical and Warning thresholds are optional. If both are set, Critical is
raised when the postmaster uptime or the time since shared memory
initialization is less than the critical threshold.

Warning is raised when the time since configuration reload is less than the
warning threshold.  If only a warning or critical threshold is given, it will
be used for both cases.  Obviously these alerts will disappear from themselves
once enough time has passed.

Perfdata contain the three values (when available).

Required privileges: pg_monitor on PG10+; otherwise unprivileged role.

=cut

sub check_uptime {
    my @rs;
    my @hosts;
    my @perfdata;
    my @msg;
    my @msg_warn;
    my @msg_crit;
    my $uptime;
    my $shmem_init_time;
    my $reload_conf_time;
    my $reload_conf_flag;
    my $msg_uptime;
    my $msg_shmem_init_time;
    my $msg_reload_conf;
    my $c_limit;
    my $w_limit;
    my $me           = 'POSTGRES_UPTIME';
    my %queries      = (
        $PG_VERSION_81 => q{
            SELECT extract('epoch' from (current_timestamp - pg_postmaster_start_time())) AS time_since_postmaster_start,
                   null,
                   pg_postmaster_start_time() as postmaster_start_time
        },
        $PG_VERSION_84 => q{
            SELECT extract('epoch' from (current_timestamp - pg_postmaster_start_time())) AS time_since_postmaster_start,
                   extract('epoch' from (current_timestamp - pg_conf_load_time())) AS time_since_conf_reload,
                   pg_postmaster_start_time(),
                   pg_conf_load_time()
        },
        $PG_VERSION_100 => q{
            SELECT extract('epoch' from (current_timestamp - pg_postmaster_start_time())) AS time_since_postmaster_start,
                   extract('epoch' from (current_timestamp - pg_conf_load_time())) AS time_since_conf_reload,
                   pg_postmaster_start_time(),
                   pg_conf_load_time(),
                   -- oldest child (usually checkpointer, startup...)
                   extract('epoch' from (current_timestamp - min(backend_start))) AS age_oldest_child_process,
                   min(backend_start) AS oldest_child_process
            FROM pg_stat_activity WHERE backend_type != 'client backend'
        }
    );

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "uptime".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'uptime', $PG_VERSION_81 or exit 1;

    $c_limit = get_time $args{'critical'} if (defined $args{'critical'}) ;
    $w_limit = get_time $args{'warning'}  if (defined $args{'warning'});

    @rs = @{ query_ver( $hosts[0], %queries ) };
    $uptime = int( $rs[0][0] );
    $msg_uptime = "postmaster started for ".to_interval($uptime)." (since $rs[0][2])" ;
    push @perfdata => [ 'postmaster uptime', $uptime , 's', undef, undef, 0 ];

    # time since configuration reload
    $reload_conf_flag = !(check_compat $hosts[0], $PG_VERSION_81, $PG_VERSION_84);
    if ($reload_conf_flag) {
        $reload_conf_time = int( $rs[0][1] );
        $msg_reload_conf = "configuration reloaded for ".to_interval($reload_conf_time)." (since $rs[0][3])";
        push @perfdata => [ 'configuration uptime', $reload_conf_time , 's',
            undef, undef, 0 ];
    } else {
        $msg_reload_conf = "";
    };

    # time since last share memory reinit
    if ( check_compat $hosts[0], $PG_VERSION_100 ) {
        $shmem_init_time = int ( $rs[0][4] );
        $msg_shmem_init_time = "shared memory initialized for ".to_interval($shmem_init_time)." (since $rs[0][5])";
        push @perfdata => [ 'shmem init time', $shmem_init_time , 's', undef, undef, 0 ];
    }

    # uptime check
    if ( defined $c_limit and $uptime < $c_limit ) {
        push @msg_crit => $msg_uptime;
    }
    elsif ( not defined $c_limit and defined $w_limit and $uptime < $w_limit ) {
        push @msg_warn => $msg_uptime;
    }
    else {
        push @msg => $msg_uptime;
    }

    # shmem init check
    if ( defined $shmem_init_time and defined $c_limit and $shmem_init_time < $c_limit ) {
        push @msg_crit => $msg_shmem_init_time;
    }
    elsif ( defined $shmem_init_time and not defined $c_limit and defined $w_limit
            and $shmem_init_time < $w_limit
    ) {
        push @msg_warn => $msg_shmem_init_time;
    }
    elsif ( defined $shmem_init_time ) {
        push @msg => $msg_shmem_init_time;
    }

    # reload check
    if ( $reload_conf_flag and defined $c_limit and not defined $w_limit
         and $reload_conf_time < $c_limit
    ) {
        push @msg_crit => $msg_reload_conf;
    }
    elsif ($reload_conf_flag and defined $w_limit and $reload_conf_time < $w_limit) {
        push @msg_warn => $msg_reload_conf;
    }
    elsif ( $reload_conf_flag ) {
        push @msg => $msg_reload_conf;
    }

    return status_critical( $me, [ @msg_crit, @msg_warn, @msg ], \@perfdata )
        if @msg_crit;

    return status_warning( $me, [ @msg_warn, @msg ], \@perfdata )
        if @msg_warn;

    return status_ok( $me, \@msg, \@perfdata );
}

=item B<wal_files> (8.1+)

Check the number of WAL files.

Perfdata returns the total number of WAL files, current number of written WAL,
the current number of recycled WAL, the rate of WAL written to disk since the
last execution on the master cluster and the current timeline.

Critical and Warning thresholds accept either a raw number of files or a
percentage. In case of percentage, the limit is computed based on:

  100% = 1 + checkpoint_segments * (2 + checkpoint_completion_target)

For PostgreSQL 8.1 and 8.2:

  100% = 1 + checkpoint_segments * 2

If C<wal_keep_segments> is set for 9.0 to 9.4, the limit is the greatest
of the following formulas:

  100% = 1 + checkpoint_segments * (2 + checkpoint_completion_target)
  100% = 1 + wal_keep_segments + 2 * checkpoint_segments

For 9.5 to 12, the limit is:

  100% =  max_wal_size      (as a number of WAL)
        + wal_keep_segments (if set)

For 13 and above:

  100% =  max_wal_size + wal_keep_size (as numbers of WAL)

Required privileges:
 <10:superuser (<10)
 v10:unprivileged user with pg_monitor
 v11+ :unprivileged user with pg_monitor, or with grant EXECUTE on function
pg_ls_waldir

=cut

sub check_wal_files {
    my $seg_written  = 0;
    my $seg_recycled = 0;
    my $seg_kept     = 0;
    my $num_seg      = 0;
    my $tli;
    my $max_segs;
    my $first_seg;
    my @rs;
    my @perfdata;
    my @msg;
    my @hosts;
    my %args     = %{ $_[0] };
    my $me       = 'POSTGRES_WAL_FILES';
    my $wal_size = hex('ff000000');
    my %queries  = (
     # The logic of these queries is mainly to compute a number of WALs to
     # compare against the current number of WALs (see rules above).
     # Parameters and the units stored in pg_settings have changed often across
     # versions.
     $PG_VERSION_130 => q{
        WITH wal_settings AS (
          SELECT sum(setting::int) filter (WHERE name='max_wal_size') as max_wal_size, -- unit: MB
                 sum(setting::int) filter (WHERE name='wal_segment_size') as wal_segment_size, -- unit: B
                 sum(setting::int) filter (WHERE name='wal_keep_size') as wal_keep_size -- unit: MB
          FROM pg_settings
          WHERE name IN ('max_wal_size','wal_segment_size','wal_keep_size')
        )
        SELECT s.name,
          (wal_keep_size + max_wal_size) / (wal_segment_size/1024^2)  AS max_nb_wal, -- unit: nb of WALs
          CASE WHEN pg_is_in_recovery()
            THEN NULL
            ELSE pg_current_wal_lsn()
          END,
          floor(wal_keep_size / (wal_segment_size/1024^2)) AS wal_keep_segments, -- unit: nb of WALs
          (pg_control_checkpoint()).timeline_id     AS tli
        FROM pg_ls_waldir() AS s
        CROSS JOIN wal_settings
        WHERE name ~ '^[0-9A-F]{24}$'
        ORDER BY
          s.modification DESC,
          name DESC},
     $PG_VERSION_110 => q{
        WITH wal_settings AS (
          SELECT sum(setting::int) filter (WHERE name='max_wal_size') as max_wal_size, -- unit: MB
                 sum(setting::int) filter (WHERE name='wal_segment_size') as wal_segment_size, -- unit: B
                 sum(setting::int) filter (WHERE name='wal_keep_segments') as wal_keep_segments -- unit: nb of WALs
          FROM pg_settings
          WHERE name IN ('max_wal_size','wal_segment_size','wal_keep_segments')
        )
        SELECT s.name,
          wal_keep_segments + max_wal_size / (wal_segment_size / 1024^2)  AS max_nb_wal, --unit: nb of WALs
          CASE WHEN pg_is_in_recovery()
            THEN NULL
            ELSE pg_current_wal_lsn()
          END,
          wal_keep_segments, -- unit: nb of WALs
          (pg_control_checkpoint()).timeline_id AS tli
        FROM pg_ls_waldir() AS s
        CROSS JOIN wal_settings
        WHERE name ~ '^[0-9A-F]{24}$'
        ORDER BY
          s.modification DESC,
          name DESC},
     $PG_VERSION_100 => q{
        WITH wal_settings AS (
          SELECT sum(setting::int) filter (WHERE name='max_wal_size') as max_wal_size, --unit: MB
                 sum(setting::int) filter (WHERE name='wal_segment_size') as wal_segment_size, --usually 2048 (blocks)
                 sum(setting::int) filter (WHERE name='wal_block_size') as wal_block_size, --usually 8192
                 sum(setting::int) filter (WHERE name='wal_keep_segments') as wal_keep_segments -- unit:nb of WALs
          FROM pg_settings
          WHERE name IN ('max_wal_size','wal_segment_size','wal_block_size','wal_keep_segments')
        )
        SELECT s.name,
          wal_keep_segments
           + (max_wal_size / (wal_block_size * wal_segment_size / 1024^2)) AS max_nb_wal, --unit: nb of WALs
          CASE WHEN pg_is_in_recovery()
            THEN NULL
            ELSE pg_current_wal_lsn()
          END,
          wal_keep_segments,
          (pg_control_checkpoint()).timeline_id AS tli
        FROM pg_ls_waldir() AS s
        CROSS JOIN wal_settings
        WHERE name ~ '^[0-9A-F]{24}$'
        ORDER BY
          s.modification DESC,
          name DESC},
     $PG_VERSION_95 => q{
        WITH wal_settings AS (
          SELECT setting::int + current_setting('wal_keep_segments')::int as max_nb_wal --unit: nb of WALs
          FROM pg_settings
          WHERE name = 'max_wal_size' -- unit for max_wal_size: 16MB
        )
        SELECT s.f,
          max_nb_wal,
          CASE WHEN pg_is_in_recovery()
            THEN NULL
            ELSE pg_current_xlog_location()
          END,
          current_setting('wal_keep_segments')::integer,
          substring(s.f from 1 for 8) AS tli
        FROM pg_ls_dir('pg_xlog') AS s(f)
        CROSS JOIN wal_settings
        WHERE f ~ '^[0-9A-F]{24}$'
        ORDER BY
          (pg_stat_file('pg_xlog/'||s,true)).modification DESC,
          f DESC},
      $PG_VERSION_90 => q{
        SELECT s.f,
          greatest(
            1 + current_setting('checkpoint_segments')::float4 *
              (2 + current_setting('checkpoint_completion_target')::float4),
            1 + current_setting('wal_keep_segments')::float4 +
              2 * current_setting('checkpoint_segments')::float4
          ),
          CASE WHEN pg_is_in_recovery()
            THEN NULL
            ELSE pg_current_xlog_location()
          END,
          current_setting('wal_keep_segments')::integer,
          substring(s.f from 1 for 8) AS tli
        FROM pg_ls_dir('pg_xlog') AS s(f)
        WHERE f ~ '^[0-9A-F]{24}$'
        ORDER BY
          (pg_stat_file('pg_xlog/'||s.f)).modification DESC,
          f DESC},
      $PG_VERSION_83 => q{
        SELECT s.f,
          1 + (
            current_setting('checkpoint_segments')::float4
            * ( 2 + current_setting('checkpoint_completion_target')::float4 )
          ), pg_current_xlog_location(),
          NULL, substring(s.f from 1 for 8) AS tli
        FROM pg_ls_dir('pg_xlog') AS s(f)
        WHERE f ~ '^[0-9A-F]{24}$'
        ORDER BY
          (pg_stat_file('pg_xlog/'||s.f)).modification DESC,
          f DESC},
      $PG_VERSION_82 => q{
        SELECT s.f,
          1 + (current_setting('checkpoint_segments')::integer * 2), pg_current_xlog_location(),
          NULL, substring(s.f from 1 for 8) AS tli
        FROM pg_ls_dir('pg_xlog') AS s(f)
        WHERE f ~ '^[0-9A-F]{24}$'
        ORDER BY
          (pg_stat_file('pg_xlog/'||s.f)).modification DESC,
          f DESC},
      $PG_VERSION_81 => q{
        SELECT s.f,
          1 + (current_setting('checkpoint_segments')::integer * 2),
          NULL, NULL, substring(s.f from 1 for 8) AS tli
        FROM pg_ls_dir('pg_xlog') AS s(f)
        WHERE f ~ '^[0-9A-F]{24}$'
        ORDER BY
          (pg_stat_file('pg_xlog/'||s.f)).modification DESC,
          f DESC}
    );

    if ( defined $args{'warning'} ) {
        # warning and critical are mandatory.
        pod2usage(
            -message => "FATAL: you must specify critical and warning thresholds.",
            -exitval => 127
        ) unless defined $args{'warning'} and defined $args{'critical'} ;

        # warning and critical must be raw or %.
        pod2usage(
            -message => "FATAL: critical and warning thresholds only accept raw numbers or %.",
            -exitval => 127
        ) unless $args{'warning'}  =~ m/^([0-9.]+)%?$/
            and  $args{'critical'} =~ m/^([0-9.]+)%?$/;
    }

    @hosts = @{ parse_hosts %args };

    pod2usage(
        -message => 'FATAL: you must give only one host with service "wal_files".',
        -exitval => 127
    ) if @hosts != 1;

    is_compat $hosts[0], 'wal_files', $PG_VERSION_81 or exit 1;

    $wal_size = 4294967296 if $hosts[0]{'version_num'} >= $PG_VERSION_93;

    @rs = @{ query_ver( $hosts[0], %queries ) };

    $first_seg = $rs[0][0];
    $max_segs  = $rs[0][1]; #segments to keep including kept segments
    $tli = hex($rs[0][4]);

    foreach my $r (@rs) {
        $num_seg++;
        $seg_recycled++ if $r->[0] gt $first_seg;
    }

    $seg_written = $num_seg - $seg_recycled;

    push @perfdata => [ "total_wal", $num_seg, undef ];
    push @perfdata => [ "recycled_wal", $seg_recycled ];
    push @perfdata => [ "tli", $tli ];

    # pay attention to the wal_keep_segment in perfdata
    if ( $hosts[0]{'version_num'} >= $PG_VERSION_90) {
        $seg_kept = $rs[0][3];
        if ($seg_kept > 0) {
            # cheat with numbers if the keep_segment was just set and the
            # number of wal doesn't match it yet.
            if ($seg_kept > $seg_written) {
                push @perfdata => [ "written_wal", 1 ];
                push @perfdata => [ "kept_wal", $seg_written - 1 ];
            }
            else {
                push @perfdata => [ "written_wal", $seg_written - $seg_kept ];
                push @perfdata => [ "kept_wal", $seg_kept ];
            }
        }
        else {
            push @perfdata => [ "written_wal", $seg_written ];
            push @perfdata => [ "kept_wal", 0 ];
        }
    }
    else {
        push @perfdata => [ "written_wal", $seg_written ];
    }

    push @msg => "$num_seg WAL files";

    if ( $hosts[0]{'version_num'} >= $PG_VERSION_82 and $rs[0][2] ne '') {
        my $now = time();
        my $curr_lsn = $rs[0][2];
        my @prev_lsn = @{ load( $hosts[0], 'last wal files LSN', $args{'status-file'} ) || [] };

        $curr_lsn =~ m{^([0-9A-F]+)/([0-9A-F]+)$};
        $curr_lsn = ( $wal_size * hex($1) ) + hex($2);

        unless ( @prev_lsn == 0 or $now == $prev_lsn[0] ) {
            my $rate = ($curr_lsn - $prev_lsn[1])/($now - $prev_lsn[0]);
            $rate = int($rate*100+0.5)/100;

            push @perfdata => [ "wal_rate", $rate, 'Bps' ];
        }

        save $hosts[0], 'last wal files LSN', [ $now, $curr_lsn ], $args{'status-file'};
    }

    if ( defined $args{'warning'} ) {
        my $w_limit = get_size( $args{'warning'},  $max_segs );
        my $c_limit = get_size( $args{'critical'}, $max_segs );

        push @{ $perfdata[0] } => ( $w_limit, $c_limit, 1, $max_segs );

        return status_critical( $me, \@msg, \@perfdata ) if $num_seg >= $c_limit;
        return status_warning( $me, \@msg, \@perfdata )  if $num_seg >= $w_limit;
    }

    return status_ok( $me, \@msg, \@perfdata );
}

# End of SERVICE section in pod doc
=pod

=back

=cut

Getopt::Long::Configure('bundling');
GetOptions(
    \%args,
        'checkpoint_segments=i',
        'critical|c=s',
        'dbexclude=s',
        'dbinclude=s',
        'debug!',
        'detailed!',
        'dump-status-file!',
        'dump-bin-file:s',
        'effective_cache_size=i',
        'exclude=s',
        'format|F=s',
        'global-pattern=s',
        'help|?!',
        'host|h=s',
        'ignore-wal-size!',
        'unarchiver=s',
        'dbname|d=s',
        'dbservice|S=s',
        'list|l!',
        'maintenance_work_mem=i',
        'no_check_autovacuum!',
        'no_check_enable!',
        'no_check_fsync!',
        'no_check_track_counts!',
        'output|o=s',
        'path=s',
        'pattern=s',
        'port|p=s',
        'psql|P=s',
        'query=s',
        'reverse!',
        'save!',
        'service|s=s',
        'shared_buffers=i',
        'slave=s',
        'status-file=s',
        'suffix=s',
        'timeout|t=s',
        'tmpdir=s',
        'type=s',
        'username|U=s',
        'uid=s',
        'version|V!',
        'wal_buffers=i',
        'warning|w=s',
        'work_mem=i'
) or pod2usage( -exitval => 127 );

list_services() if $args{'list'};
version()       if $args{'version'};

pod2usage( -verbose => 2 ) if $args{'help'};

dump_status_file( $args{'dump-bin-file'} ) if $args{'dump-status-file'}
                                           or defined $args{'dump-bin-file'};


# One service must be given
pod2usage(
    -message => "FATAL: you must specify one service.\n"
        . "    See -s or --service command line option.",
    -exitval => 127
) unless defined $args{'service'};


# Check that the given service exists.
pod2usage(
    -message => "FATAL: service $args{'service'} does not exist.\n"
        . "    Use --list to show the available services.",
    -exitval => 127
) unless exists $services{ $args{'service'} };


# Check we have write permission to the tempdir
pod2usage(
    -message => 'FATAL: temp directory given or found not writable.',
    -exitval => 127
) if not -d $args{'tmpdir'} or not -x $args{'tmpdir'};

# Both critical and warning must be given is optional,
# but for pga_version, minor_version and uptime which use only one of them or
# none
pod2usage(
    -message => 'FATAL: you must provide both warning and critical thresholds.',
    -exitval => 127
) if $args{'service'} !~ m/^(pga_version|minor_version|uptime)$/ and (
    ( defined $args{'critical'} and not defined $args{'warning'} )
    or ( not defined $args{'critical'} and defined $args{'warning'} ));

# Query, type and reverse are only allowed with "custom_query" service
pod2usage(
    -message => 'FATAL: query, type and reverse are only allowed with "custom_query" service.',
    -exitval => 127
) if ( ( defined $args{'query'} or defined $args{'type'} or $args{'reverse'} == 1 ) and ( $args{'service'} ne 'custom_query' ) );


# Check "configuration" specific arg
pod2usage(
    -message => 'FATAL: work_mem, maintenance_work_mem, shared_buffers, wal_buffers, checkpoint_segments, effective_cache_size, no_check_autovacuum, no_check_fsync, no_check_enable, no_check_track_counts are only allowed with "configuration" service.',
    -exitval => 127
) if ( (defined $args{'work_mem'} or defined $args{'maintenance_work_mem'} or defined $args{'shared_buffers'}
    or defined $args{'wal_buffers'} or defined $args{'checkpoint_segments'} or defined $args{'effective_cache_size'}
    or $args{'no_check_autovacuum'} == 1 or $args{'no_check_fsync'} == 1 or $args{'no_check_enable'} ==1
    or $args{'no_check_track_counts'} == 1) and ( $args{'service'} ne 'configuration' ) );


# Check "archive_folder" specific args --ignore-wal-size and --suffix
pod2usage(
    -message => 'FATAL: "ignore-wal-size" and "suffix" are only allowed with "archive_folder" service.',
    -exitval => 127
) if ( $args{'ignore-wal-size'} or $args{'suffix'} )
    and $args{'service'} ne 'archive_folder';


# Check "streaming_delta" specific args --slave
pod2usage(
    -message => 'FATAL: "slave" is only allowed with "streaming_delta" service.',
    -exitval => 127
) if scalar @{ $args{'slave'} }  and $args{'service'} ne 'streaming_delta';


# Check "oldest_xmin" specific args --detailed
pod2usage(
    -message => 'FATAL: "detailed" argument is only allowed with "oldest_xmin" service.',
    -exitval => 127
) if scalar $args{'detailed'} and $args{'service'} ne 'oldest_xmin';


# Set psql absolute path
unless ($args{'psql'}) {
    if ( $ENV{PGBINDIR} ) {
        $args{'psql'} = "$ENV{PGBINDIR}/psql";
    }
    else {
        $args{'psql'} = 'psql';
    }
}

# Pre-compile given regexp
unless (($args{'service'} eq 'pg_dump_backup') or ($args{'service'} eq 'oldest_idlexact')) {
    $_ = qr/$_/ for @{ $args{'exclude'} } ;
    $_ = qr/$_/ for @{ $args{'dbinclude'} };
}

$_ = qr/$_/ for @{ $args{'dbexclude'} };

# Output format
for ( $args{'format'} ) {
       if ( /^binary$/        ) { $output_fmt = \&bin_output  }
    elsif ( /^debug$/         ) { $output_fmt = \&debug_output  }
    elsif ( /^human$/         ) { $output_fmt = \&human_output  }
    elsif ( /^nagios$/        ) { $output_fmt = \&nagios_output }
    elsif ( /^nagios_strict$/ ) { $output_fmt = \&nagios_strict_output }
    elsif ( /^json$/          ) { $output_fmt = \&json_output }
    elsif ( /^json_strict$/   ) { $output_fmt = \&json_strict_output }
    else {
        pod2usage(
            -message => "FATAL: unrecognized output format \"$_\" (see \"--format\")",
            -exitval => 127
        );
    }
}

if ( $args{'format'} =~ '^json' ) {
    require JSON::PP;
    JSON::PP->import;
}

exit $services{ $args{'service'} }{'sub'}->( \%args );

__END__

=head2 EXAMPLES

=over

=item Execute service "last_vacuum" on host "host=localhost port=5432":

  check_pgactivity -h localhost -p 5432 -s last_vacuum -w 30m -c 1h30m

=item Execute service "hot_standby_delta" between hosts "service=pg92" and "service=pg92s":

  check_pgactivity --dbservice pg92,pg92s --service hot_standby_delta -w 32MB -c 160MB

=item Execute service "streaming_delta" on host "service=pg92" to check its slave "stby1" with the IP address "192.168.1.11":

  check_pgactivity --dbservice pg92 --slave "stby1 192.168.1.11" --service streaming_delta -w 32MB -c 160MB

=item Execute service "hit_ratio" on host "slave" port "5433, excluding database matching the regexps "idelone" and "(?i:sleep)":

  check_pgactivity -p 5433 -h slave --service hit_ratio --dbexclude idelone --dbexclude "(?i:sleep)" -w 90% -c 80%

=item Execute service "hit_ratio" on host "slave" port "5433, only for databases matching the regexp "importantone":

  check_pgactivity -p 5433 -h slave --service hit_ratio --dbinclude importantone -w 90% -c 80%

=back

=head1 VERSION

check_pgactivity version 2.5, released on Tue Nov 24 2020

=head1 LICENSING

This program is open source, licensed under the PostgreSQL license.
For license terms, see the LICENSE provided with the sources.

=head1 AUTHORS

S<Author: Open PostgreSQL Monitoring Development Group>
S<Copyright: (C) 2012-2020 Open PostgreSQL Monitoring Development Group>

=cut
