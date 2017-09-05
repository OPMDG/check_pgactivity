# Contributing to check_pgactivity

## Adding a new service

### Get check_pgactivity know about your new service

At the beginning of the check_pgactivity, the %services hash describes every
available services.

The hash is defined this way :
```
my %services = (
    # 'service_name' => {
    #    'sub'     => sub reference to call to run this service
    #    'desc'    => 'a desctiption of the service'
    # }

    'autovacuum' => {
        'sub'  => \&check_autovacuum,
        'desc' => 'Check the autovacuum activity.'
    },
...
    'example' => {
        'sub'  => \&check_example,
        'desc' => 'Check number of connections (example service).'
    }
```

First, add the service_name and values for the sub and desc entries.

That is enough to declare a new service. You can now see your new service
listed when you call check_pgactivity with the --list argument.


### Implement your service

Now, define a new check_servicename function before the mark "End of SERVICE
section in pod doc".

To know about arguments provided to the service support function, see the %args
hash.

Get some inspiration from other service functions to see how to handle an new
one, for example check_stat_snapshot_age is a good starter as it is really
simple. Here are however some guidelines.

Your service has to identify itself with a variable $me. This variable will be
used when calling the output functions. For example :
```
sub check_example {
    my $me       = 'POSTGRES_EXAMPLE';
```

Several other variables are defined :

* `@rs`: array to store the result set of the monitoring query
* `@perfdata`: array to store the returned perdata
* `@msg`: array to store the returned messages
* `@hosts`: array to know the host(s) to query
* `%args`: hash containing service arguments

Consider using these variables names as a convention.

Also populate your own %args hash from the first argument :
```
    my %args     = %{ $_[0] };
```

Now the interesting stuff comes. You can declare a simple query to monitor
something in your PostgreSQL server. Here we declare a query that may work with
any PostgreSQL version, for example :
```
my $query  = qq{ SELECT count(*) FROM pg_stat_activity};
```

If you must provide multiple queries for mulitple PostgreSQL versions, please
refer to the section "Supporting multiple PostgreSQL versions".

If your service has to react according some user-thresholds, verify that the
user has given the thresholds as arguments :
```
defined $args{'warning'} and defined $args{'critical'}
```

Use pod2usage to output the error message.

It is recommended to validate the format of the threshold using a regexp :
```
$args{'warning'}  =~ m/^([0-9.]+)/
```

The parse_hosts function will populate the @hosts array :
```
@hosts = @{ parse_hosts %args };
```

If your service does not work until a given PostgreSQL version, use some code
like :
```
is_compat $hosts[0], 'example', $PG_VERSION_95 or exit 1;
```

Query the database using the query function :
```
@rs = @{ query ( $hosts[0], $query ) };
```

In our example, we can directly get the result :
```
$num_connections = $rs[0][0];
```

Populate the @perfdata array with the resulting metrics :
```
push @perfdata => [ "connections_number", $num_connections, undef ];
```

You must provide some mandatory data in the @perfdata array :

* perfdata name
* the data itself, in numerical form
* the unit used:'B' for bytes, 's' for secondes or undef for raw numbers

The following data are optional :

* warning threshold
* critical threshold
* minimum value
* maximum value

Your service can return "ok" by calling the function of the same name :
```
return ok( $me, \@msg, \@perfdata );
```

check_pgactivity provides 4 functions for the 4 given A Nagios-compatible
service states :

* ok
* warning
* critical
* unknown


### Document your new service

check_pgactivity's documentation is handled in its source file, in POD format,
as Plain Old Documentation format. Refer to the perlpod documentation for
further informations.

See also the releasing.md file to see how to regenerate the documentation.


### Test your new service

Test your service in several conditions, verify that it returns a warning or
critical alert by simulating each conditions.

Test your service upon several PostgreSQL versions. Verify that the service
returns an error for unsupported versions, and that every other versions work
well.


### Submit your patch

The best way to submit your patch is to send a pull request in github.


## Supporting multiple PostgreSQL versions

Each major PostgreSQL version brings some incompatibilities from previous
releases. You can easily add compatibility to a new PostgreSQL version by
following some guidelines given here.

First, you have to add a new $PG_VERSION_XXX variable, as following :
```
my $PG_VERSION_MIN =  70400;
my $PG_VERSION_74  =  70400;
...
my $PG_VERSION_95  =  90500;
my $PG_VERSION_96  =  90600;
my $PG_VERSION_100 = 100000;
```

The value of the variable is given from the parameter server_version_num. You
can look at the function set_pgversion() and is_compat() to see how it is used.


Then, you will probably have to adapt some queries for the new PostgreSQL
version. In order to ease this process, most probes provides a %queries hash
that stores a given query associated to a given PostgreSQL version. You don't
have to write the same query for each major release, you can simply store the
appropriate query for the version that enters the incompatibility, it will be
used for each following version.

For example, the probe autovacuum, implemeted in the check_autovacuum function
provides the following hash :
```
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
``` 

Then, call the query_ver() function, giving the host you want to query, usually
$host[0], and the %queries hash :
```
@rs = @{ query_ver( $hosts[0], %queries ) };
```

query_ver() will do the job for you and use the appropriate query for the
current PostgreSQL release and return the result in the @rs array.

Also, if a probe does not work until a given version, use a derivate of the
following code :
```
is_compat $hosts[0], 'autovacuum', $PG_VERSION_81 or exit 1;
```

See the code around to look how to support new PostgreSQL versions. Sometimes
you have to write a totally different code path to support a new release, as it
happened for PostgreSQL 10. See for example check_archiver how it is handled.


## Storing statistics

One of the key feature of check_pgactivity consists of the ability to store
intermediate results in a binary file. That allows to calculate delta values
between two calls of the same service.

The underlying implementation uses the Storable library from the Perl language.
Thus, you can easily store any Perl data structure into the resulting
statistics file.

First, the load call will populate the data structure using the following
arguments :

* the host structure ref that holds the "host" and "port" parameters
* the name of the structure to load
* the path to the file storage

As you may guess, there's also a save function to store the data structure into
the statistics file with the following arguments :

* the host structure ref that holds the "host" and "port" parameters
* the name of the structure to save
* the ref of the structure to save
* the path to the file storage

See for example the function check_bgwriter to see how to use the functions and
how to store your intermediate metrics.


## Debugging check_pgactivity

Use the --debug option to enable the debug output.

Use dprint() function to output some specific debugging messages to the
developer or the user.


