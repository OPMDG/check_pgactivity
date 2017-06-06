# check_pgactivity

check\_pgactivity is a Nagios-compatible checker to monitor every key features
of a PostgreSQL cluster :

* number of sessions, longuest queries, locked sessions, etc
* database size, bloated tables and bloated indexes
* WAL files, archiver state, database dumps
* streaming replication, replication slots
* and many more

check\_pgactivity supports several output formats :

* Nagios, strict or not
* Human-readable
* binary (Perl-compatible)


## Downloads

All releases can by downloaded from github : https://github.com/OPMDG/check_pgactivity/releases


## Usage

check\_pgactivity is primarily aimed to be used with a Nagios-compatible
monitoring system.


## Contributing

The project page is hosted on github: https://github.com/OPMDG/check_pgactivity

Feel free to open issues and submit patches. We used the Perl language for the
code, so some basic knowledge of Perl is warmly welcomed to contribute.


## Origins

The OPMDG was initially formed by Dalibo to support the development of our OPM
monitoring suite. The OPMDG is an informal group of people contributing to OPM and
related tools, and is independant from the company in order to encourage other
contributors to submit patches.

We initially thought about using check\_postgres for the OPM monitoring suite,
but it lacked some crucial performance datas and the base code was difficult to
maintain. We decided to write our own Nagios checker from scratch, in a more
maintainable manner and with a focus on a rich perfdata set.

Thus, it's now very easy to extend check\_pactivity to support new services or
simply support a new PostgreSQL release. The output format is automatically
treated by check\_pgactivity, a service just has to return a some variables.



