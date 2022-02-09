#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use File::Find;
use Test::More;

# Try to catch all copyright mentions in source code and
# fail if the second part of the year is bad.

my @filelist;
my $year = (gmtime)[5] + 1900;

# Build list of readable files
find(
    sub {
        # ignore root
        return if m/^\.+$/;
        # ignore hidden folders
        $File::Find::prune = 1 if -d $File::Find::name and m/^\./;
        push @filelist, $File::Find::name unless m/^\./;
    },
    '.'
);

### Begin tests ###

foreach my $f (@filelist) {
    open my $fh, '<', $f;

    while (<$fh>) {
        if ( m/(copyright.*?\d+\s*-\s*(\d+).*Open PostgreSQL Monitoring Development Group.*)$/i ) {
            is($2, $year, "up to date copyright year in $f:$.")
                or diag("The copyright mention is: $1");
        }
    }

    close $fh;
}

### End of tests ###

done_testing;
