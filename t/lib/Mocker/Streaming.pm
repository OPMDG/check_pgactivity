# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

package Streaming;

use strict;
use warnings;

# This module is a simple wrapper around the "query" sub existing in
# check_pgactivity script. Its purpose is to capture and edit query results
# to test some part of the streaming_delta check.
#
# You must load it on check_pgactivity execution using eg.:
#   perl -It/ -MMocker::Streaming check_pgactivity --service streaming_delta

CHECK {
    # keep reference to old query sub
    $main::{'query_orig'} = $main::{'query'};

    # FIXME: check given query

    # install wrapper around query sub to capture and modify the result
    $main::{'query'} = sub {
        my $res;

        $res = $main::{'query_orig'}->(@_);

        return $res unless $_[1] =~ m/FROM pg_stat_replication/;

        # mock 1MB of write delta.
        #      2MB of flush delta.
        #      3MB of replay delta.
        # We don't mind the total WAL size (the X part of XXX/YYYYY) as the
        # test stay far below it.
        $res->[0][7] =~ m{^0/([0-9A-F]+)$};

        $res->[0][4] = sprintf('0/%X', hex($1) - 1048576);
        $res->[0][5] = sprintf('0/%X', hex($1) - 2097152);
        $res->[0][6] = sprintf('0/%X', hex($1) - 3145728);

        return $res;
    };
}

1
