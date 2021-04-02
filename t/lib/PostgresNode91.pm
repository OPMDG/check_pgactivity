# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2021: Open PostgreSQL Monitoring Development Group

package PostgresNode91;

use strict;
use warnings;

use TestLib 'slurp_file';
use Test::More;
use parent 'PostgresNode92';

sub version { return 9.1 }

1;
