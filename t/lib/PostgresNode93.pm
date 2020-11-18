# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode93;

use strict;
use warnings;

use Test::More;
use parent 'PostgresNode94';

sub version       { return 9.3 }
sub can_slots     { return 0 }
sub can_log_hints { return 0 }

1;
