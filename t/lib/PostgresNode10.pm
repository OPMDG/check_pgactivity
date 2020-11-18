# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode10;

use strict;
use warnings;

use parent 'PostgresNode11';

sub version { return 10 }

1;
