# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode12;

use strict;
use warnings;

use parent 'PostgresNode13';

sub version { return 12 }

1;
