//go:build !custom || aggregators || aggregators.dbcounter

package all

import _ "github.com/influxdata/telegraf/plugins/aggregators/dbcounter" // register plugin
