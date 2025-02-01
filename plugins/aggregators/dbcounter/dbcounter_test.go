package dbcounter

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name        string
	description string
	windows     []testAggregationWindow
}

type testAggregationWindow struct {
	metricsInput          []telegraf.Metric
	expectedMetricsOutput []telegraf.Metric
}

func TestSimple(t *testing.T) {
	testCases := []testCase{
		{
			name:        "simple counter increment",
			description: "arrival of 2 and 3 each brings a delta of 1, making a total of 2",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(1)},
							time.Unix(1530939936, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(2)},
							time.Unix(1530939937, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(3)},
							time.Unix(1530939938, 0)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(2)},
							time.Unix(0, 0)),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			acc := &testutil.Accumulator{}
			agg := newAggregator(t)

			for _, window := range tc.windows {

				for _, m := range window.metricsInput {
					agg.Add(m)
				}
				agg.Push(acc)

				check(t, acc, window.expectedMetricsOutput)
			}
		})
	}
}

func TestDeltaEdgeCases(t *testing.T) {
	testCases := []testCase{
		{
			name:        "single sample point in a window",
			description: "should return 0, 0",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(999)},
							time.Unix(1530939936, 0)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
					},
				},
			},
		},
		{
			name:        "single sample point in a window, multiple nodes",
			description: "should still return 0, 0",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(9)},
							time.Unix(1530939936, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node2"},
							map[string]interface{}{"value": float64(99)},
							time.Unix(1530939937, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node3"},
							map[string]interface{}{"value": float64(999)},
							time.Unix(1530939938, 0)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			acc := &testutil.Accumulator{}
			agg := newAggregator(t)

			for _, window := range tc.windows {
				for _, m := range window.metricsInput {
					agg.Add(m)
				}
				agg.Push(acc)

				check(t, acc, window.expectedMetricsOutput)
			}
		})
	}
}

func TestLabelDropping(t *testing.T) {
	testCases := []testCase{
		{
			name:        "label dropping for dbletNode",
			description: "output should not have dbletNode label and should combine delta from all nodes",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(1)},
							time.Unix(1530939936, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node1"},
							map[string]interface{}{"value": float64(2)},
							time.Unix(1530939936, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node2"},
							map[string]interface{}{"value": float64(3)},
							time.Unix(1530939937, 0)),
						metric.New("m1",
							map[string]string{"foo": "bar", "dbletNode": "node2"},
							map[string]interface{}{"value": float64(4)},
							time.Unix(1530939938, 0)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
						metric.New("m1_dbcounter",
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(2)},
							time.Unix(0, 0)),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			acc := &testutil.Accumulator{}
			agg := newAggregator(t)

			for _, window := range tc.windows {
				for _, m := range window.metricsInput {
					agg.Add(m)
				}
				agg.Push(acc)

				check(t, acc, window.expectedMetricsOutput)
			}
		})
	}
}

func newAggregator(t *testing.T) *Aggregator {
	agg := NewAggregator()
	agg.outputNameSuffix = "_dbcounter"
	agg.excludeByLabels = []string{"dbletNode"}
	agg.Log = testutil.Logger{}

	require.NoError(t, agg.Init())
	return agg
}

func check(t *testing.T, acc *testutil.Accumulator, expectedMetrics []telegraf.Metric) {
	// Not checking the order of the metrics and not checking the timestamp of the metrics
	for _, expectedMetric := range expectedMetrics {
		acc.AssertContainsTaggedFields(
			t,
			expectedMetric.Name(),
			expectedMetric.Fields(),
			expectedMetric.Tags(),
		)
	}
}
