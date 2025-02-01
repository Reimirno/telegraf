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

const (
	startOfTheWorld = 1530939936
	excludeByLabel  = "dbletNode"
	testMeasurement = "m1"
)

func TestSimple(t *testing.T) {
	testCases := []testCase{
		{
			name:        "simple counter increment",
			description: "arrival of 2 and 3 each brings a delta of 1, making a total of 2",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(1)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(1)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(3)},
							timeFromOffset(2)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(0)),
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
				agg.Reset()
			}
		})
	}
}

func TestMultipleWindows(t *testing.T) {
	testCases := []testCase{
		{
			name:        "multiple windows",
			description: "should calculate deltas across multiple windows",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(1)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(3)},
							timeFromOffset(1)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(0)),
					},
				},
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(7)},
							timeFromOffset(2)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(15)},
							timeFromOffset(3)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(14)},
							timeFromOffset(2)),
					},
				},
			},
		},
		{
			name:        "multiple windows multiple nodes",
			description: "should calculate deltas across multiple windows with multiple nodes",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(1)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(3)},
							timeFromOffset(1)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(4)},
							timeFromOffset(1)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(4)},
							timeFromOffset(0)),
					},
				},
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(7)},
							timeFromOffset(2)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(8)},
							timeFromOffset(2)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(9)},
							timeFromOffset(3)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(10)},
							timeFromOffset(3)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(16)},
							timeFromOffset(2)),
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
				agg.Reset()
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
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(999)},
							timeFromOffset(0)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							time.Unix(0, 0)),
						metric.New(testMeasurement,
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
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(9)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(99)},
							timeFromOffset(1)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node3"},
							map[string]interface{}{"value": float64(999)},
							timeFromOffset(2)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
					},
				},
			},
		},
		{
			name:        "real counter reset",
			description: "should inject a 0",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(99)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(100)},
							timeFromOffset(1)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(3)},
							timeFromOffset(2)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(5)},
							timeFromOffset(3)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64((100 - 99) + (5 - 3))},
							timeFromOffset(0)),
					},
				},
			},
		},
		{
			name:        "real counter reset across two windows",
			description: "should not do negative delta",
			windows: []testAggregationWindow{
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(99)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(100)},
							timeFromOffset(1)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(100 - 99)},
							timeFromOffset(0)),
					},
				},
				{
					metricsInput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(101)},
							timeFromOffset(2)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(4)},
							timeFromOffset(3)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(5)},
							timeFromOffset(4)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64((100 - 99) + (101 - 100) + 0 + (5 - 4))},
							timeFromOffset(0)),
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
				agg.Reset()
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
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(1)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node1"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(3)},
							timeFromOffset(1)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar", excludeByLabel: "node2"},
							map[string]interface{}{"value": float64(4)},
							timeFromOffset(2)),
					},
					expectedMetricsOutput: []telegraf.Metric{
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(0)},
							timeFromOffset(0)),
						metric.New(testMeasurement,
							map[string]string{"foo": "bar"},
							map[string]interface{}{"value": float64(2)},
							timeFromOffset(0)),
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
				agg.Reset()
			}
		})
	}
}

func newAggregator(t *testing.T) *Aggregator {
	agg := NewAggregator()
	agg.GroupWithoutLabels = []string{excludeByLabel}
	agg.Log = testutil.Logger{}

	require.NoError(t, agg.Init())
	return agg
}

func check(t *testing.T, acc *testutil.Accumulator, expectedMetrics []telegraf.Metric) {
	// Limitation: Not checking the order of the metrics and not checking the timestamp of the metrics
	// But since we do a check per window, it's probably sufficient
	for _, expectedMetric := range expectedMetrics {
		acc.AssertContainsTaggedFields(
			t,
			expectedMetric.Name(),
			expectedMetric.Fields(),
			expectedMetric.Tags(),
		)
	}
}

func timeFromOffset(offset int64) time.Time {
	return time.Unix(startOfTheWorld+offset, 0)
}
