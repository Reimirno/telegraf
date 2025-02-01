package dbcounter

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	acc := testutil.Accumulator{}
	agg := newAggregator(t)

	m1 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node1"},
		map[string]interface{}{"value": float64(1)},
		time.Unix(1530939936, 0))
	m2 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node1"},
		map[string]interface{}{"value": float64(2)},
		time.Unix(1530939937, 0))
	m3 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node1"},
		map[string]interface{}{"value": float64(3)},
		time.Unix(1530939938, 0))
	agg.Add(m1)
	agg.Add(m2)
	agg.Add(m3)
	agg.Push(&acc)

	expectedName := "m1_dbcounter"
	expectedTags := map[string]string{
		"foo": "bar",
	}
	expectedValues := []map[string]interface{}{
		{
			"value": float64(0),
		},
		{
			"value": float64(2),
		},
	}
	for _, expectValue := range expectedValues {
		acc.AssertContainsTaggedFields(
			t,
			expectedName,
			expectValue,
			expectedTags,
		)
	}
}

func TestDeltaHandling(t *testing.T) {
	acc := testutil.Accumulator{}
	agg := newAggregator(t)

	m1 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node1"},
		map[string]interface{}{"value": float64(99)},
		time.Unix(1530939936, 0))
	agg.Add(m1)
	agg.Push(&acc)

	expectedName := "m1_dbcounter"
	expectedTags := map[string]string{
		"foo": "bar",
	}
	expectedValues := []map[string]interface{}{
		{
			"value": float64(0),
		},
		{
			"value": float64(0),
		},
	}
	for _, expectValue := range expectedValues {
		acc.AssertContainsTaggedFields(
			t,
			expectedName,
			expectValue,
			expectedTags,
		)
	}
}

func TestLabelDropping(t *testing.T) {
	acc := testutil.Accumulator{}
	agg := newAggregator(t)

	m1 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node1"},
		map[string]interface{}{"value": float64(1)},
		time.Unix(1530939936, 0))
	m2 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node2"},
		map[string]interface{}{"value": float64(2)},
		time.Unix(1530939937, 0))
	m3 := metric.New("m1",
		map[string]string{"foo": "bar", "dbletNode": "node2"},
		map[string]interface{}{"value": float64(3)},
		time.Unix(1530939938, 0))
	agg.Add(m1)
	agg.Add(m2)
	agg.Add(m3)
	agg.Push(&acc)

	expectedName := "m1_dbcounter"
	expectedTags := map[string]string{
		"foo": "bar",
	}
	expectedValues := []map[string]interface{}{
		{
			"value": float64(0),
		},
		{
			"value": float64(1),
		},
	}
	for _, expectValue := range expectedValues {
		acc.AssertContainsTaggedFields(
			t,
			expectedName,
			expectValue,
			expectedTags,
		)
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
