//go:generate ../../../tools/readme_config_includer/generator
package dbcounter

// dbcounter.go

import (
	_ "embed"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

//go:embed sample.conf
var sampleConfig string

type Aggregator struct {
	Log telegraf.Logger `toml:"-"`

	GroupWithoutLabels    []string        `toml:"group_without_labels"`
	GroupByLabels         []string        `toml:"group_by_labels"`
	LateSeriesGracePeriod config.Duration `toml:"late_series_grace_period"`
	AggregationInterval   config.Duration `toml:"period"`

	groupingMode      GroupingMode
	groupingLabelsSet map[string]struct{}
	deltaState        map[uint64]*DeltaAggregate
	sumState          map[uint64]*SumAggregate
}

type GroupingMode int

const (
	None GroupingMode = iota
	GroupBy
	GroupWithout
)

type DeltaAggregate struct {
	Name             string
	Tags             map[string]string
	LastValue        float64
	ThisValue        float64
	SeenInLastWindow bool
	Time             time.Time
}

type SumAggregate struct {
	Name         string
	Tags         map[string]string
	Value        float64
	FirstSeen    bool
	SeenInWindow bool
	Time         time.Time
}

func (*Aggregator) SampleConfig() string {
	return sampleConfig
}

func NewAggregator() *Aggregator {
	a := &Aggregator{
		GroupWithoutLabels:    make([]string, 0),
		LateSeriesGracePeriod: config.Duration(5 * time.Minute),
		AggregationInterval:   config.Duration(30 * time.Second),
	}
	a.Reset()
	return a
}

func (a *Aggregator) Init() error {
	hasGroupWithout := a.GroupWithoutLabels != nil && len(a.GroupWithoutLabels) > 0
	hasGroupBy := a.GroupByLabels != nil && len(a.GroupByLabels) > 0
	a.groupingLabelsSet = make(map[string]struct{})
	if hasGroupWithout && hasGroupBy {
		return fmt.Errorf("at most one of group_without_labels and group_by_labels may be set")
	} else if hasGroupWithout {
		a.groupingMode = GroupWithout
		for _, label := range a.GroupWithoutLabels {
			a.groupingLabelsSet[label] = struct{}{}
		}
	} else if hasGroupBy {
		a.groupingMode = GroupBy
		for _, label := range a.GroupByLabels {
			a.groupingLabelsSet[label] = struct{}{}
		}
	} else {
		a.groupingMode = None
	}

	a.deltaState = make(map[uint64]*DeltaAggregate)
	a.sumState = make(map[uint64]*SumAggregate)

	a.Log.Debug("dbcounter aggregator inited with grouping mode: ", a.groupingMode)
	return nil
}

func (a *Aggregator) Add(metric telegraf.Metric) {
	fieldList := metric.FieldList()
	if fieldList == nil || len(fieldList) != 1 {
		return
	}
	firstField := fieldList[0]
	if firstField.Key != "value" {
		return
	}
	value, ok := convert(firstField.Value)
	if !ok {
		return
	}

	metricName := metric.Name()
	origTags := metric.Tags()
	deltaGroupID := generateGroupID(metricName, origTags)
	deltaAgg, found := a.deltaState[deltaGroupID]
	if !found {
		deltaAgg = &DeltaAggregate{
			Name:             metricName,
			Tags:             cloneTags(origTags, None, nil),
			LastValue:        -1, // counter never negative, we use negative to indicate uninitialized
			ThisValue:        value,
			SeenInLastWindow: true,
			Time:             metric.Time(),
		}
		a.deltaState[deltaGroupID] = deltaAgg
	} else {
		deltaAgg.SeenInLastWindow = true
		deltaAgg.LastValue = deltaAgg.ThisValue
		deltaAgg.ThisValue = value
		deltaAgg.Time = metric.Time()
	}

	delta := computeDelta(deltaAgg.LastValue, deltaAgg.ThisValue)

	newTags := cloneTags(origTags, a.groupingMode, a.groupingLabelsSet)
	sumGroupID := generateGroupID(metricName, newTags)
	sumAgg, found := a.sumState[sumGroupID]
	if !found {
		sumAgg = &SumAggregate{
			Name:         metricName,
			Tags:         newTags,
			Value:        delta,
			FirstSeen:    true,
			SeenInWindow: true,
			Time:         metric.Time(),
		}
		a.sumState[sumGroupID] = sumAgg
	} else {
		sumAgg.SeenInWindow = true
		sumAgg.Value += delta
		sumAgg.Time = metric.Time()
	}
}

func (a *Aggregator) Push(acc telegraf.Accumulator) {
	now := time.Now()
	for _, sumAgg := range a.sumState {
		if !sumAgg.SeenInWindow {
			continue
		}
		if sumAgg.FirstSeen {
			acc.AddFields(sumAgg.Name, map[string]interface{}{"value": float64(0)}, sumAgg.Tags, now.Add(-time.Duration(a.AggregationInterval)/2))
		}
		acc.AddFields(sumAgg.Name, map[string]interface{}{"value": sumAgg.Value}, sumAgg.Tags, now)
	}
}

func (a *Aggregator) Reset() {
	now := time.Now()
	for id, deltaAgg := range a.deltaState {
		if now.Sub(deltaAgg.Time) > time.Duration(a.LateSeriesGracePeriod) {
			delete(a.deltaState, id)
			continue
		}
		deltaAgg.SeenInLastWindow = false
	}

	for id, sumAgg := range a.sumState {
		if now.Sub(sumAgg.Time) > time.Duration(a.LateSeriesGracePeriod) {
			delete(a.sumState, id)
			continue
		}
		sumAgg.SeenInWindow = false
		sumAgg.FirstSeen = false
	}
}

func computeDelta(last, current float64) float64 {
	if last < 0 || current < 0 {
		return 0
	}
	delta := current - last
	if delta < 0 {
		delta = 0
	}
	return delta
}

func generateGroupID(name string, tags map[string]string) uint64 {
	sortedKeys := make([]string, 0, len(tags))
	for k := range tags {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys) // Ensure consistent ordering

	h := fnv.New64a() // Initialize FNV-1a hasher

	// Write never fails for FNV-1a, check source code
	h.Write([]byte(name)) // Hash metric name
	h.Write([]byte(";"))  // Separator for safety
	for _, k := range sortedKeys {
		h.Write([]byte(k))       // Hash tag key
		h.Write([]byte("="))     // Separator for safety
		h.Write([]byte(tags[k])) // Hash tag value
		h.Write([]byte(";"))     // Separator for safety
	}
	return h.Sum64()
}

// Every aggregator has this convert func, we just steal it from other implementations
func convert(in interface{}) (float64, bool) {
	switch v := in.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func cloneTags(in map[string]string, groupingMode GroupingMode, labelSet map[string]struct{}) map[string]string {
	out := make(map[string]string, len(in))
	if groupingMode == None {
		for k, v := range in {
			out[k] = v
		}
	} else if groupingMode == GroupWithout {
		for k, v := range in {
			if _, ok := labelSet[k]; ok {
				continue
			}
			out[k] = v
		}
	} else if groupingMode == GroupBy {
		for k, v := range in {
			if _, ok := labelSet[k]; !ok {
				continue
			}
			out[k] = v
		}
	} else {
		panic("unexpected grouping mode")
	}
	return out
}

func init() {
	aggregators.Add("dbcounter", func() telegraf.Aggregator {
		return NewAggregator()
	})
}
