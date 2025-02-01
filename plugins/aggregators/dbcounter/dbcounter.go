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

	OutputNameSuffix      string          `toml:"output_name_suffix"`
	ExcludeByLabels       []string        `toml:"exclude_by_labels"`
	LateSeriesGracePeriod config.Duration `toml:"late_series_grace_period"`
	AggregationInterval   config.Duration `toml:"period"`

	excludeByLabelsSet map[string]struct{}
	deltaState         map[uint64]*DeltaAggregate
	sumState           map[uint64]*SumAggregate
}

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
		OutputNameSuffix:      "",
		ExcludeByLabels:       make([]string, 0),
		LateSeriesGracePeriod: config.Duration(5 * time.Minute),
		AggregationInterval:   config.Duration(30 * time.Second),
	}
	a.Reset()
	return a
}

func (a *Aggregator) Init() error {
	if a.ExcludeByLabels == nil || len(a.ExcludeByLabels) == 0 {
		return fmt.Errorf("exclude_by_labels must be set and non-empty")
	} else {
		a.excludeByLabelsSet = make(map[string]struct{})
		for _, label := range a.ExcludeByLabels {
			a.excludeByLabelsSet[label] = struct{}{}
		}
	}
	a.deltaState = make(map[uint64]*DeltaAggregate)
	a.sumState = make(map[uint64]*SumAggregate)
	a.Log.Debug("dbcounter aggregator inited")
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
			Tags:             cloneTags(origTags, nil),
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

	newTags := cloneTags(origTags, a.excludeByLabelsSet)
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
		nameWithSuffix := sumAgg.Name + a.OutputNameSuffix
		if sumAgg.FirstSeen {
			acc.AddFields(nameWithSuffix, map[string]interface{}{"value": float64(0)}, sumAgg.Tags, now.Add(-time.Duration(a.AggregationInterval)/2))
		}
		acc.AddFields(nameWithSuffix, map[string]interface{}{"value": sumAgg.Value}, sumAgg.Tags, now)
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

func cloneTags(in map[string]string, drop map[string]struct{}) map[string]string {
	out := make(map[string]string, len(in))
	if drop == nil {
		for k, v := range in {
			out[k] = v
		}
	} else {
		for k, v := range in {
			if _, ok := drop[k]; ok {
				continue
			}
			out[k] = v
		}
	}
	return out
}

func init() {
	aggregators.Add("dbcounter", func() telegraf.Aggregator {
		return NewAggregator()
	})
}
