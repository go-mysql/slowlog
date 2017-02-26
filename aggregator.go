/*
	Copyright 2017 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

package slowlog

import (
	"time"
)

// A Result contains a global class and per-ID classes with finalized metric
// statistics. The classes are keyed on class ID.
type Result struct {
	Global    *Class            // all classes
	Class     map[string]*Class // keyed on class ID
	RateLimit uint
	Error     string
}

// An Aggregator groups events by class ID. When there are no more events,
// a call to Finalize computes all metric statistics and returns a Result.
type Aggregator struct {
	samples     bool
	utcOffset   time.Duration
	outlierTime float64
	// --
	global    *Class
	classes   map[string]*Class
	rateLimit uint
}

// NewAggregator returns a new Aggregator.
func NewAggregator(samples bool, utcOffset time.Duration, outlierTime float64) *Aggregator {
	a := &Aggregator{
		samples:     samples,
		utcOffset:   utcOffset,
		outlierTime: outlierTime,
		// --
		global:  NewClass("", "", false),
		classes: map[string]*Class{},
	}
	return a
}

// AddEvent adds the event to the aggregator, automatically creating new classes
// as needed.
func (a *Aggregator) AddEvent(event Event, id, fingerprint string) {
	if a.rateLimit != event.RateLimit {
		a.rateLimit = event.RateLimit
	}

	outlier := false
	if a.outlierTime > 0 && event.TimeMetrics["Query_time"] > a.outlierTime {
		outlier = true
	}

	a.global.AddEvent(event, outlier)

	class, ok := a.classes[id]
	if !ok {
		class = NewClass(id, fingerprint, a.samples)
		a.classes[id] = class
	}
	class.AddEvent(event, outlier)
}

// Finalize calculates all metric statistics and returns a Result.
// Call this function when done adding events to the aggregator.
func (a *Aggregator) Finalize() Result {
	a.global.Finalize(a.rateLimit)
	a.global.UniqueQueries = uint(len(a.classes))
	for _, class := range a.classes {
		class.Finalize(a.rateLimit)
		class.UniqueQueries = 1
		if class.Example != nil && class.Example.Ts != "" {
			if t, err := time.Parse("060102 15:04:05", class.Example.Ts); err != nil {
				class.Example.Ts = ""
			} else {
				class.Example.Ts = t.Add(a.utcOffset).Format("2006-01-02 15:04:05")
			}
		}
	}
	return Result{
		Global:    a.global,
		Class:     a.classes,
		RateLimit: a.rateLimit,
	}
}
