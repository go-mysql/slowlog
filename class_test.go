// Copyright 2019 Daniel Nichter

package slowlog_test

import (
	"testing"

	"github.com/go-mysql/slowlog"
	"github.com/go-test/deep"
)

func TestAggregateClass(t *testing.T) {
	c1 := &slowlog.Class{
		Id:            "111",
		Fingerprint:   "select *",
		TotalQueries:  5,
		UniqueQueries: 1,
		Metrics: slowlog.Metrics{
			TimeMetrics: map[string]*slowlog.TimeStats{
				"Query_time": {Sum: 1.123, Min: 0.100, Avg: 0.2, Med: 0.155, P95: 0.101, Max: 3.222},
			},
			NumberMetrics: map[string]*slowlog.NumberStats{
				"Rows_sent": {Sum: 90, Min: 4, Avg: 18, Med: 7, P95: 8, Max: 10},
			},
			BoolMetrics: map[string]*slowlog.BoolStats{
				"Full_scan": {Sum: 10},
			},
		},
	}
	c2 := &slowlog.Class{
		Id:            "222",
		Fingerprint:   "insert t",
		TotalQueries:  4,
		UniqueQueries: 1,
		Metrics: slowlog.Metrics{
			TimeMetrics: map[string]*slowlog.TimeStats{
				"Query_time": {Sum: 1.123, Min: 0.111, Avg: 0.2, Med: 5.555, P95: 9.999, Max: 5.222},
			},
			NumberMetrics: map[string]*slowlog.NumberStats{
				"Rows_sent": {Sum: 100, Min: 0, Avg: 25, Med: 7, P95: 8, Max: 11},
			},
			BoolMetrics: map[string]*slowlog.BoolStats{
				"Full_scan": {Sum: 10},
			},
		},
	}

	expect := &slowlog.Class{
		Id:            "anId",
		Fingerprint:   "aFingerprint",
		TotalQueries:  9,
		UniqueQueries: 2,
		Metrics: slowlog.Metrics{
			TimeMetrics: map[string]*slowlog.TimeStats{
				"Query_time": {Sum: 2.246, Min: 0.100, Avg: 0.2495555, Med: 0.155, P95: 0.101, Max: 5.222},
			},
			NumberMetrics: map[string]*slowlog.NumberStats{
				"Rows_sent": {Sum: 190, Min: 0, Avg: 21, Med: 7, P95: 8, Max: 11},
			},
			BoolMetrics: map[string]*slowlog.BoolStats{
				"Full_scan": {Sum: 20},
			},
		},
	}

	got := slowlog.NewAggregateClass("anId", "aFingerprint", []*slowlog.Class{c1, c2})

	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}
