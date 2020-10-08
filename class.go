/*
	Copyright 2017, 2019 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

package slowlog

const (
	// MAX_EXAMPLE_BYTES defines the maximum Example.Query size.
	MAX_EXAMPLE_BYTES = 1024 * 10
)

// A Class represents all events with the same fingerprint and class ID.
// This is only enforced by convention, so be careful not to mix events from
// different classes.
type Class struct {
	Id            string   // 32-character hex checksum of fingerprint
	Fingerprint   string   // canonical form of query: values replaced with "?"
	Metrics       Metrics  // statistics for each metric, e.g. max Query_time
	TotalQueries  uint64   // total number of queries in class
	UniqueQueries uint     // unique number of queries in class
	Example       *Example `json:",omitempty"` // sample query with max Query_time
	// --
	outliers                uint64
	lastDb                  string
	sample                  bool
	maxQueryTime            float64
	MaxQueryCommentMetadata map[string]string
}

// A Example is a real query and its database, timestamp, and Query_time.
// If the query is larger than MAX_EXAMPLE_BYTES, it is truncated and "..."
// is appended.
type Example struct {
	QueryTime float64 // Query_time
	Db        string  // Schema: <db> or USE <db>
	Query     string  // truncated to MAX_EXAMPLE_BYTES
	Ts        string  `json:",omitempty"` // in MySQL time zone
}

// NewClass returns a new Class for the class ID and fingerprint.
// If sample is true, the query with the greatest Query_time is saved.
func NewClass(id, fingerprint string, sample bool) *Class {
	return &Class{
		Id:                      id,
		Fingerprint:             fingerprint,
		Metrics:                 NewMetrics(),
		TotalQueries:            0,
		Example:                 &Example{},
		sample:                  sample,
		maxQueryTime:            0,
		MaxQueryCommentMetadata: map[string]string{},
	}
}

// AddEvent adds an event to the query class.
func (c *Class) AddEvent(e Event, outlier bool) {
	if outlier {
		c.outliers++
	} else {
		c.TotalQueries++
	}

	c.Metrics.AddEvent(e, outlier)

	// Save last db seen for this query. This helps ensure the sample query
	// has a db.
	if e.Db != "" {
		c.lastDb = e.Db
	}
	if c.sample {
		if n, ok := e.TimeMetrics["Query_time"]; ok {
			if float64(n) > c.Example.QueryTime {
				c.Example.QueryTime = float64(n)
				if e.Db != "" {
					c.Example.Db = e.Db
				} else {
					c.Example.Db = c.lastDb
				}
				if len(e.Query) > MAX_EXAMPLE_BYTES {
					c.Example.Query = e.Query[0:MAX_EXAMPLE_BYTES-3] + "..."
				} else {
					c.Example.Query = e.Query
				}
				c.Example.Ts = e.Ts
			}
		}
	}

	if queryTime, ok := e.TimeMetrics["Query_time"]; ok {
		if queryTime > c.maxQueryTime {
			c.MaxQueryCommentMetadata = e.CommentMetadata
		}
	}

}

// Finalize calculates all metric statistics. Call this function when done
// adding events to the class.
func (c *Class) Finalize(rateLimit uint) {
	if rateLimit == 0 {
		rateLimit = 1
	}
	c.Metrics.Finalize(rateLimit)
	c.TotalQueries = (c.TotalQueries * uint64(rateLimit)) + c.outliers
	if c.Example.QueryTime == 0 {
		c.Example = nil
	}
}

// NewAggregateClass makes a new Class from the given member classes.
func NewAggregateClass(id, fingerprint string, members []*Class) *Class {
	aggClass := &Class{
		Id:            id,
		Fingerprint:   fingerprint,
		Metrics:       NewMetrics(),
		UniqueQueries: uint(len(members)),
		TotalQueries:  0,
	}

	for _, memberClass := range members {
		aggClass.TotalQueries += memberClass.TotalQueries

		for newMetric, newStats := range memberClass.Metrics.TimeMetrics {
			stats, ok := aggClass.Metrics.TimeMetrics[newMetric]
			if !ok {
				m := *newStats
				aggClass.Metrics.TimeMetrics[newMetric] = &m
			} else {
				stats.Sum += newStats.Sum
				stats.Avg = stats.Sum / float64(aggClass.TotalQueries)
				if newStats.Min < stats.Min {
					stats.Min = newStats.Min
				}
				if newStats.Max > stats.Max {
					stats.Max = newStats.Max
				}
			}
		}

		for newMetric, newStats := range memberClass.Metrics.NumberMetrics {
			stats, ok := aggClass.Metrics.NumberMetrics[newMetric]
			if !ok {
				m := *newStats
				aggClass.Metrics.NumberMetrics[newMetric] = &m
			} else {
				stats.Sum += newStats.Sum
				stats.Avg = stats.Sum / aggClass.TotalQueries
				if newStats.Min < stats.Min {
					stats.Min = newStats.Min
				}
				if newStats.Max > stats.Max {
					stats.Max = newStats.Max
				}
			}
		}

		for newMetric, newStats := range memberClass.Metrics.BoolMetrics {
			stats, ok := aggClass.Metrics.BoolMetrics[newMetric]
			if !ok {
				m := *newStats
				aggClass.Metrics.BoolMetrics[newMetric] = &m
			} else {
				stats.Sum += newStats.Sum
			}
		}
	}

	return aggClass
}
