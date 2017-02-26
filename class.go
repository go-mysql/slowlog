/*
	Copyright 2017 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

package slowlog

const (
	MAX_EXAMPLE_BYTES = 1024 * 10
)

// A Class represents all events with the same fingerprint and class ID.
// This is only enforced by convention, so be careful not to mix events from
// different classes.
type Class struct {
	Id            string   // 32-character hex checksum of fingerprint
	Fingerprint   string   // canonical form of query: values replaced with "?"
	Metrics       Metrics  // statistics for each metric, e.g. max Query_time
	TotalQueries  uint     // total number of queries in class
	UniqueQueries uint     // unique number of queries in class
	Example       *Example `json:",omitempty"` // sample query with max Query_time
	// --
	outliers uint
	lastDb   string
	sample   bool
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
		Id:           id,
		Fingerprint:  fingerprint,
		Metrics:      NewMetrics(),
		TotalQueries: 0,
		Example:      &Example{},
		sample:       sample,
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
}

// Finalize calculates all metric statistics. Call this function when done
// adding events to the class.
func (c *Class) Finalize(rateLimit uint) {
	if rateLimit == 0 {
		rateLimit = 1
	}
	c.Metrics.Finalize(rateLimit)
	c.TotalQueries = (c.TotalQueries * rateLimit) + c.outliers
	if c.Example.QueryTime == 0 {
		c.Example = nil
	}
}
