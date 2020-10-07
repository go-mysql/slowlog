/*
	Copyright 2017 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

package slowlog

// An Event is a query like "SELECT col FROM t WHERE id = 1", some metrics like
// Query_time (slow log) or SUM_TIMER_WAIT (Performance Schema), and other
// metadata like default database, timestamp, etc. Metrics and metadata are not
// guaranteed to be defined--and frequently they are not--but at minimum an
// event is expected to define the query and Query_time metric. Other metrics
// and metadata vary according to MySQL version, distro, and configuration.
type Event struct {
	Offset          uint64 // byte offset in file at which event starts
	Ts              string // raw timestamp of event
	Admin           bool   // true if Query is admin command
	Query           string // SQL query or admin command
	User            string
	Host            string
	Db              string
	TimeMetrics     map[string]float64 // *_time and *_wait metrics
	NumberMetrics   map[string]uint64  // most metrics
	BoolMetrics     map[string]bool    // yes/no metrics
	RateType        string             // Percona Server rate limit type
	RateLimit       uint               // Percona Server rate limit value
	CommentMetadata map[string]string
}

// NewEvent returns a new Event with initialized metric maps.
func NewEvent() *Event {
	return &Event{
		TimeMetrics:   map[string]float64{},
		NumberMetrics: map[string]uint64{},
		BoolMetrics:   map[string]bool{},
	}
}
