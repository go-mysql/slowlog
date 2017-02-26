/*
	Copyright 2017 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

package slowlog_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/daniel-nichter/deep-equal"
	"github.com/go-mysql/query"
	"github.com/go-mysql/slowlog"
)

var examples = true

func aggregateSlowLog(t *testing.T, input, output string, utcOffset time.Duration) (got slowlog.Result, expect slowlog.Result) {
	bytes, err := ioutil.ReadFile(path.Join("test", "results", output))
	if err != nil {
		t.Fatal(err)
	}
	expect = slowlog.Result{}
	if err := json.Unmarshal(bytes, &expect); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path.Join("test", "slow-logs", input))
	if err != nil {
		t.Fatal(err)
	}
	p := slowlog.NewFileParser(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(slowlog.Options{}); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	a := slowlog.NewAggregator(examples, utcOffset, 10)
	for e := range p.Events() {
		f := query.Fingerprint(e.Query)
		id := query.Id(f)
		a.AddEvent(e, id, f)
	}
	got = a.Finalize()
	return got, expect
}

func zeroPercentiles(r *slowlog.Result) {
	for _, metrics := range r.Global.Metrics.TimeMetrics {
		metrics.Med = 0
		metrics.P95 = 0
	}
	for _, metrics := range r.Global.Metrics.NumberMetrics {
		metrics.Med = 0
		metrics.P95 = 0
	}
	for _, class := range r.Class {
		for _, metrics := range class.Metrics.TimeMetrics {
			metrics.Med = 0
			metrics.P95 = 0
		}
		for _, metrics := range class.Metrics.NumberMetrics {
			metrics.Med = 0
			metrics.P95 = 0
		}
	}
}

// --------------------------------------------------------------------------

func TestSlow001(t *testing.T) {
	got, expect := aggregateSlowLog(t, "slow001.log", "slow001.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

func TestSlow001WithTzOffset(t *testing.T) {
	got, expect := aggregateSlowLog(t, "slow001.log", "slow001.json", -1*time.Hour)
	// Use the same files as TestSlow001NoExamples but with a tz=-1
	expect.Class["7F7D57ACDD8A346E"].Example.Ts = "2007-10-15 20:43:52"
	expect.Class["3A99CC42AEDCCFCD"].Example.Ts = "2007-10-15 20:45:10"
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

func TestSlow001NoExamples(t *testing.T) {
	examples = false
	defer func() { examples = true }()
	got, expect := aggregateSlowLog(t, "slow001.log", "slow001-no-examples.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

// Test p95 and median.
func TestSlow010(t *testing.T) {
	got, expect := aggregateSlowLog(t, "slow010.log", "slow010.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

func TestSlow018(t *testing.T) {
	got, expect := aggregateSlowLog(t, "slow018.log", "slow018.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

// Tests for PCT-1006 & PCT-1085
func TestUseDb(t *testing.T) {
	// Test db is not inherited
	got, expect := aggregateSlowLog(t, "slow020.log", "slow020.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
	// Test "use" is not case sensitive
	got, expect = aggregateSlowLog(t, "slow021.log", "slow021.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
	// Test we are parsing db names in backticks
	got, expect = aggregateSlowLog(t, "slow022.log", "slow022.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}

func TestOutlierSlow025(t *testing.T) {
	got, expect := aggregateSlowLog(t, "slow025.log", "slow025.json", 0)
	if diff, _ := deep.Equal(got, expect); diff != nil {
		dump(got)
		t.Error(diff)
	}
}
