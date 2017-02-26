/*
	Copyright 2017 Daniel Nichter
*/

package slowlog_test

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql/slowlog"
	"github.com/go-test/deep"
)

var noOptions = slowlog.Options{}

func init() {
	deep.FloatPrecision = 6 // microsecond time
	deep.LogErrors = true
}

func dump(v interface{}) {
	bytes, _ := json.MarshalIndent(v, "", " ")
	fmt.Println(string(bytes))
}
