/*
	Copyright 2017 Daniel Nichter
*/

package slowlog_test

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql/slowlog"
)

var noOptions = slowlog.Options{}

func dump(v interface{}) {
	bytes, _ := json.MarshalIndent(v, "", " ")
	fmt.Println(string(bytes))
}
