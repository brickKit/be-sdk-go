package besdk

import (
	"reflect"
	"testing"
)

func TestParseArgs(t *testing.T) {
	cases := []struct {
		args []string
		want command
		ok   bool
	}{
		{nil, command{kind: cmdServe}, true},
		{[]string{"migrate", "up"}, command{kind: cmdMigrateUp}, true},
		{[]string{"migrate", "status"}, command{kind: cmdMigrateStatus}, true},
		{[]string{"migrate", "down", "2"}, command{kind: cmdMigrateDown, n: 2}, true},
		{[]string{"job", "run", "widget.daily"}, command{kind: cmdJobRun, job: "widget.daily"}, true},
		{[]string{"migrate"}, command{}, false},
		{[]string{"migrate", "up", "extra"}, command{}, false},
		{[]string{"migrate", "down"}, command{}, false},
		{[]string{"migrate", "down", "0"}, command{}, false},
		{[]string{"migrate", "down", "-1"}, command{}, false},
		{[]string{"migrate", "down", "x"}, command{}, false},
		{[]string{"migrate", "upp"}, command{}, false},
		{[]string{"migrations", "up"}, command{}, false},
		{[]string{"job", "run"}, command{}, false},
		{[]string{"job", "run", "a", "b"}, command{}, false},
		{[]string{"job", "list"}, command{}, false},
		{[]string{"--help"}, command{}, false},
		{[]string{"serve"}, command{}, false},
	}
	for _, c := range cases {
		got, err := parseArgs(c.args)
		if c.ok != (err == nil) {
			t.Fatalf("%v: ok=%v err=%v", c.args, c.ok, err)
		}
		if c.ok && !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%v: got %+v want %+v", c.args, got, c.want)
		}
	}
}
