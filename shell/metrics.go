package shell

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// labelled gathers one member's registry and adds component=<member> to every series, so the shell's /metrics
// tells the members apart (protocol 17: metrics summed on the shell's port, each member labelled).
type labelled struct {
	g         prometheus.Gatherer
	component string
}

func (l labelled) Gather() ([]*dto.MetricFamily, error) {
	fams, err := l.g.Gather()
	name, value := "component", l.component
	for _, f := range fams {
		for _, m := range f.Metric {
			m.Label = append(m.Label, &dto.LabelPair{Name: proto.String(name), Value: proto.String(value)})
		}
	}
	return fams, err
}
