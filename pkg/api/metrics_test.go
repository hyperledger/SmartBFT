package api_test

import (
	"testing"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/stretchr/testify/require"
)

var countOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "count_of_files",
	Help:         "Count.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nodesOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "node",
	Help:         "Node ID.",
	LabelNames:   []string{"id"},
	StatsdFormat: "%{#fqname}.%{id}",
}

func TestMakeStatsdFormat(t *testing.T) {
	countOptsTmp := api.NewGaugeOpts(countOpts, []string{"label2", "label1"})
	nodesOptsTmp := api.NewGaugeOpts(nodesOpts, []string{"label2", "label1"})
	require.Equal(t, "%{#fqname}.%{label1}.%{label2}", countOptsTmp.StatsdFormat)
	require.Equal(t, "%{#fqname}.%{id}.%{label1}.%{label2}", nodesOptsTmp.StatsdFormat)
}

func TestMakeLabelNames(t *testing.T) {
	countOptsTmp := api.NewGaugeOpts(countOpts, []string{"label2", "label1"})
	nodesOptsTmp := api.NewGaugeOpts(nodesOpts, []string{"label2", "label1"})
	require.Equal(t, []string{"label1", "label2"}, countOptsTmp.LabelNames)
	require.Equal(t, []string{"id", "label1", "label2"}, nodesOptsTmp.LabelNames)
}
