package envelope_test

import (
	"testing"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

type vectorContext struct {
	Kind    string `json:"kind"`
	Handled struct {
		ID       string `json:"id"`
		HopCount int    `json:"hop_count"`
	} `json:"handled"`
	Job struct {
		CausationID string `json:"causation_id"`
		HopCount    int    `json:"hop_count"`
	} `json:"job"`
}

func originOf(t *testing.T, c vectorContext) envelope.Origin {
	t.Helper()
	switch c.Kind {
	case "request":
		return envelope.Origin{Kind: envelope.OriginRequest}
	case "event":
		return envelope.Origin{Kind: envelope.OriginEvent, CausationID: c.Handled.ID, HopCount: c.Handled.HopCount}
	case "queued_job":
		return envelope.Origin{Kind: envelope.OriginQueuedJob, CausationID: c.Job.CausationID, HopCount: c.Job.HopCount}
	case "cron":
		return envelope.Origin{Kind: envelope.OriginCron}
	case "singleton":
		return envelope.Origin{Kind: envelope.OriginSingleton}
	case "every":
		return envelope.Origin{Kind: envelope.OriginEvery}
	case "reconciler":
		return envelope.Origin{Kind: envelope.OriginReconciler}
	}
	t.Fatalf("unknown context kind %q", c.Kind)
	return envelope.Origin{}
}

func TestDeriveVectors(t *testing.T) {
	decode := func(t *testing.T, c vectors.Case) envelope.Origin {
		var in struct {
			Context vectorContext `json:"context"`
		}
		mustUnmarshal(t, c.Input, &in)
		return originOf(t, in.Context)
	}
	vectors.Run(t, "envelope", "derive", map[string]func(*testing.T, vectors.Case){
		"derive": func(t *testing.T, c vectors.Case) {
			causation, hop := envelope.Derive(decode(t, c))
			vectors.RequireJSON(t, c, map[string]any{"causation_id": causation, "hop_count": hop})
		},
		"enqueue_context": func(t *testing.T, c vectors.Case) {
			causation, hop := envelope.EnqueueContext(decode(t, c))
			vectors.RequireJSON(t, c, map[string]any{"job_causation_id": causation, "job_hop_count": hop})
		},
	})
}

func TestDeriveEveryStartsAChain(t *testing.T) {
	causation, hop := envelope.Derive(envelope.Origin{Kind: envelope.OriginEvery, CausationID: "ignored", HopCount: 3})
	if causation != "" || hop != 0 {
		t.Fatalf("every: %q %d", causation, hop)
	}
}
