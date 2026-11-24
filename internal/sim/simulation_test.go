package sim

import (
	"testing"
	"time"
)

func TestFixedSeedSimulationAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-minute virtual simulation in short mode")
	}

	report, err := Run(Config{
		Seed:         15833,
		Duration:     20 * time.Minute,
		MaxDeviation: 0.30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Pass {
		for _, c := range report.Checks {
			if !c.Pass {
				t.Errorf("check failed: %s — %s", c.Name, c.Detail)
			}
		}
		for _, missed := range report.MissedMustKeep {
			t.Errorf("missed mandatory trace: %s", missed)
		}
	}
	if report.GeneratedTraces < 10_000 || report.GeneratedSpans < 100_000 {
		t.Fatalf("simulation too small: traces=%d spans=%d", report.GeneratedTraces, report.GeneratedSpans)
	}
}
