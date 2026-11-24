package sim

// ServiceInfo describes one simulated resort system.
type ServiceInfo struct {
	Name   string
	Async  bool
	Target float64
	RPS    float64
}

// Services returns 36 ski-resort services. The exact rates are deterministic;
// the traffic RNG still uses the fixed verification seed.
func Services() []ServiceInfo {
	names := []string{
		"api-gateway", "auth-service", "booking-service", "payment-service",
		"order-service", "ticket-catalog", "ticket-issuer", "season-pass",
		"lift-pass-service", "qrcode-service", "gate-access", "turnstile-control",
		"snowmaking-scheduler", "snow-gun-control", "weather-station", "snow-depth-sensor",
		"slope-status", "lift-monitor", "chair-control", "cableway-scada",
		"dispatch-radio", "patrol-incident", "emergency-response", "rental-shop",
		"equipment-qc", "locker-service", "pos-service", "restaurant-order",
		"inventory-service", "parking-gate", "shuttle-bus", "ski-school",
		"notification-worker", "event-bus", "iot-mqtt-gateway", "analytics-pipeline",
	}
	async := map[string]bool{
		"notification-worker": true,
		"event-bus":           true,
		"iot-mqtt-gateway":    true,
		"analytics-pipeline":  true,
	}
	out := make([]ServiceInfo, 0, len(names))
	for i, name := range names {
		rps := 0.55 + float64(i%6)*0.16 // 0.55 .. 1.35 new root traces/s
		// Targets stay above the normal deterministic-policy floor and scale
		// with each service's arrival volume. payment-service is itself the
		// configured critical-service policy, so nearly all its roots are
		// mandatory and its target reflects that floor plus simulated spikes.
		target := 0.16 + rps*0.30
		if name == "payment-service" {
			target = 1.35
		}
		out = append(out, ServiceInfo{Name: name, Async: async[name], Target: target, RPS: rps})
	}
	return out
}

func TargetMap(services []ServiceInfo) map[string]float64 {
	m := make(map[string]float64, len(services))
	for _, s := range services {
		m[s.Name] = s.Target
	}
	return m
}
