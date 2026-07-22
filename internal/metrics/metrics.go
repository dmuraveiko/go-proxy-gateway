package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	NetworkRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_network_requests_total",
			Help: "Total NATS network requests",
		},
		[]string{"direction", "status", "kind"},
	)

	DBRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_db_requests_total",
			Help: "Total database requests",
		},
		[]string{"direction", "status", "kind"},
	)

	HTTPDispatches = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_http_dispatches_total",
			Help: "Total outgoing HTTP dispatches to external services",
		},
		[]string{"status"},
	)

	DeliveriesPending = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "proxy_deliveries_pending",
			Help: "Number of pending undelivered messages",
		},
	)

	DeliveryRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_delivery_retries_total",
			Help: "Total delivery retry attempts",
		},
		[]string{"kind"},
	)
)

func init() {
	prometheus.MustRegister(NetworkRequests, DBRequests, HTTPDispatches, DeliveriesPending, DeliveryRetries)
}
