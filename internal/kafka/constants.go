package kafka

const (
	EventTopic = "events"
	RetryTopic = "retries"
	DLQTopic   = "dead-letter"

	DeliveryGroup    = "delivery-worker"
	RetryWorkerGroup = "retry-worker"
)
