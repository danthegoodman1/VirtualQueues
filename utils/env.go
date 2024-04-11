package utils

import "os"

var (
	Env_SleepSeconds = MustEnvOrDefaultInt64("SHUTDOWN_SLEEP_SEC", 0)

	Env_InstanceID     = os.Getenv("INSTANCE_ID")
	Env_ConsumerGroup  = os.Getenv("CONSUMER_GROUP")
	Env_KafkaTopic     = os.Getenv("KAFKA_TOPIC")
	Env_KafkaSessionMs = MustEnvOrDefaultInt64("KAFKA_SESSION_MS", 60_000)
	Env_KafkaSeeds     = os.Getenv("KAFKA_SEEDS")
	Env_KafkaUsername  = os.Getenv("KAFKA_USER")
	Env_NumPartitions  = MustEnvInt64("KAFKA_PARTITIONS")
	Env_KafkaPassword  = os.Getenv("KAFKA_PASS")
	Env_KafkaTLS       = os.Getenv("KAFKA_TLS") == "1"
	Env_KafkaTLSCAPath = os.Getenv("KAFKA_TLS_CA_PATH")

	Env_APIPort       = EnvOrDefault("API_PORT", "8190")
	Env_InternalPort  = EnvOrDefault("INTERNAL_PORT", "8191")
	Env_GossipPort    = MustEnvOrDefaultInt64("GOSSIP_PORT", 8192)
	Env_AdvertiseAddr = os.Getenv("ADVERTISE_ADDR") // API, e.g. localhost:8190
	// csv like localhost:8192,localhost:8292
	Env_GossipPeers       = os.Getenv("GOSSIP_PEERS")
	Env_GossipBroadcastMS = MustEnvOrDefaultInt64("GOSSIP_BROADCAST_MS", 1000)

	Env_Debug       = os.Getenv("DEBUG") == "1"
	Env_GossipDebug = os.Getenv("GOSSIP_DEBUG") == "1"

	Env_Profile = os.Getenv("PROFILE") == "1"
)
