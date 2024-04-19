package main

import (
	"context"
	"github.com/danthegoodman1/VirtualQueues/api"
	"github.com/danthegoodman1/VirtualQueues/gologger"
	"github.com/danthegoodman1/VirtualQueues/internal"
	"github.com/danthegoodman1/VirtualQueues/log_consumer"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	logger = gologger.NewLogger()
)

func main() {
	logger.Info().Msg("Starting VirtualQueues")

	g := errgroup.Group{}
	g.Go(func() error {
		logger.Debug().Msg("starting internal server")
		return internal.StartServer()
	})

	pm := partitions.Map{}

	var logConsumer *log_consumer.LogConsumer
	g.Go(func() (err error) {
		logConsumer, err = log_consumer.NewLogConsumer(utils.Env_InstanceID, utils.Env_ConsumerGroup, utils.Env_KafkaDataTopic, utils.Env_KafkaOffsetTopic, utils.Env_KafkaDataTopic, utils.Env_AdvertiseAddr, strings.Split(utils.Env_KafkaSeeds, ","), utils.Env_KafkaSessionMs, &pm)
		return
	})

	err := g.Wait()
	if err != nil {
		logger.Fatal().Err(err).Msg("Error starting services, exiting")
	}

	apiServer, err := api.StartServer(utils.Env_APIPort, logConsumer, uint32(utils.Env_NumPartitions))
	if err != nil {
		logger.Fatal().Err(err).Msg("error starting api server")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	logger.Info().Msg("received shutdown signal!")

	// Provide time for load balancers to de-register before stopping accepting connections
	if utils.Env_SleepSeconds > 0 {
		logger.Info().Msgf("sleeping for %ds before exiting", utils.Env_SleepSeconds)
		time.Sleep(time.Second * time.Duration(utils.Env_SleepSeconds))
		logger.Info().Msgf("slept for %ds, exiting", utils.Env_SleepSeconds)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(utils.Env_SleepSeconds))
	defer cancel()

	g = errgroup.Group{}
	g.Go(func() error {
		return internal.Shutdown(ctx)
	})
	g.Go(func() error {
		return apiServer.Shutdown(ctx)
	})
	g.Go(func() error {
		return logConsumer.Shutdown()
	})

	if err := g.Wait(); err != nil {
		logger.Error().Err(err).Msg("error shutting down servers")
		os.Exit(1)
	}
	logger.Info().Msg("shutdown servers, exiting")
}
