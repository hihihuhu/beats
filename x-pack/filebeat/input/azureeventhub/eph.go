// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

//go:build !aix

package azureeventhub

import (
	"context"
	"errors"
	"fmt"
	"time"
	_ "unsafe"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
)

// users can select from one of the already defined azure cloud envs
var environments = map[string]azure.Environment{
	azure.ChinaCloud.ResourceManagerEndpoint:        azure.ChinaCloud,
	azure.GermanCloud.ResourceManagerEndpoint:       azure.GermanCloud,
	azure.PublicCloud.ResourceManagerEndpoint:       azure.PublicCloud,
	azure.USGovernmentCloud.ResourceManagerEndpoint: azure.USGovernmentCloud,
}

// runWithEPH will consume ingested events using the Event Processor Host (EPH).
//
// To learn more, check the following resources:
// - https://github.com/Azure/azure-event-hubs-go#event-processor-host
// - https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-event-processor-host
func (a *azureInput) runWithEPH() error {
	// create a new Azure Storage Leaser / Checkpointer
	cred, err := azblob.NewSharedKeyCredential(a.config.SAName, a.config.SAKey)
	if err != nil {
		return err
	}
	env, err := getAzureEnvironment(a.config.OverrideEnvironment)
	if err != nil {
		return err
	}
	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, a.config.SAName, a.config.SAContainer, env)
	if err != nil {
		a.log.Errorw("error creating storage leaser checkpointer", "error", err)
		return err
	}

	// adding a nil EventProcessorHostOption will break the code,
	// this is why a condition is added and a.processor is assigned.
	if a.config.ConsumerGroup != "" {
		a.processor, err = eph.NewFromConnectionString(
			a.workerCtx,
			fmt.Sprintf("%s%s%s", a.config.ConnectionString, eventHubConnector, a.config.EventHubName),
			leaserCheckpointer,
			leaserCheckpointer,
			eph.WithConsumerGroup(a.config.ConsumerGroup),
			eph.WithNoBanner())
	} else {
		a.processor, err = eph.NewFromConnectionString(
			a.workerCtx,
			fmt.Sprintf("%s%s%s", a.config.ConnectionString, eventHubConnector, a.config.EventHubName),
			leaserCheckpointer,
			leaserCheckpointer,
			eph.WithNoBanner())
	}
	if err != nil {
		a.log.Errorw("error creating processor", "error", err)
		return err
	}

	watcherCtx, cancelWatcher := context.WithCancel(a.workerCtx)
	// register a message handler -- many can be registered
	handlerID, err := a.processor.RegisterHandler(a.workerCtx,
		func(c context.Context, e *eventhub.Event) error {
			var onEventErr error
			// partitionID is not yet mapped in the azure-eventhub sdk
			ok := a.processEvents(e, "")
			if !ok {
				onEventErr = errors.New("OnEvent function returned false. Stopping input worker")
				a.log.Error(onEventErr.Error())
				a.Stop()
				// something serious wrong happened, just panic here and let k8s restart the process
				// this is likely a bug in the azure-event-hubs-go, where the error is ignored when close the scheduler
				// https://github.com/Azure/azure-event-hubs-go/blob/v3.3.18/eph/scheduler.go#L193
				// thus lead to half closed processor
				cancelWatcher()
				panic(onEventErr)
			}
			return onEventErr
		})
	if err != nil {
		a.log.Errorw("error registering handler", "error", err)
		return err
	}
	a.log.Infof("handler id: %q is registered\n", handlerID)

	// Start handling messages from all of the partitions balancing across
	// multiple consumers.
	// The processor can be stopped by calling `Close()` on the processor.
	err = a.processor.StartNonBlocking(a.workerCtx)
	if err != nil {
		a.log.Errorw("error starting the processor", "error", err)
		return err
	}

	lf, err := NewLeaseFixer(cred, a.config.SAName, a.config.SAContainer, env, a.processor)
	if err != nil {
		a.log.Errorw("error creating lease fixer", "error", err)
		return err
	}

	// temporary workaround
	// there is probaby some nasty bug in the azure-event-hubs-go sdk
	// where the consumption stops but lease keep renewing
	// so crash the process if the process doesn't do anything in 1 minute
	go func() {
		zeroCount := 0
		for {
			select {
			case <-watcherCtx.Done():
				return
			default:
				if len(a.processor.PartitionIDsBeingProcessed()) == 0 {
					zeroCount++
				} else {
					// clear the counter if there is any activity
					zeroCount = 0
				}
				if zeroCount > 60 {
					a.log.Errorw("process is idle for a while")
					// if one process is idle for a while, then it will check for stale leases
					leases, err := lf.GetLeases(watcherCtx)
					if err != nil {
						a.log.Errorw("error getting leases", "error", err)
					} else {
						var lastErr error
						for _, lease := range leases {
							// the checkpoint is not updated for a while, likely hits the bug
							if lease.Checkpoint.EnqueueTime.Before(time.Now().Add(-30 * time.Minute)) {
								a.log.Errorw("lease is stale, deleting", "lease", lease, "checkpoint", lease.Checkpoint)
								if err = lf.ReleaseLease(watcherCtx, lease); err != nil {
									a.log.Errorw("error deleting lease", "error", err)
									lastErr = err
								}
							}
						}
						if lastErr == nil {
							// reset the counter if no error, so that it won't repeatly check for stale leases
							zeroCount = 0
						}
					}
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return nil
}

func getAzureEnvironment(overrideResManager string) (azure.Environment, error) {
	// if no override is set then the azure public cloud is used
	if overrideResManager == "" || overrideResManager == "<no value>" {
		return azure.PublicCloud, nil
	}
	if env, ok := environments[overrideResManager]; ok {
		return env, nil
	}
	// can retrieve hybrid env from the resource manager endpoint
	return azure.EnvironmentFromURL(overrideResManager)
}
