package azureeventhub

// pretty much copied from  github.com/Azure/azure-event-hubs-go/v3/storage/storageLeaserCheckpointer
import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/url"

	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
	"github.com/Azure/azure-event-hubs-go/v3/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
)

type LeaseFixer struct {
	credential     storage.Credential
	containerURL   *azblob.ContainerURL
	serviceURL     *azblob.ServiceURL
	containerName  string
	accountName    string
	blobPathPrefix string
	env            azure.Environment
	processor      *eph.EventProcessorHost
}

type StorageLease struct {
	*eph.Lease
	Checkpoint *persist.Checkpoint   `json:"checkpoint"`
	State      azblob.LeaseStateType `json:"state"`
	Token      string                `json:"token"`
}

func NewLeaseFixer(credential storage.Credential, accountName, containerName string, env azure.Environment, eph *eph.EventProcessorHost) (*LeaseFixer, error) {
	storageURL, err := url.Parse("https://" + accountName + ".blob." + env.StorageEndpointSuffix)
	if err != nil {
		return nil, err
	}

	svURL := azblob.NewServiceURL(*storageURL, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	containerURL := svURL.NewContainerURL(containerName)

	return &LeaseFixer{
		credential:    credential,
		containerName: containerName,
		accountName:   accountName,
		env:           env,
		serviceURL:    &svURL,
		containerURL:  &containerURL,
		processor:     eph,
	}, nil
}

// GetLeases gets all of the partition leases
func (sl *LeaseFixer) GetLeases(ctx context.Context) ([]*StorageLease, error) {
	pids := sl.processor.GetPartitionIDs()
	leases := make([]*StorageLease, len(pids))
	for i, pid := range pids {
		lease, err := sl.getLease(ctx, pid)
		if err != nil {
			return nil, err
		}
		leases[i] = lease
	}
	return leases, nil
}

// ReleaseLease releases the lease to the blob in Azure storage
func (sl *LeaseFixer) ReleaseLease(ctx context.Context, lease *StorageLease) error {
	blobURL := sl.containerURL.NewBlobURL(sl.blobPathPrefix + lease.GetPartitionID())
	_, err := blobURL.ReleaseLease(ctx, lease.Token, azblob.ModifiedAccessConditions{})
	return err
}

func (sl *LeaseFixer) getLease(ctx context.Context, partitionID string) (*StorageLease, error) {
	blobURL := sl.containerURL.NewBlobURL(sl.blobPathPrefix + partitionID)
	res, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	return sl.leaseFromResponse(res)
}

func (sl *LeaseFixer) leaseFromResponse(res *azblob.DownloadResponse) (*StorageLease, error) {
	b, err := ioutil.ReadAll(res.Response().Body)
	if err != nil {
		return nil, err
	}

	var lease StorageLease
	if err := json.Unmarshal(b, &lease); err != nil {
		return nil, err
	}
	lease.State = res.LeaseState()
	return &lease, nil
}
