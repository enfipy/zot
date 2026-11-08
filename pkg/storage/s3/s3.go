package s3

import (
	// Add s3 support.
	"github.com/distribution/distribution/v3/registry/storage/driver"
	// Load s3 driver.
	_ "github.com/distribution/distribution/v3/registry/storage/driver/s3-aws"

	"zotregistry.dev/zot/v2/pkg/compat"
	"zotregistry.dev/zot/v2/pkg/extensions/events"
	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	common "zotregistry.dev/zot/v2/pkg/storage/common"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// NewImageStore returns a new image store backed by cloud storages.
// see https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers
// Use cacheDriver to properly set a cache database, or it will default to boltDB local storage.
// With params, the storage driver's parameters, whole blobs are written and copied in parallel
// parts (parts.go); without them, or when they make no client, through store alone.
func NewImageStore(rootDir string, cacheDir string, dedupe, commit bool, log zlog.Logger,
	metrics monitoring.MetricServer, linter common.Lint, store driver.StorageDriver,
	cacheDriver storageTypes.Cache, compat []compat.MediaCompatibility, recorder events.Recorder,
	params ...map[string]any,
) storageTypes.ImageStore {
	var storeDriver storageTypes.Driver = New(store)

	if len(params) > 0 && params[0] != nil {
		withParts, err := NewWithParts(store, params[0])
		if err != nil {
			log.Warn().Err(err).Msg("failed to make a client for parallel parts; blobs are written one part at a time")
		} else {
			storeDriver = withParts
		}
	}

	return imagestore.NewImageStore(
		rootDir,
		cacheDir,
		dedupe,
		commit,
		log,
		metrics,
		linter,
		storeDriver,
		cacheDriver,
		compat,
		recorder,
	)
}
