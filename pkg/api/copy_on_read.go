package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/singleflight"

	zerr "zotregistry.dev/zot/v2/errors"
	"zotregistry.dev/zot/v2/pkg/api/constants"
	mTypes "zotregistry.dev/zot/v2/pkg/meta/types"
	reqCtx "zotregistry.dev/zot/v2/pkg/requestcontext"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// blobCopies runs one copy of a blob into a repository at a time, however many requests ask.
var blobCopies singleflight.Group //nolint:gochecknoglobals

// copyBlobOnRead is hydrateBlobOnRead for a store without dedupe. With no dedupe cache to link a
// digest from, a blob the repository lacks is copied, in full, from another repository of the
// same store that holds it and that the caller may read, found through MetaDB. A client that asks
// whether the repository holds a layer before uploading it, as every image pusher does, then skips
// the upload, while the store still holds each repository's blobs in full, never a placeholder
// that only the cache resolves. The copy reads and writes outside the store lock, as an upload
// does, and goes on when the client leaves, so that the client's next attempt finds it done.
func (rh *RouteHandler) copyBlobOnRead(
	request *http.Request, imgStore storageTypes.ImageStore, repo string, digest godigest.Digest,
) (bool, int64, error) {
	ok, size, err := statBlob(imgStore, repo, digest)
	if ok || !blobMissing(err) || rh.c.MetaDB == nil {
		return ok, size, err
	}

	userAc, acErr := reqCtx.UserAcFromContext(request.Context())
	if acErr != nil {
		return false, -1, acErr
	}

	checkAccess := shouldCheckMountSourceAccess(rh.c.Config.CopyAccessControlConfig(), userAc)
	if checkAccess && !userAc.CanOnResource(constants.CreatePermission, repo, digest.String()) {
		return ok, size, err
	}

	for _, source := range rh.blobHolders(request.Context(), userAc, checkAccess, imgStore, repo, digest) {
		key := fmt.Sprintf("%p %s %s", imgStore, repo, digest)

		copied, copyErr, _ := blobCopies.Do(key, func() (any, error) {
			return rh.copyBlob(context.WithoutCancel(request.Context()), imgStore, source, repo, digest)
		})
		if copyErr == nil {
			copiedSize, _ := copied.(int64)

			return true, copiedSize, nil
		}

		rh.c.Log.Error().Err(copyErr).Str("repository", repo).Str("source", source).
			Str("digest", digest.String()).Msg("failed to copy blob on read")
	}

	return ok, size, err
}

// blobHolders lists the repositories of imgStore, other than repo, whose tagged images hold digest
// as a layer or config and which the caller may read.
func (rh *RouteHandler) blobHolders(ctx context.Context, userAc *reqCtx.UserAccessControl, checkAccess bool,
	imgStore storageTypes.ImageStore, repo string, digest godigest.Digest,
) []string {
	readable := map[string]bool{}

	acceptRepo := func(name, _ string) bool {
		if name == repo || rh.getImageStore(name) != imgStore {
			return false
		}

		if allowed, seen := readable[name]; seen {
			return allowed
		}

		readable[name] = !checkAccess || userAc.CanOnResource(constants.ReadPermission, name, digest.String())

		return readable[name]
	}

	holds := func(_ mTypes.RepoMeta, imageMeta mTypes.ImageMeta) bool {
		return slices.ContainsFunc(imageMeta.Manifests, func(manifest mTypes.ManifestMeta) bool {
			return manifest.Manifest.Config.Digest == digest ||
				slices.ContainsFunc(manifest.Manifest.Layers, func(layer ispec.Descriptor) bool {
					return layer.Digest == digest
				})
		})
	}

	// FilterTags joins the errors of entries it could not read to what it found in the others.
	images, err := rh.c.MetaDB.FilterTags(ctx, acceptRepo, holds)
	if err != nil {
		rh.c.Log.Warn().Err(err).Str("digest", digest.String()).Msg("failed to read some images while finding a blob")
	}

	holders := []string{}

	for _, image := range images {
		if !slices.Contains(holders, image.Repo) {
			holders = append(holders, image.Repo)
		}
	}

	return holders
}

func (rh *RouteHandler) copyBlob(ctx context.Context, imgStore storageTypes.ImageStore, source, repo string,
	digest godigest.Digest,
) (int64, error) {
	// A copy that finished while this request waited for its turn.
	if ok, size, _ := statBlob(imgStore, repo, digest); ok {
		return size, nil
	}

	start := time.Now()

	reader, _, err := imgStore.GetBlob(source, digest, ispec.MediaTypeImageLayer)
	if err != nil {
		return -1, err
	}
	defer reader.Close()

	_, size, err := imgStore.FullBlobUpload(ctx, repo, reader, digest)
	if err != nil {
		return -1, err
	}

	rh.c.Log.Info().Str("repository", repo).Str("source", source).Str("digest", digest.String()).
		Int64("size", size).Str("duration", time.Since(start).String()).Msg("copied blob on read")

	return size, nil
}

func statBlob(imgStore storageTypes.ImageStore, repo string, digest godigest.Digest) (bool, int64, error) {
	var ok bool

	var size int64

	err := imgStore.WithRepoReadLock(repo, func() error {
		var statErr error

		ok, size, _, statErr = imgStore.StatBlob(repo, digest)

		return statErr
	})

	return ok, size, err
}

// blobMissing reports whether a failed StatBlob found no blob, in a repository or no repository yet.
func blobMissing(err error) bool {
	return errors.Is(err, zerr.ErrBlobNotFound) || errors.Is(err, zerr.ErrRepoNotFound)
}
