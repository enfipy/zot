package api_test

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"sync"
	"testing"

	godigest "github.com/opencontainers/go-digest"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/resty.v1"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
	"zotregistry.dev/zot/v2/pkg/api/constants"
	test "zotregistry.dev/zot/v2/pkg/test/common"
	. "zotregistry.dev/zot/v2/pkg/test/image-utils"
)

func TestCopyBlobOnRead(t *testing.T) {
	const password = "secret"

	users := []string{"admin", "builder", "reader", "owner"}
	content := ""

	for _, user := range users {
		content += test.GetBcryptCredString(user, password)
	}

	start := func(hydrate bool) (string, string, func()) {
		conf := config.New()
		conf.HTTP.Port = "0"
		conf.Storage.GC = false
		conf.Storage.Dedupe = false
		conf.Storage.HydrateBlobOnRead = hydrate
		conf.HTTP.Auth = &config.AuthConfig{HTPasswd: config.AuthHTPasswd{Path: test.MakeHtpasswdFileFromString(t, content)}}
		conf.HTTP.AccessControl = &config.AccessControlConfig{
			Repositories: config.Repositories{
				"mirror/**": config.PolicyGroup{DefaultPolicy: []string{constants.ReadPermission}},
				"team/**": config.PolicyGroup{Policies: []config.Policy{
					{Users: []string{"builder"}, Actions: []string{constants.ReadPermission, constants.CreatePermission}},
					{Users: []string{"reader"}, Actions: []string{constants.ReadPermission}},
				}},
				"other/**": config.PolicyGroup{Policies: []config.Policy{
					{Users: []string{"owner"}, Actions: []string{constants.ReadPermission, constants.CreatePermission}},
				}},
			},
			AdminPolicy: config.Policy{
				Users:   []string{"admin"},
				Actions: []string{constants.ReadPermission, constants.CreatePermission, constants.UpdatePermission},
			},
		}

		dir := t.TempDir()
		ctlr := api.NewController(conf)
		ctlr.Config.Storage.RootDirectory = dir

		manager := test.NewControllerManager(ctlr)
		baseURL := manager.StartAndWait()

		return baseURL, dir, manager.StopServer
	}

	client := func(user string) *resty.Client {
		return resty.New().SetBasicAuth(user, password)
	}

	head := func(baseURL, user, repo string, digest godigest.Digest) *resty.Response {
		resp, err := client(user).R().Head(fmt.Sprintf("%s/v2/%s/blobs/%s", baseURL, repo, digest))
		So(err, ShouldBeNil)

		return resp
	}

	blobFile := func(dir, repo string, digest godigest.Digest) string {
		return path.Join(dir, repo, "blobs", digest.Algorithm().String(), digest.Encoded())
	}

	Convey("Without dedupe, hydrateBlobOnRead copies a blob from a repository the caller may read", t, func() {
		baseURL, dir, stop := start(true)
		defer stop()

		mirrored := CreateImageWith().RandomLayers(1, 4096).DefaultConfig().Build()
		So(UploadImageWithBasicAuth(mirrored, baseURL, "mirror/alpine", "3.22", "admin", password), ShouldBeNil)

		private := CreateImageWith().RandomLayers(1, 4096).DefaultConfig().Build()
		So(UploadImageWithBasicAuth(private, baseURL, "other/secret", "1", "owner", password), ShouldBeNil)

		layer := mirrored.Manifest.Layers[0]

		Convey("A writer of the repository gets the blob, held in full", func() {
			resp := head(baseURL, "builder", "team/app", layer.Digest)
			So(resp.StatusCode(), ShouldEqual, http.StatusOK)
			So(resp.Header().Get("Content-Length"), ShouldEqual, fmt.Sprint(layer.Size))
			So(resp.Header().Get(constants.DistContentDigestKey), ShouldEqual, layer.Digest.String())

			info, err := os.Stat(blobFile(dir, "team/app", layer.Digest))
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, layer.Size)

			got, err := client("builder").R().Get(fmt.Sprintf("%s/v2/team/app/blobs/%s", baseURL, layer.Digest))
			So(err, ShouldBeNil)
			So(got.StatusCode(), ShouldEqual, http.StatusOK)
			So(godigest.FromBytes(got.Body()), ShouldEqual, layer.Digest)

			So(head(baseURL, "builder", "mirror/alpine", layer.Digest).StatusCode(), ShouldEqual, http.StatusOK)
		})

		Convey("Requests at once share one copy", func() {
			var wait sync.WaitGroup

			codes := make([]int, 8)

			for i := range codes {
				wait.Go(func() {
					resp, err := client("builder").R().
						Head(fmt.Sprintf("%s/v2/team/app/blobs/%s", baseURL, layer.Digest))
					if err == nil {
						codes[i] = resp.StatusCode()
					}
				})
			}

			wait.Wait()

			for _, code := range codes {
				So(code, ShouldEqual, http.StatusOK)
			}
		})

		Convey("A repository the caller may not read is no source", func() {
			resp := head(baseURL, "builder", "team/app", private.Manifest.Layers[0].Digest)
			So(resp.StatusCode(), ShouldEqual, http.StatusNotFound)

			_, err := os.Stat(blobFile(dir, "team/app", private.Manifest.Layers[0].Digest))
			So(os.IsNotExist(err), ShouldBeTrue)
		})

		Convey("A caller that may not push to the repository gets no copy", func() {
			So(head(baseURL, "reader", "team/app", layer.Digest).StatusCode(), ShouldEqual, http.StatusNotFound)

			_, err := os.Stat(blobFile(dir, "team/app", layer.Digest))
			So(os.IsNotExist(err), ShouldBeTrue)
		})

		Convey("A digest no repository holds stays missing", func() {
			missing := godigest.FromString("held nowhere")
			So(head(baseURL, "builder", "team/app", missing).StatusCode(), ShouldEqual, http.StatusNotFound)
		})
	})

	Convey("Without hydrateBlobOnRead a blob read stays repository-local", t, func() {
		baseURL, dir, stop := start(false)
		defer stop()

		mirrored := CreateImageWith().RandomLayers(1, 4096).DefaultConfig().Build()
		So(UploadImageWithBasicAuth(mirrored, baseURL, "mirror/alpine", "3.22", "admin", password), ShouldBeNil)

		layer := mirrored.Manifest.Layers[0]
		So(head(baseURL, "builder", "team/app", layer.Digest).StatusCode(), ShouldEqual, http.StatusNotFound)

		_, err := os.Stat(blobFile(dir, "team/app", layer.Digest))
		So(os.IsNotExist(err), ShouldBeTrue)
	})
}
