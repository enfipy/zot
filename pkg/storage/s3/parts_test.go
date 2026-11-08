package s3

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // an ETag, as S3 makes it
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	godigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"

	zerr "zotregistry.dev/zot/v2/errors"
	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	"zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage/errclass"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// fakeS3 is a bucket that keeps completed objects in store, at keys equal to their paths, and their
// ETags in etags, as S3 makes them. hook, when set, runs before each request and may hold it or
// fail it.
type fakeS3 struct {
	s3iface.S3API

	store storagedriver.StorageDriver
	hook  func(ctx context.Context, op string, number int64, attempt int) error

	mu        sync.Mutex
	uploads   map[string]map[int64][]byte
	keys      map[string]string
	next      int
	attempts  map[string]int
	inFlight  int
	most      int
	calls     map[string]int
	completed map[string]bool
	etags     map[string]string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		store: inmemory.New(), uploads: map[string]map[int64][]byte{}, keys: map[string]string{},
		attempts: map[string]int{}, calls: map[string]int{}, completed: map[string]bool{},
		etags: map[string]string{},
	}
}

func (f *fakeS3) begin(ctx context.Context, op string, number int64) error {
	f.mu.Lock()
	f.calls[op]++
	name := fmt.Sprintf("%s %d", op, number)
	f.attempts[name]++
	attempt := f.attempts[name]
	f.inFlight++
	f.most = max(f.most, f.inFlight)
	f.mu.Unlock()

	if f.hook == nil {
		return nil
	}

	return f.hook(ctx, op, number, attempt)
}

func (f *fakeS3) end() {
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
}

func (f *fakeS3) attempt(op string, number int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.attempts[fmt.Sprintf("%s %d", op, number)]
}

func (f *fakeS3) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[op]
}

// held answers a request only when its context ends, as a store that holds it does.
func held(ctx context.Context) error {
	<-ctx.Done()

	return awserr.New(request.CanceledErrorCode, "request context canceled", ctx.Err())
}

func failure(code string, status int) error {
	return awserr.NewRequestFailure(awserr.New(code, code, nil), status, "request")
}

func (f *fakeS3) CreateMultipartUploadWithContext(ctx aws.Context, in *s3.CreateMultipartUploadInput,
	_ ...request.Option,
) (*s3.CreateMultipartUploadOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "create", 0); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.next++
	id := fmt.Sprint(f.next)
	f.uploads[id] = map[int64][]byte{}
	f.keys[id] = *in.Key

	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (f *fakeS3) part(id string, number int64, data []byte) (*string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	upload, ok := f.uploads[id]
	if !ok {
		return nil, failure(s3.ErrCodeNoSuchUpload, 404)
	}

	upload[number] = data

	return aws.String(fmt.Sprintf("%x", md5.Sum(data))), nil //nolint:gosec
}

func (f *fakeS3) UploadPartWithContext(ctx aws.Context, in *s3.UploadPartInput, _ ...request.Option,
) (*s3.UploadPartOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "part", *in.PartNumber); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	etag, err := f.part(*in.UploadId, *in.PartNumber, data)

	return &s3.UploadPartOutput{ETag: etag}, err
}

func (f *fakeS3) source(copySource string) ([]byte, error) {
	_, key, _ := strings.Cut(copySource, "/")

	return f.store.GetContent(context.Background(), key)
}

func (f *fakeS3) UploadPartCopyWithContext(ctx aws.Context, in *s3.UploadPartCopyInput, _ ...request.Option,
) (*s3.UploadPartCopyOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "copy", *in.PartNumber); err != nil {
		return nil, err
	}

	data, err := f.source(*in.CopySource)
	if err != nil {
		return nil, failure("NoSuchKey", 404)
	}

	if in.CopySourceRange != nil {
		var first, last int

		fmt.Sscanf(*in.CopySourceRange, "bytes=%d-%d", &first, &last)

		data = data[first : last+1]
	}

	etag, err := f.part(*in.UploadId, *in.PartNumber, data)

	return &s3.UploadPartCopyOutput{CopyPartResult: &s3.CopyPartResult{ETag: etag}}, err
}

func (f *fakeS3) finish(id string, parts []*s3.CompletedPart) error {
	f.mu.Lock()
	upload, ok := f.uploads[id]
	key := f.keys[id]
	delete(f.uploads, id)
	f.mu.Unlock()

	if !ok {
		return failure(s3.ErrCodeNoSuchUpload, 404)
	}

	var whole []byte

	sum := md5.New() //nolint:gosec

	for i, part := range parts {
		if *part.PartNumber != int64(i+1) {
			return failure("InvalidPartOrder", 400)
		}

		data := upload[*part.PartNumber]
		partSum := md5.Sum(data) //nolint:gosec
		whole = append(whole, data...)
		sum.Write(partSum[:])
	}

	f.mu.Lock()
	f.etags[key] = fmt.Sprintf("%x-%d", sum.Sum(nil), len(parts))
	f.mu.Unlock()

	return f.store.PutContent(context.Background(), key, whole)
}

func (f *fakeS3) CompleteMultipartUploadWithContext(ctx aws.Context, in *s3.CompleteMultipartUploadInput,
	_ ...request.Option,
) (*s3.CompleteMultipartUploadOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "complete", 0); err != nil {
		return nil, err
	}

	return &s3.CompleteMultipartUploadOutput{}, f.finish(*in.UploadId, in.MultipartUpload.Parts)
}

func (f *fakeS3) AbortMultipartUploadWithContext(ctx aws.Context, in *s3.AbortMultipartUploadInput,
	_ ...request.Option,
) (*s3.AbortMultipartUploadOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "abort", 0); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.uploads[*in.UploadId]; !ok {
		return nil, failure(s3.ErrCodeNoSuchUpload, 404)
	}

	delete(f.uploads, *in.UploadId)

	return &s3.AbortMultipartUploadOutput{}, nil
}

func (f *fakeS3) HeadObjectWithContext(ctx aws.Context, in *s3.HeadObjectInput, _ ...request.Option,
) (*s3.HeadObjectOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "head", 0); err != nil {
		return nil, err
	}

	// A directory is no object, and its key is NotFound.
	data, err := f.store.GetContent(ctx, *in.Key)
	if err != nil {
		return nil, failure("NotFound", 404)
	}

	f.mu.Lock()
	etag, ok := f.etags[*in.Key]
	f.mu.Unlock()

	// An object not put together from parts, as a PutObject writes it.
	if !ok {
		etag = fmt.Sprintf("%x", md5.Sum(data)) //nolint:gosec
	}

	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(data))), ETag: aws.String(`"` + etag + `"`)}, nil
}

func (f *fakeS3) GetObjectWithContext(ctx aws.Context, in *s3.GetObjectInput, _ ...request.Option,
) (*s3.GetObjectOutput, error) {
	defer f.end()

	if err := f.begin(ctx, "get", 0); err != nil {
		return nil, err
	}

	data, err := f.store.GetContent(ctx, *in.Key)
	if err != nil {
		return nil, failure("NoSuchKey", 404)
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

// ListObjectsV2WithContext answers a listing of at most one key: the key equal to the prefix, or
// else one under it.
func (f *fakeS3) ListObjectsV2WithContext(ctx aws.Context, in *s3.ListObjectsV2Input, _ ...request.Option,
) (*s3.ListObjectsV2Output, error) {
	defer f.end()

	if err := f.begin(ctx, "list", 0); err != nil {
		return nil, err
	}

	out := &s3.ListObjectsV2Output{}

	info, err := f.store.Stat(ctx, *in.Prefix)
	if err != nil {
		return out, nil //nolint:nilerr // nothing has that prefix
	}

	if !info.IsDir() {
		out.Contents = []*s3.Object{{Key: in.Prefix, Size: aws.Int64(info.Size()), LastModified: aws.Time(info.ModTime())}}

		return out, nil
	}

	if children, err := f.store.List(ctx, *in.Prefix); err == nil && len(children) > 0 {
		out.Contents = []*s3.Object{{Key: aws.String(children[0])}}
	}

	return out, nil
}

func (f *fakeS3) open() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.uploads)
}

func shortDeadlines(t *testing.T) {
	t.Helper()

	part, copied, complete, abort, read := partPolicy, copyPolicy, completePolicy, abortPolicy, readPolicy
	short := policy{first: 100 * time.Millisecond, last: 100 * time.Millisecond, patience: 10 * time.Second}
	partPolicy, copyPolicy, completePolicy, abortPolicy, readPolicy = short, short, short, short, short

	t.Cleanup(func() {
		partPolicy, copyPolicy, completePolicy, abortPolicy, readPolicy = part, copied, complete, abort, read
	})
}

func random(t *testing.T, size int) []byte {
	t.Helper()

	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	return data
}

func newTestParts(fake *fakeS3) *parts {
	return newPartsWith(fake, "bucket", func(path string) string { return path })
}

func content(t *testing.T, fake *fakeS3, path string) []byte {
	t.Helper()

	data, err := fake.store.GetContent(context.Background(), path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}

	return data
}

func absent(t *testing.T, fake *fakeS3, path string) {
	t.Helper()

	if _, err := fake.store.Stat(context.Background(), path); err == nil {
		t.Fatalf("%s is visible", path)
	}
}

func TestPartsUploadInParallelAndPublishOnComplete(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	// Each part waits for others to start, so that parts in flight together show.
	fake.hook = func(ctx context.Context, op string, _ int64, _ int) error {
		if op == "part" {
			time.Sleep(20 * time.Millisecond)
		}

		return nil
	}
	data := random(t, 5*partSize+partSize/2)

	upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	absent(t, fake, "/blob")

	if upload.Size() != int64(len(data)) {
		t.Fatalf("size %d, want %d", upload.Size(), len(data))
	}

	if err := upload.Complete(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content(t, fake, "/blob"), data) {
		t.Fatal("published blob differs")
	}

	if fake.count("part") != 6 || fake.most < 2 || fake.most > uploadWorkers {
		t.Fatalf("%d parts, %d at most in flight", fake.count("part"), fake.most)
	}
}

func TestPartsHeldPartIsSentAgain(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, number int64, attempt int) error {
		if op == "part" && number == 3 && attempt == 1 {
			return held(ctx)
		}

		if op == "part" && number == 4 && attempt == 1 {
			return failure("ServiceUnavailable", 504)
		}

		return nil
	}
	data := random(t, 4*partSize)

	upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if err := upload.Complete(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content(t, fake, "/blob"), data) {
		t.Fatal("published blob differs")
	}

	if fake.attempt("part", 3) != 2 || fake.attempt("part", 4) != 2 || fake.attempt("part", 1) != 1 {
		t.Fatalf("attempts: part 1 %d, part 3 %d, part 4 %d",
			fake.attempt("part", 1), fake.attempt("part", 3), fake.attempt("part", 4))
	}
}

func TestPartsRefusedPartAbortsTheUpload(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, number int64, _ int) error {
		if op == "part" && number == 2 {
			return failure("AccessDenied", 403)
		}

		return nil
	}

	_, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(random(t, 3*partSize)))
	if err == nil {
		t.Fatal("upload succeeded")
	}

	if fake.attempt("part", 2) != 1 || fake.count("abort") != 1 || fake.open() != 0 {
		t.Fatalf("%d attempts of the refused part, %d aborts, %d uploads open",
			fake.attempt("part", 2), fake.count("abort"), fake.open())
	}

	absent(t, fake, "/blob")
}

func TestPartsGiveUpAfterPatience(t *testing.T) {
	shortDeadlines(t)

	partPolicy = policy{first: 40 * time.Millisecond, last: 100 * time.Millisecond, patience: time.Second}
	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, number int64, _ int) error {
		if op == "part" && number == 1 {
			return held(ctx)
		}

		return nil
	}

	start := time.Now()

	_, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(random(t, 2*partSize)))
	if err == nil || !strings.Contains(err.Error(), "upload part 1 of /blob: ") || fake.open() != 0 {
		t.Fatalf("err %v, %d uploads open", err, fake.open())
	}

	// 40, 60, 90 ms, then 100 ms an attempt until a second has passed.
	if attempts := fake.attempt("part", 1); attempts < 8 || attempts > 13 || time.Since(start) > 3*time.Second {
		t.Fatalf("%d attempts in %s", attempts, time.Since(start))
	}
}

// A part the store only takes long over gets through once its deadline has grown enough.
func TestPartsDeadlineGrowsAfterAHeldAttempt(t *testing.T) {
	shortDeadlines(t)

	partPolicy = policy{first: 40 * time.Millisecond, last: time.Second, patience: 10 * time.Second}
	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, number int64, _ int) error {
		if deadline, _ := ctx.Deadline(); op == "part" && number == 1 && time.Until(deadline) < 100*time.Millisecond {
			return held(ctx)
		}

		return nil
	}
	data := random(t, 2*partSize)

	upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
	if err == nil {
		err = upload.Complete()
	}

	// 40, 60 and 90 ms are too short; 135 ms is enough.
	if err != nil || !bytes.Equal(content(t, fake, "/blob"), data) || fake.attempt("part", 1) != 4 {
		t.Fatalf("err %v after %d attempts", err, fake.attempt("part", 1))
	}
}

// A blob that fits in one part is still a multipart upload: the fake has no PutObject.
func TestPartsSmallBlobIsOnePart(t *testing.T) {
	for _, size := range []int{0, 1 << 20, partSize - 1, partSize} {
		fake := newFakeS3()
		data := random(t, size)

		upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}

		absent(t, fake, "/blob")

		if err := upload.Complete(); err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(content(t, fake, "/blob"), data) || fake.count("create") != 1 || fake.count("part") != 1 {
			t.Fatalf("size %d: %d uploads created, %d parts", size, fake.count("create"), fake.count("part"))
		}
	}
}

func TestPartsBuffersAreShared(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, _ int64, _ int) error {
		if op == "part" {
			time.Sleep(10 * time.Millisecond)
		}

		return nil
	}
	p := newTestParts(fake)

	blobs := [][]byte{random(t, 4*partSize), random(t, 4*partSize), random(t, 4*partSize)}
	errs := make([]error, len(blobs))

	var uploads sync.WaitGroup

	for i, data := range blobs {
		uploads.Go(func() {
			upload, err := p.upload(context.Background(), fmt.Sprintf("/blob%d", i), bytes.NewReader(data))
			if err == nil {
				err = upload.Complete()
			}

			errs[i] = err
		})
	}

	uploads.Wait()

	for i, data := range blobs {
		if errs[i] != nil || !bytes.Equal(content(t, fake, fmt.Sprintf("/blob%d", i)), data) {
			t.Fatalf("blob %d: %v", i, errs[i])
		}
	}

	if p.made > partBuffers || fake.most > partBuffers {
		t.Fatalf("%d buffers made, %d parts in flight", p.made, fake.most)
	}
}

func TestPartsCopyOnTheStoreSide(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, number int64, attempt int) error {
		if op == "copy" && number == 2 && attempt == 1 {
			return held(ctx)
		}

		return nil
	}
	data := random(t, 3*partSize+partSize/4)

	if err := fake.store.PutContent(context.Background(), "/src", data); err != nil {
		t.Fatal(err)
	}

	p := newTestParts(fake)

	upload, err := p.copy(context.Background(), "/src", "/dst", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	absent(t, fake, "/dst")

	if err := upload.Complete(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content(t, fake, "/dst"), data) || fake.count("copy") != 5 || fake.attempt("copy", 2) != 2 {
		t.Fatalf("%d part copies, %d attempts of part 2", fake.count("copy"), fake.attempt("copy", 2))
	}

	// A blob that fits in one part, even an empty one, is copied as one part of the whole source:
	// the fake has no CopyObject.
	for i, size := range []int{100, 0} {
		small := random(t, size)
		if err := fake.store.PutContent(context.Background(), "/small", small); err != nil {
			t.Fatal(err)
		}

		upload, err = p.copy(context.Background(), "/small", "/small-copy", int64(len(small)))
		if err == nil {
			err = upload.Complete()
		}

		if err != nil || !bytes.Equal(content(t, fake, "/small-copy"), small) || fake.count("copy") != 6+i {
			t.Fatalf("copy of %d bytes: %v, %d part copies", size, err, fake.count("copy"))
		}
	}
}

func TestPartsCompleteCutAfterItCompleted(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	data := random(t, 2*partSize)

	upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// The first completion lands, but its answer is held past the deadline, and the store refuses
	// the next while it is still completing the first.
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		switch {
		case op == "complete" && attempt == 1:
			up, _ := upload.(*partsUpload)
			if err := fake.finish(up.id, up.done); err != nil {
				return err
			}

			return held(ctx)
		case op == "complete" && attempt == 2:
			return failure("InternalError", 500)
		}

		return nil
	}

	if err := upload.Complete(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content(t, fake, "/blob"), data) || fake.attempt("complete", 0) != 3 {
		t.Fatalf("%d completions", fake.attempt("complete", 0))
	}
}

// A cut completion followed by an answer that the upload is gone is taken for a completion only when
// the key holds this upload, not an older object of the same size.
func TestPartsCompleteKnowsAnOlderObject(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	older := random(t, 1000)

	if err := fake.store.PutContent(context.Background(), "/index.json", older); err != nil {
		t.Fatal(err)
	}

	upload, err := newTestParts(fake).upload(context.Background(), "/index.json", bytes.NewReader(random(t, 1000)))
	if err != nil {
		t.Fatal(err)
	}

	// The first completion is held and never lands, and the store has dropped the upload by the next.
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		switch {
		case op == "complete" && attempt == 1:
			return held(ctx)
		case op == "complete":
			return failure(s3.ErrCodeNoSuchUpload, 404)
		}

		return nil
	}

	if err := upload.Complete(); !isCode(err, s3.ErrCodeNoSuchUpload) {
		t.Fatalf("completion over an older object: %v", err)
	}

	if !bytes.Equal(content(t, fake, "/index.json"), older) {
		t.Fatal("older object changed")
	}
}

// settled is whether every upload not completed has been aborted, which for one left behind
// happens meanwhile.
func settled(fake *fakeS3) bool {
	for range 100 {
		if fake.open() == 0 {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return false
}

// A store holds every attempt at one part number of an upload as long as the first, so a held only
// part goes again in a new upload, and the old upload is aborted.
func TestPartsHeldOnlyPartGoesInANewUpload(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		if op == "part" && attempt <= 2 {
			return held(ctx)
		}

		return nil
	}
	data := random(t, 1000)

	upload, err := newTestParts(fake).upload(context.Background(), "/blob", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if err := upload.Complete(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content(t, fake, "/blob"), data) || upload.Size() != int64(len(data)) ||
		fake.count("create") != 3 || fake.count("part") != 3 || !settled(fake) || fake.count("abort") != 2 {
		t.Fatalf("%d uploads created, %d part attempts, %d aborted, %d open",
			fake.count("create"), fake.count("part"), fake.count("abort"), fake.open())
	}
}

// A Complete of one part has a part's deadline, not the minutes of one that puts many together.
func TestPartsCompleteOfOnePartHasAPartsDeadline(t *testing.T) {
	shortDeadlines(t)

	completePolicy = policy{first: time.Minute, last: time.Minute, patience: time.Minute}
	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		if op == "complete" && attempt == 1 {
			return held(ctx)
		}

		return nil
	}

	start := time.Now()
	if err := newTestParts(fake).put(context.Background(), "/index.json", random(t, 1000)); err != nil {
		t.Fatal(err)
	}

	if took := time.Since(start); took > 10*time.Second || fake.attempt("complete", 0) != 2 {
		t.Fatalf("%d completions in %s", fake.attempt("complete", 0), took)
	}
}

// Small reads and stats have deadlines, and say what the upstream driver says of a file, a
// directory and a missing path.
func TestPartsReadAndStat(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	driver := &partsDriver{Driver: New(fake.store), parts: newTestParts(fake)}
	data := random(t, 1000)

	if err := fake.store.PutContent(context.Background(), "/repo/index.json", data); err != nil {
		t.Fatal(err)
	}

	// Each request is held at its first attempt.
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		if attempt == 1 {
			return held(ctx)
		}

		return nil
	}

	if got, err := driver.ReadFile("/repo/index.json"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read: %v", err)
	}

	if info, err := driver.Stat("/repo/index.json"); err != nil || info.IsDir() || info.Size() != int64(len(data)) ||
		info.Path() != "/repo/index.json" {
		t.Fatalf("stat of a file: %+v, %v", info, err)
	}

	if info, err := driver.Stat("/repo"); err != nil || !info.IsDir() || !driver.DirExists("/repo") {
		t.Fatalf("stat of a directory: %+v, %v", info, err)
	}

	if fake.attempt("get", 0) != 2 || fake.attempt("head", 0) < 2 || fake.attempt("list", 0) < 2 {
		t.Fatalf("%d gets, %d heads, %d lists", fake.attempt("get", 0), fake.attempt("head", 0), fake.attempt("list", 0))
	}

	if _, err := driver.ReadFile("/repo/missing"); !errclass.IsStorageObjectMissing(err) {
		t.Fatalf("read of a missing file: %v", err)
	}

	if _, err := driver.Stat("/repo/missing"); !errclass.IsStorageObjectMissing(err) || driver.DirExists("/repo/missing") {
		t.Fatalf("stat of a missing file: %v", err)
	}
}

// A stat the store keeps failing fails, and does not take the path for missing, so zot does not
// write a new index.json over the one there.
func TestPartsFailedStatIsNotMissing(t *testing.T) {
	shortDeadlines(t)

	readPolicy = policy{first: 50 * time.Millisecond, last: 50 * time.Millisecond, patience: 300 * time.Millisecond}
	fake := newFakeS3()
	store := newPartsStore(t, fake)
	ctx := context.Background()

	if err := store.InitRepo(ctx, "repo"); err != nil {
		t.Fatal(err)
	}

	index := []byte(`{"schemaVersion":2,"manifests":[{"digest":"sha256:0"}]}`)
	if err := fake.store.PutContent(ctx, "/zot/repo/index.json", index); err != nil {
		t.Fatal(err)
	}

	fake.hook = func(_ context.Context, op string, _ int64, _ int) error {
		if op == "head" || op == "list" {
			return failure("InternalError", 500)
		}

		return nil
	}
	lists := fake.count("list")

	err := store.InitRepo(ctx, "repo")
	if err == nil || errclass.IsStorageObjectMissing(err) || fake.count("list") != lists {
		t.Fatalf("init over a failing stat: %v, %d lists", err, fake.count("list")-lists)
	}

	if !bytes.Equal(content(t, fake, "/zot/repo/index.json"), index) {
		t.Fatal("index.json written over")
	}
}

// A file written whole from memory, as zot writes manifests and index.json, goes as parts sent
// from that memory, with deadlines, and is completed: the fake has no PutObject.
func TestPartsPutFromMemory(t *testing.T) {
	shortDeadlines(t)

	for _, size := range []int{0, 1000, partSize, 2*partSize + 1} {
		fake := newFakeS3()
		fake.hook = func(ctx context.Context, op string, number int64, attempt int) error {
			if op == "part" && number == 1 && attempt == 1 {
				return held(ctx)
			}

			return nil
		}
		p := newTestParts(fake)
		data := random(t, size)

		if err := p.put(context.Background(), "/index.json", data); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}

		// The only part of an upload is sent again in a new one.
		parts := max(1, (size+partSize-1)/partSize)
		creates := map[bool]int{true: 2, false: 1}[parts == 1]
		if !bytes.Equal(content(t, fake, "/index.json"), data) || fake.count("create") != creates ||
			fake.count("part") != parts+1 || fake.attempt("part", 1) != 2 || !settled(fake) || p.made != 0 {
			t.Fatalf("size %d: %d uploads created, %d part attempts, %d of part 1, %d open, %d buffers made",
				size, fake.count("create"), fake.count("part"), fake.attempt("part", 1), fake.open(), p.made)
		}
	}

	// A refused completion aborts the upload and leaves the key as it was.
	fake := newFakeS3()
	fake.hook = func(ctx context.Context, op string, _ int64, _ int) error {
		if op == "complete" {
			return failure("AccessDenied", 403)
		}

		return nil
	}

	if err := newTestParts(fake).put(context.Background(), "/index.json", random(t, 1000)); err == nil {
		t.Fatal("put succeeded")
	}

	if fake.count("abort") != 1 || fake.open() != 0 {
		t.Fatalf("%d aborts, %d uploads open", fake.count("abort"), fake.open())
	}

	absent(t, fake, "/index.json")
}

// A store that speaks HTTP/2 picks it when the client offers it, so the client must not offer it.
func TestPartsTransportSpeaksHTTP1(t *testing.T) {
	var (
		mu      sync.Mutex
		offered []string
	)

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{ //nolint:gosec // a test server
		NextProtos: []string{"h2", "http/1.1"},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			mu.Lock()
			offered = hello.SupportedProtos
			mu.Unlock()

			return nil, nil //nolint:nilnil // the server's own configuration
		},
	}
	server.StartTLS()
	defer server.Close()

	ask := func(client *http.Client) (string, []string) {
		t.Helper()

		resp, err := client.Get(server.URL) //nolint:noctx
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		mu.Lock()
		defer mu.Unlock()

		return string(body), offered
	}

	if proto, _ := ask(server.Client()); proto != "HTTP/2.0" {
		t.Fatalf("the server speaks %s", proto)
	}

	transport := partsTransport(false)
	trusted, _ := server.Client().Transport.(*http.Transport)
	transport.TLSClientConfig.RootCAs = trusted.TLSClientConfig.RootCAs

	proto, protos := ask(&http.Client{Transport: transport})
	if proto != "HTTP/1.1" || slices.Contains(protos, "h2") {
		t.Fatalf("parts went over %s, offering %v", proto, protos)
	}
}

func newPartsStore(t *testing.T, fake *fakeS3) storageTypes.ImageStore {
	t.Helper()

	driver := &partsDriver{Driver: New(fake.store), parts: newTestParts(fake)}

	return imagestore.NewImageStore("/zot", "", false, false, log.NewTestLogger(), monitoring.NewNopMetricServer(),
		nil, driver, nil, nil, nil)
}

func TestImageStoreUploadsAndCopiesInParts(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	store := newPartsStore(t, fake)
	data := random(t, 2*partSize+1)
	digest := godigest.FromBytes(data)
	ctx := context.Background()

	if _, _, err := store.FullBlobUpload(ctx, "repo", bytes.NewReader(data), godigest.FromString("other")); !errors.Is(err, zerr.ErrBadBlobDigest) {
		t.Fatalf("wrong digest: %v", err)
	}

	if fake.open() != 0 || fake.count("complete") != 0 {
		t.Fatalf("after a wrong digest: %d uploads open, %d completed", fake.open(), fake.count("complete"))
	}

	_, size, err := store.FullBlobUpload(ctx, "repo", bytes.NewReader(data), digest)
	if err != nil || size != int64(len(data)) {
		t.Fatalf("upload: %d, %v", size, err)
	}

	if ok, size, _, err := store.StatBlob("repo", digest); !ok || size != int64(len(data)) || err != nil {
		t.Fatalf("stat: %v %d %v", ok, size, err)
	}

	if uploads, _ := fake.store.List(ctx, "/zot/repo/.uploads"); len(uploads) != 0 {
		t.Fatalf("staged uploads left: %v", uploads)
	}

	copier, ok := store.(storageTypes.BlobCopier)
	if !ok {
		t.Fatal("image store does not copy")
	}

	size, err = copier.CopyBlob(ctx, "repo", "other", digest)
	if err != nil || size != int64(len(data)) || fake.count("copy") != 3 {
		t.Fatalf("copy: %d, %v, %d part copies", size, err, fake.count("copy"))
	}

	reader, _, err := store.GetBlob("other", digest, "application/vnd.oci.image.layer.v1.tar")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	copied, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(copied, data) {
		t.Fatalf("copied blob differs: %v", err)
	}

	if _, err := copier.CopyBlob(ctx, "repo", "third", godigest.FromString("missing")); !errors.Is(err, zerr.ErrBlobNotFound) {
		t.Fatalf("missing source: %v", err)
	}
}

// A manifest and the index.json that tags it are written in parts with deadlines, index.json
// straight to its key with no staged copy.
func TestImageStoreWritesMetadataInParts(t *testing.T) {
	shortDeadlines(t)

	fake := newFakeS3()
	store := newPartsStore(t, fake)
	ctx := context.Background()

	config, err := json.Marshal(ispec.Image{
		Platform: ispec.Platform{Architecture: "amd64", OS: "linux"},
		RootFS:   ispec.RootFS{Type: "layers", DiffIDs: []godigest.Digest{}},
	})
	if err != nil {
		t.Fatal(err)
	}

	layer := random(t, 1000)

	for _, blob := range [][]byte{config, layer} {
		if _, _, err := store.FullBlobUpload(ctx, "repo", bytes.NewReader(blob), godigest.FromBytes(blob)); err != nil {
			t.Fatal(err)
		}
	}

	manifest, err := json.Marshal(ispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ispec.MediaTypeImageManifest,
		Config: ispec.Descriptor{
			MediaType: ispec.MediaTypeImageConfig, Digest: godigest.FromBytes(config), Size: int64(len(config)),
		},
		Layers: []ispec.Descriptor{{
			MediaType: ispec.MediaTypeImageLayer, Digest: godigest.FromBytes(layer), Size: int64(len(layer)),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Every part is held at its first attempt.
	fake.hook = func(ctx context.Context, op string, _ int64, attempt int) error {
		if op == "part" && attempt%2 == 1 {
			return held(ctx)
		}

		return nil
	}
	completed, parts := fake.count("complete"), fake.count("part")

	digest, _, err := store.PutImageManifest(ctx, "repo", "v1", ispec.MediaTypeImageManifest, manifest, nil)
	if err != nil || digest != godigest.FromBytes(manifest) {
		t.Fatalf("put manifest: %s, %v", digest, err)
	}

	// The manifest and index.json, each held once.
	if fake.count("complete")-completed != 2 || fake.count("part")-parts != 4 {
		t.Fatalf("%d uploads completed, %d part attempts", fake.count("complete")-completed, fake.count("part")-parts)
	}

	if !bytes.Equal(content(t, fake, "/zot/repo/blobs/sha256/"+digest.Encoded()), manifest) {
		t.Fatal("manifest differs")
	}

	var index ispec.Index
	if err := json.Unmarshal(content(t, fake, "/zot/repo/index.json"), &index); err != nil {
		t.Fatal(err)
	}

	if len(index.Manifests) != 1 || index.Manifests[0].Digest != digest ||
		index.Manifests[0].Annotations[ispec.AnnotationRefName] != "v1" {
		t.Fatalf("index.json: %+v", index.Manifests)
	}

	if uploads, _ := fake.store.List(ctx, "/zot/repo/.uploads"); len(uploads) != 0 {
		t.Fatalf("staged files left: %v", uploads)
	}
}
