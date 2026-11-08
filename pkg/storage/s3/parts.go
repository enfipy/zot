package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"

	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// Some object stores hold a request for up to a minute, or answer it with 504 after a minute,
// while most requests finish in well under a second. The upstream driver writes a blob's parts one
// after another with no deadline, then copies the finished upload to the blob's key, so one held
// request stops the whole blob, and every blob is written twice. A whole blob, whose digest is
// known before it is written, needs neither. Its parts go in parallel straight to the blob's own
// key, every attempt has a deadline after which it is sent again, and the multipart upload, which
// publishes the blob at once, is completed only after the caller has checked the digest.
//
// A blob that fits in one part goes as a multipart upload of one part too, never as a PutObject or
// a CopyObject. A store that held an attempt may fail it minutes later and then remove what the
// key holds, which by then a later attempt wrote: a key put again after its first PutObject was cut
// was there for minutes and then gone. An upload's parts are its own objects until it completes,
// and the store completes an upload once.
const (
	partSize = 8 << 20
	// The part buffers of all uploads together, 192 MiB, made only as they are first needed.
	partBuffers = 24
	// Parts uploaded at once for one blob. A part mostly waits on the store, and a blob is done
	// only when its slowest part is, so more parts in flight make a blob faster.
	uploadWorkers = 16
	// Parts copied at once for one blob, and for all copies together; a copy holds no buffer.
	copyWorkers    = 16
	copiesInFlight = 32
	maxParts       = 10000
	firstWait      = 250 * time.Millisecond
	lastWait       = 8 * time.Second
)

// policy says how long one attempt at a request may take, at first and at most, and how long the
// request is tried before it fails. The deadline grows by half after each attempt the store held, so
// that a store that is only slow still gets the request through, and the request is tried for
// minutes, as a store can hold every request for one key that long, while a failed blob costs its
// whole sync again.
type policy struct {
	first, last, patience time.Duration
}

// Variables for tests.
var (
	// Half of all 8 MiB parts take under a second, and four in five under 5 s.
	partPolicy = policy{first: 5 * time.Second, last: time.Minute, patience: 15 * time.Minute} //nolint:gochecknoglobals
	// A copied part reads its range of the source, and a store that holds the read holds every attempt
	// at that range, so a copied part has as long as the store takes to answer it with 504.
	copyPolicy     = policy{first: 2 * time.Minute, last: 3 * time.Minute, patience: 15 * time.Minute} //nolint:gochecknoglobals
	completePolicy = policy{first: 2 * time.Minute, last: 5 * time.Minute, patience: 15 * time.Minute} //nolint:gochecknoglobals
	// An abort only frees the store's space, which it frees by itself in time.
	abortPolicy = policy{first: 20 * time.Second, last: 20 * time.Second, patience: time.Minute} //nolint:gochecknoglobals
)

var errPartsTooLarge = errors.New("blob has more parts than a multipart upload holds")

type parts struct {
	client  s3iface.S3API
	bucket  string
	key     func(path string) string
	buffers chan []byte
	made    int
	mu      sync.Mutex
	copies  chan struct{}
}

// newParts makes a client from the storage driver's parameters, as the upstream driver does, and
// names keys through the upstream driver, so that both agree on where a path is.
func newParts(store storagedriver.StorageDriver, params map[string]any) (*parts, error) {
	keyed, ok := store.(interface{ S3BucketKey(path string) string })
	if !ok {
		return nil, fmt.Errorf("storage driver %s does not name its keys", store.Name())
	}

	text := func(name string) string {
		if value, ok := params[name]; ok && value != nil {
			return fmt.Sprint(value)
		}

		return ""
	}

	flag := func(name string, fallback bool) (bool, error) {
		if text(name) == "" {
			return fallback, nil
		}

		return strconv.ParseBool(text(name))
	}

	if text("bucket") == "" {
		return nil, errors.New("no bucket parameter provided")
	}

	pathStyle, err := flag("forcepathstyle", false)
	if err != nil {
		return nil, err
	}

	secure, err := flag("secure", true)
	if err != nil {
		return nil, err
	}

	skipVerify, err := flag("skipverify", false)
	if err != nil {
		return nil, err
	}

	region := text("region")
	if region == "" {
		region = "us-east-1"
	}

	config := aws.NewConfig().WithRegion(region).WithS3ForcePathStyle(pathStyle).WithDisableSSL(!secure)
	if endpoint := text("regionendpoint"); endpoint != "" {
		config.WithEndpoint(endpoint)
	}

	if text("accesskey") != "" && text("secretkey") != "" {
		config.WithCredentials(credentials.NewStaticCredentials(text("accesskey"), text("secretkey"), text("sessiontoken")))
	}

	config.WithHTTPClient(&http.Client{Transport: partsTransport(skipVerify)})

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	return newPartsWith(s3.New(sess), text("bucket"), keyed.S3BucketKey), nil
}

// partsTransport gives each request in flight a connection of its own. Over HTTP/2 all requests to
// the store share one connection, and parts the store holds use up that connection's flow-control
// window, which stops every other part with them. It is made afresh, with the default transport's
// settings, as a clone of the default transport keeps offering HTTP/2 to the server, which then
// takes the client's HTTP/1.1 for a broken HTTP/2 greeting and closes the connection.
func partsTransport(skipVerify bool) *http.Transport {
	var protocols http.Protocols

	protocols.SetHTTP1(true)

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: skipVerify}, //nolint:gosec // as the upstream driver
		Protocols:             &protocols,
		MaxIdleConnsPerHost:   partBuffers + copiesInFlight,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

func newPartsWith(client s3iface.S3API, bucket string, key func(string) string) *parts {
	return &parts{
		client:  client,
		bucket:  bucket,
		key:     key,
		buffers: make(chan []byte, partBuffers),
		copies:  make(chan struct{}, copiesInFlight),
	}
}

// buffer waits for one of the part buffers, which are made as they are first needed.
func (p *parts) buffer(ctx context.Context) ([]byte, error) {
	select {
	case buf := <-p.buffers:
		return buf, nil
	default:
	}

	p.mu.Lock()
	if p.made < partBuffers {
		p.made++
		p.mu.Unlock()

		return make([]byte, partSize), nil
	}
	p.mu.Unlock()

	select {
	case buf := <-p.buffers:
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// upload writes body to path's key as parts of a multipart upload that it leaves open. The caller
// completes or aborts it.
func (p *parts) upload(ctx context.Context, path string, body io.Reader) (storageTypes.PartsUpload, error) {
	up := &partsUpload{parts: p, key: p.key(path)}

	first, err := p.buffer(ctx)
	if err != nil {
		return nil, err
	}

	n, err := io.ReadFull(body, first)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = nil
	}

	if err == nil {
		err = up.create(ctx)
	}

	if err != nil {
		p.buffers <- first

		return nil, err
	}

	if err := up.send(ctx, first, first[:n], body); err != nil {
		up.Abort()

		return nil, err
	}

	return up, nil
}

// copy copies the size bytes at src's key to dst's key on the store's side, as parts of a
// multipart upload that it leaves open.
func (p *parts) copy(ctx context.Context, src, dst string, size int64) (storageTypes.PartsUpload, error) {
	up := &partsUpload{parts: p, key: p.key(dst), source: p.bucket + "/" + p.key(src), size: size}
	count := max(1, (size+partSize-1)/partSize)
	if count > maxParts {
		return nil, errPartsTooLarge
	}

	if err := up.create(ctx); err != nil {
		return nil, err
	}

	up.done = make([]*s3.CompletedPart, count)
	numbers := make(chan int64)
	run := newRun(ctx)

	defer run.cancel()

	var workers sync.WaitGroup

	for range copyWorkers {
		workers.Go(func() {
			for number := range numbers {
				first := (number - 1) * partSize
				last := min(first+partSize, size) - 1

				p.copies <- struct{}{}
				what := fmt.Sprintf("copy part %d of %s", number, up.key)
				in := &s3.UploadPartCopyInput{
					Bucket: aws.String(p.bucket), Key: aws.String(up.key), UploadId: aws.String(up.id),
					PartNumber: aws.Int64(number), CopySource: aws.String(up.source),
				}
				// One part is the whole source, which may be empty, and no range names an empty one.
				if count > 1 {
					in.CopySourceRange = aws.String(fmt.Sprintf("bytes=%d-%d", first, last))
				}

				err := try(run.ctx, what, copyPolicy, func(ctx context.Context) error {
					out, err := p.client.UploadPartCopyWithContext(ctx, in)
					if err == nil {
						up.done[number-1] = &s3.CompletedPart{ETag: out.CopyPartResult.ETag, PartNumber: aws.Int64(number)}
					}

					return err
				})
				<-p.copies

				run.fail(err)
			}
		})
	}

	for number := int64(1); number <= count && run.ctx.Err() == nil; number++ {
		select {
		case numbers <- number:
		case <-run.ctx.Done():
		}
	}

	close(numbers)
	workers.Wait()

	if err := run.err(ctx); err != nil {
		up.Abort()

		return nil, err
	}

	return up, nil
}

// run is a set of part requests that stops at its first failure.
type run struct {
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
	once   sync.Once
	first  error
}

func newRun(ctx context.Context) *run {
	r := &run{}
	r.ctx, r.cancel = context.WithCancel(ctx)

	return r
}

func (r *run) fail(err error) {
	if err != nil {
		r.once.Do(func() {
			r.first = err
			r.cancel()
		})
	}
}

// err is the first failure, or the caller's context's error when it ended the run.
func (r *run) err(ctx context.Context) error {
	if r.first != nil {
		return r.first
	}

	return ctx.Err()
}

// partsUpload is a blob in a multipart upload not yet completed.
type partsUpload struct {
	parts  *parts
	key    string
	id     string
	size   int64
	source string

	mu   sync.Mutex
	done []*s3.CompletedPart
}

func (up *partsUpload) Size() int64 {
	return up.size
}

func (up *partsUpload) create(ctx context.Context) error {
	return try(ctx, "start upload of "+up.key, partPolicy, func(ctx context.Context) error {
		out, err := up.parts.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(up.parts.bucket), Key: aws.String(up.key),
			ContentType: aws.String("application/octet-stream"),
		})
		if err == nil {
			up.id = aws.StringValue(out.UploadId)
		}

		return err
	})
}

type part struct {
	number int64
	data   []byte
	buf    []byte
}

// send uploads first and the rest of body as parts, uploadWorkers at a time.
func (up *partsUpload) send(ctx context.Context, first, data []byte, body io.Reader) error {
	p := up.parts
	jobs := make(chan part)
	run := newRun(ctx)

	defer run.cancel()

	var workers sync.WaitGroup

	for range uploadWorkers {
		workers.Go(func() {
			for job := range jobs {
				what := fmt.Sprintf("upload part %d of %s", job.number, up.key)
				err := try(run.ctx, what, partPolicy, func(ctx context.Context) error {
					out, err := p.client.UploadPartWithContext(ctx, &s3.UploadPartInput{
						Bucket: aws.String(p.bucket), Key: aws.String(up.key), UploadId: aws.String(up.id),
						PartNumber: aws.Int64(job.number), Body: bytes.NewReader(job.data),
						ContentLength: aws.Int64(int64(len(job.data))),
					})
					if err == nil {
						up.mu.Lock()
						up.done = append(up.done, &s3.CompletedPart{ETag: out.ETag, PartNumber: aws.Int64(job.number)})
						up.mu.Unlock()
					}

					return err
				})
				p.buffers <- job.buf

				run.fail(err)
			}
		})
	}

	run.fail(up.read(run.ctx, jobs, first, data, body))
	close(jobs)
	workers.Wait()

	if err := run.err(ctx); err != nil {
		return err
	}

	sort.Slice(up.done, func(i, j int) bool { return *up.done[i].PartNumber < *up.done[j].PartNumber })

	return nil
}

// read hands data, read into the buffer first, and the rest of body to the workers, a part at a
// time.
func (up *partsUpload) read(ctx context.Context, jobs chan<- part, first, data []byte, body io.Reader) error {
	p := up.parts
	buf := first

	for number := int64(1); ; number++ {
		if number > maxParts {
			p.buffers <- buf

			return errPartsTooLarge
		}

		select {
		case jobs <- part{number: number, data: data, buf: buf}:
			up.size += int64(len(data))
		case <-ctx.Done():
			p.buffers <- buf

			return ctx.Err()
		}

		var err error
		if buf, err = p.buffer(ctx); err != nil {
			return err
		}

		n, err := io.ReadFull(body, buf)
		if n == 0 || (err != nil && !errors.Is(err, io.ErrUnexpectedEOF)) {
			p.buffers <- buf

			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		data = buf[:n]
	}
}

// Complete publishes the blob at its key.
func (up *partsUpload) Complete() error {
	ctx := context.Background()
	p := up.parts

	attempted := false

	return try(ctx, "complete upload of "+up.key, completePolicy, func(ctx context.Context) error {
		_, err := p.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(p.bucket), Key: aws.String(up.key), UploadId: aws.String(up.id),
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: up.done},
		})
		// An attempt cut at its deadline may have completed the upload, which then is gone.
		if attempted && isCode(err, s3.ErrCodeNoSuchUpload) {
			head, headErr := p.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(p.bucket), Key: aws.String(up.key),
			})
			if headErr == nil && aws.Int64Value(head.ContentLength) == up.size {
				return nil
			}
		}

		attempted = true

		return err
	})
}

// Abort drops the parts uploaded so far.
func (up *partsUpload) Abort() {
	if up.id == "" {
		return
	}

	_ = try(context.Background(), "abort upload of "+up.key, abortPolicy, func(ctx context.Context) error {
		_, err := up.parts.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(up.parts.bucket), Key: aws.String(up.key), UploadId: aws.String(up.id),
		})
		if isCode(err, s3.ErrCodeNoSuchUpload) {
			return nil
		}

		return err
	})
}

// try runs call until it succeeds, fails for good, or has been tried for the policy's patience,
// cutting each attempt at a deadline that grows after each attempt the store held. A held attempt is
// sent again at once; one the store answered with an error is sent again after a wait that doubles,
// as the store may be busy, or may still be completing an upload whose first answer was lost.
func try(ctx context.Context, what string, limits policy, call func(ctx context.Context) error) error {
	start := time.Now()
	deadline, wait := limits.first, firstWait

	for attempts := 1; ; attempts++ {
		attempt, cancel := context.WithTimeout(ctx, deadline)
		err := call(attempt)
		held := attempt.Err() != nil

		cancel()

		if err == nil {
			return nil
		}

		if ctx.Err() != nil || !retryable(err) || time.Since(start) >= limits.patience {
			return fmt.Errorf("%s: %d attempts in %s: %w", what, attempts, time.Since(start).Round(time.Millisecond), err)
		}

		if held {
			deadline = min(deadline+deadline/2, limits.last)

			continue
		}

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return fmt.Errorf("%s: %d attempts in %s: %w", what, attempts, time.Since(start).Round(time.Millisecond), err)
		}

		wait = min(2*wait, lastWait)
	}
}

// retryable is false for an answer that another attempt would get again.
func retryable(err error) bool {
	var failure awserr.RequestFailure
	if errors.As(err, &failure) {
		code := failure.StatusCode()

		return code >= http.StatusInternalServerError || code == http.StatusRequestTimeout ||
			code == http.StatusTooManyRequests
	}

	return true
}

func isCode(err error, code string) bool {
	var awsErr awserr.Error

	return errors.As(err, &awsErr) && awsErr.Code() == code
}
