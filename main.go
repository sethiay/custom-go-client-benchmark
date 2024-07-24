package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.opencensus.io/stats"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	// Install google-c2p resolver, which is required for direct path.
	_ "google.golang.org/grpc/balancer/rls"
	_ "google.golang.org/grpc/xds/googledirectpath"
)

var (
	GrpcConnPoolSize    = 1
	MaxConnsPerHost     = 100
	MaxIdleConnsPerHost = 100

	MB = int64(1024 * 1024)

	NumOfWorker = flag.Int("worker", 96, "Number of concurrent worker to read")

	ReadSizePerWorker = flag.Int64("read-size-per-worker", 50*MB, "Size of read call per worker")

	MaxRetryDuration = 30 * time.Second

	RetryMultiplier = 2.0

	BucketName = flag.String("bucket", "ayushsethi-parallel-downloads", "GCS bucket name.")

	ProjectName = flag.String("project", "gcs-fuse-test", "GCP project name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	ObjectName = "1000G/fio/Workload.0/0"

	tracerName      = "ayushsethi-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")

	eG errgroup.Group
)

func CreateHttpClient(ctx context.Context, isHttp2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if isHttp2 == false {
		transport = &http.Transport{
			MaxConnsPerHost:     MaxConnsPerHost,
			MaxIdleConnsPerHost: MaxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   MaxConnsPerHost,
			ForceAttemptHTTP2: true,
		}
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSource,
		},
		Timeout: 0,
	}

	// Setting UserAgent through RoundTripper middleware
	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: "prince",
	}

	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	if err := os.Setenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS", "true"); err != nil {
		log.Fatalf("error setting direct path env var: %v", err)
	}

	client, err = storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(GrpcConnPoolSize))

	if err := os.Unsetenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS"); err != nil {
		log.Fatalf("error while unsetting direct path env var: %v", err)
	}
	return
}

func RangeReadObject(ctx context.Context, workerId int, bucketHandle *storage.BucketHandle, rangeStart int64, rangeEnd int64) (err error) {

	objectName := ObjectName

	var span trace.Span
	traceCtx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, "RangeReadObject")
	span.SetAttributes(
		attribute.KeyValue{"bucket", attribute.StringValue(*BucketName)},
	)
	start := time.Now()
	object := bucketHandle.Object(objectName)
	rc, err := object.NewRangeReader(traceCtx, rangeStart, rangeEnd-rangeStart)
	if err != nil {
		return fmt.Errorf("while creating reader object: %v", err)
	}

	// Calls Reader.WriteTo implicitly.
	_, err = io.Copy(io.Discard, rc)
	if err != nil {
		return fmt.Errorf("while reading and discarding content: %v", err)
	}

	duration := time.Since(start)
	stats.Record(ctx, readLatency.M(float64(duration.Milliseconds())))

	err = rc.Close()
	span.End()
	if err != nil {
		return fmt.Errorf("while closing the reader object: %v", err)
	}

	return
}

func main() {
	flag.Parse()
	ctx := context.Background()

	if *enableTracing {
		cleanup := enableTraceExport(ctx, *traceSampleRate)
		defer cleanup()
	}

	var client *storage.Client
	var err error
	if *clientProtocol == "http" {
		client, err = CreateHttpClient(ctx, false)
	} else {
		client, err = CreateGrpcClient(ctx)
	}

	if err != nil {
		fmt.Errorf("while creating the client: %v", err)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        MaxRetryDuration,
			Multiplier: RetryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*BucketName)

	// Enable stack-driver exporter.
	registerLatencyView()

	err = enableSDExporter()
	if err != nil {
		fmt.Fprintf(os.Stderr, "while enabling stackdriver exporter: %v", err)
		os.Exit(1)
	}
	defer closeSDExporter()

	objectStat, err := bucketHandle.Object(ObjectName).Attrs(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "while getting stat of object: %v", err)
		os.Exit(1)
	}

	startTime := time.Now()
	var objectLenRead int64
	var totalWaitingTimeByAllWorkers, totalReadTimeByAllWorkers int64
	for objectLenRead < objectStat.Size {
		// Run the actual workload
		var i int
		readLatencies := make([]int64, int64(*NumOfWorker))
		for i = 0; i < *NumOfWorker; i++ {
			var start int64 = objectLenRead + int64(i)**ReadSizePerWorker
			var end int64 = min(start+*ReadSizePerWorker, objectStat.Size)
			if start == end {
				break
			}
			idx := i
			eG.Go(func() error {
				// record latencies
				readStartTime := time.Now()
				defer func() {
					readLatencies[idx] = time.Since(readStartTime).Microseconds()
				}()

				err = RangeReadObject(ctx, idx, bucketHandle, start, end)
				if err != nil {
					err = fmt.Errorf("while reading object %v: %w", ObjectName+strconv.Itoa(idx), err)
					return err
				}
				return err
			})
		}
		err = eG.Wait()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
			os.Exit(1)
		}
		objectLenRead = min(objectLenRead+int64(*NumOfWorker)**ReadSizePerWorker, objectStat.Size)

		var minLatency, maxLatency, totalReadLatency int64 = math.MaxInt64, math.MinInt64, 0
		for j := 0; j < i; j++ {
			readLat := readLatencies[j]
			minLatency = min(minLatency, readLat)
			maxLatency = max(maxLatency, readLat)
			totalReadLatency = totalReadLatency + readLat
		}
		totalWaitingTimeByAllWorkers = totalWaitingTimeByAllWorkers + int64(i)*maxLatency - totalReadLatency
		totalReadTimeByAllWorkers = totalReadTimeByAllWorkers + totalReadLatency
	}
	duration := time.Since(startTime)
	fmt.Println(fmt.Sprintf("Read benchmark completed successfully in %v", duration.Seconds()))
	fmt.Println(fmt.Sprintf("The total read latencies across all goroutines is %v and total waiting times across all goroutines is %v",
		totalReadTimeByAllWorkers, totalWaitingTimeByAllWorkers))
}
