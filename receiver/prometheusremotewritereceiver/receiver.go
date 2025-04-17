// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	promv1 "github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
)

func newRemoteWriteReceiver(settings receiver.Settings, cfg *Config, nextConsumer consumer.Metrics) (receiver.Metrics, error) {
	return &prometheusRemoteWriteReceiver{
		settings:     settings,
		nextConsumer: nextConsumer,
		config:       cfg,
		server: &http.Server{
			ReadTimeout: 60 * time.Second,
		},
	}, nil
}

type prometheusRemoteWriteReceiver struct {
	settings     receiver.Settings
	nextConsumer consumer.Metrics

	config *Config
	server *http.Server
	wg     sync.WaitGroup
}

// MetricIdentity contains all the components that uniquely identify a metric
// according to the OpenTelemetry Protocol data model.
// The definition of the metric uniqueness is based on the following document. Ref: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#opentelemetry-protocol-data-model
type MetricIdentity struct {
	ResourceID   string
	ScopeName    string
	ScopeVersion string
	MetricName   string
	Unit         string
	Type         writev2.Metadata_MetricType
}
type CustomJSONMetric struct {
	Labels    map[string]string `json:"labels"`
	Name      string            `json:"name"`
	Timestamp string            `json:"timestamp"`
	Value     float64           `json:"value"`
	Type      string
}

func ConvertCustomMetricsToOTLP(metric CustomJSONMetric) (pmetric.Metrics, error) {
	md := pmetric.NewMetrics() // Create a new Metrics object

	// Create ResourceMetrics (this will hold resource-level attributes)
	resourceMetrics := md.ResourceMetrics().AppendEmpty()
	resourceAttributes := resourceMetrics.Resource().Attributes()
	resourceAttributes.PutStr("service.name", "custom-metrics-service") // Example attribute

	// Create Metric and set its properties directly in ResourceMetrics
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	timestamp, err := time.Parse(time.RFC3339, metric.Timestamp)
	if err != nil {
		return md, fmt.Errorf("invalid timestamp format: %v", err)
	}
	// Create a Metric (Gauge type in this case)
	metricData := scopeMetrics.Metrics().AppendEmpty()
	metricData.SetName(metric.Name)
	switch strings.ToLower(metric.Type) {
	case "gauge":
		metricData.SetEmptyGauge()
		dp := metricData.Gauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(metric.Value)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		for k, v := range metric.Labels {
			dp.Attributes().PutStr(k, v)
		}

	case "counter":
		metricData.SetEmptySum()
		sum := metricData.Sum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetDoubleValue(metric.Value)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		for k, v := range metric.Labels {
			dp.Attributes().PutStr(k, v)
		}

	default:
		return md, fmt.Errorf("unsupported metric type: %s", metric.Type)
	}
	return md, nil
}

/* // convertStringToDouble converts a string value to a double.
func convertStringToDouble(value string) (float64, error) {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to convert value %s to double: %v", value, err)
	}
	return val, nil
} */

// createMetricIdentity creates a MetricIdentity struct from the required components
func createMetricIdentity(resourceID, scopeName, scopeVersion, metricName, unit string, metricType writev2.Metadata_MetricType) MetricIdentity {
	return MetricIdentity{
		ResourceID:   resourceID,
		ScopeName:    scopeName,
		ScopeVersion: scopeVersion,
		MetricName:   metricName,
		Unit:         unit,
		Type:         metricType,
	}
}

// Hash generates a unique hash for the metric identity
func (mi MetricIdentity) Hash() uint64 {
	const separator = "\xff"

	combined := strings.Join([]string{
		mi.ResourceID,
		mi.ScopeName,
		mi.ScopeVersion,
		mi.MetricName,
		mi.Unit,
		fmt.Sprintf("%d", mi.Type),
	}, separator)

	return xxhash.Sum64String(combined)
}

func (prw *prometheusRemoteWriteReceiver) Start(ctx context.Context, host component.Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", prw.handlePRW)

	prw.server = &http.Server{
		Addr:    ":8080", // or any other address you need
		Handler: mux,
	}
	listener, err := prw.config.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to create prometheus remote-write listener: %w", err)
	}

	prw.wg.Add(1)
	go func() {
		defer prw.wg.Done()
		if err := prw.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(fmt.Errorf("error starting prometheus remote-write receiver: %w", err)))
		}
	}()
	return nil
}

func (prw *prometheusRemoteWriteReceiver) Shutdown(ctx context.Context) error {
	if prw.server == nil {
		return nil
	}
	err := prw.server.Shutdown(ctx)
	if err == nil {
		// Only wait if Shutdown returns successfully,
		// otherwise we may block indefinitely.
		prw.wg.Wait()
	}
	return err
}

func (prw *prometheusRemoteWriteReceiver) handlePRW(w http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		prw.settings.Logger.Warn("message received without Content-Type header, rejecting")
		http.Error(w, "Content-Type header is required", http.StatusUnsupportedMediaType)
		return
	}

	msgType, err := prw.parseProto(contentType)
	if err != nil {
		prw.settings.Logger.Warn("Error decoding remote-write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}
	/* if msgType != promconfig.RemoteWriteProtoMsgV2 {
		prw.settings.Logger.Warn("message received with unsupported proto version, rejecting")
		http.Error(w, "Unsupported proto version", http.StatusUnsupportedMediaType)
		return
	}
	*/
	// After parsing the content-type header, the next step would be to handle content-encoding.
	// Luckly confighttp's Server has middleware that already decompress the request body for us.
	var decompressedData []byte

	// First, we read the raw Snappy data into a buffer

	rawData, err := io.ReadAll(req.Body)

	if err != nil {
		prw.settings.Logger.Warn("Error reading request body", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use snappy.Decode to decompress the raw Snappy block
	decompressedData, err = snappy.Decode(nil, rawData)

	if err != nil {
		prw.settings.Logger.Warn("Error decompressing Snappy body", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch msgType {
	case promconfig.RemoteWriteProtoMsgV2:
		var prw2Req writev2.Request
		if err := proto.Unmarshal(decompressedData, &prw2Req); err != nil {
			prw.settings.Logger.Warn("Error decoding remote write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		otelMetrics, stats, err := prw.translateV2(req.Context(), &prw2Req)
		stats.SetHeaders(w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest) // Following instructions at https://prometheus.io/docs/specs/remote_write_spec_2_0/#invalid-samples
			return
		}
		if prw.nextConsumer != nil {
			if err := prw.nextConsumer.ConsumeMetrics(req.Context(), otelMetrics); err != nil {
				prw.settings.Logger.Error("Failed to send metrics to next consumer", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
				http.Error(w, "Failed to process metrics", http.StatusInternalServerError)
				return
			}
		}

	case promconfig.RemoteWriteProtoMsgV1:
		var prw1Req promv1.WriteRequest
		if err := proto.Unmarshal(decompressedData, &prw1Req); err != nil {
			prw.settings.Logger.Warn("Error decoding remote write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		customMetrics, stats, err := prw.translateV1(req.Context(), &prw1Req)
		stats.SetHeaders(w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if prw.nextConsumer != nil {
			for _, metric := range customMetrics {
				otlpMetric, err := ConvertCustomMetricsToOTLP(metric)
				if err != nil {
					prw.settings.Logger.Error("Failed to convert custom metric", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
					http.Error(w, "Failed to convert custom metric", http.StatusInternalServerError)
					return
				}
				// Send each metric to the next consumer (this could be a Kafka exporter or anything else)
				if err := prw.nextConsumer.ConsumeMetrics(req.Context(), otlpMetric); err != nil {
					prw.settings.Logger.Error("Failed to send metrics to next consumer", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
					http.Error(w, "Failed to process metrics", http.StatusInternalServerError)
					return
				}
			}
		}
	default:
		http.Error(w, "Unsupported proto version", http.StatusUnsupportedMediaType)
		return
	}

	fmt.Println("Received Prometheus V1 request successfully and passing on to next consumer ..")
	w.WriteHeader(http.StatusNoContent)
}
func (prw *prometheusRemoteWriteReceiver) translateV1(_ context.Context, req *promv1.WriteRequest) ([]CustomJSONMetric, promremote.WriteResponseStats, error) {
	var customMetrics []CustomJSONMetric
	stats := promremote.WriteResponseStats{}

	for _, ts := range req.Timeseries {
		var metricName string
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metricName = label.Value
				break
			}
		}
		if metricName == "" {
			continue
		}
		metricType := inferMetricType(metricName)

		for _, sample := range ts.Samples {
			tsTime := time.UnixMilli(sample.Timestamp).UTC()
			customMetric := CustomJSONMetric{
				Name:      metricName,
				Value:     sample.Value,
				Labels:    mapLabels(ts.Labels), // Create a map of other labels
				Timestamp: tsTime.Format(time.RFC3339),
				Type:      metricType,
			}

			customMetrics = append(customMetrics, customMetric)
		}
	}

	return customMetrics, stats, nil
}

func inferMetricType(name string) string {
	name = strings.ToLower(name)
	if strings.HasSuffix(name, "_total") || strings.HasSuffix(name, "_count") {
		return "counter"
	}
	return "gauge" // default fallback
}

func mapLabels(labels []promv1.Label) map[string]string {
	labelMap := make(map[string]string)
	for _, label := range labels {
		if label.Name != "__name__" && label.Name != "job" && label.Name != "instance" {
			labelMap[label.Name] = label.Value
		}
	}
	return labelMap
}

// parseProto parses the content-type header and returns the version of the remote-write protocol.
// We can't expect that senders of remote-write v1 will add the "proto=" parameter since it was not
// a requirement in v1. So, if the parameter is not found, we assume v1.
func (prw *prometheusRemoteWriteReceiver) parseProto(contentType string) (promconfig.RemoteWriteProtoMsg, error) {
	contentType = strings.TrimSpace(contentType)

	parts := strings.Split(contentType, ";")
	if parts[0] != "application/x-protobuf" {
		return "", fmt.Errorf("expected %q as the first (media) part, got %v content-type", "application/x-protobuf", contentType)
	}

	for _, part := range parts[1:] {
		parameter := strings.Split(part, "=")
		if len(parameter) != 2 {
			return "", fmt.Errorf("as per https://www.rfc-editor.org/rfc/rfc9110#parameter expected parameters to be key-values, got %v in %v content-type", part, contentType)
		}

		if strings.TrimSpace(parameter[0]) == "proto" {
			ret := promconfig.RemoteWriteProtoMsg(parameter[1])
			if err := ret.Validate(); err != nil {
				return "", fmt.Errorf("got %v content type; %w", contentType, err)
			}
			return ret, nil
		}
	}

	// No "proto=" parameter found, assume v1.
	return promconfig.RemoteWriteProtoMsgV1, nil
}

// translateV2 translates a v2 remote-write request into OTLP metrics.
// translate is not feature complete.
//
//nolint:unparam
func (prw *prometheusRemoteWriteReceiver) translateV2(_ context.Context, req *writev2.Request) (pmetric.Metrics, promremote.WriteResponseStats, error) {
	fmt.Println("Received Request:", req)

	// Or print specific fields of the request for more detailed information
	fmt.Println("Number of TimeSeries:", len(req.Timeseries))
	for i, ts := range req.Timeseries {
		fmt.Printf("TimeSeries %d: %+v\n", i, ts)
	}

	// Further print any other relevant data you want to inspect
	fmt.Println("Symbols: ", req.Symbols)
	var (
		badRequestErrors error
		otelMetrics      = pmetric.NewMetrics()
		labelsBuilder    = labels.NewScratchBuilder(0)
		stats            = promremote.WriteResponseStats{}
		// Prometheus Remote-Write can send multiple time series with the same labels in the same request.
		// Instead of creating a whole new OTLP metric, we just append the new sample to the existing OTLP metric.
		// This cache is called "intra" because in the future we'll have a "interRequestCache" to cache resourceAttributes
		// between requests based on the metric "target_info".
		intraRequestCache = make(map[uint64]pmetric.ResourceMetrics)
		// The key is composed by: resource_hash:scope_name:scope_version:metric_name:unit:type
		metricCache = make(map[uint64]pmetric.Metric)
	)

	for _, ts := range req.Timeseries {
		ls := ts.ToLabels(&labelsBuilder, req.Symbols)
		fmt.Println("Labels:", ls)
		if !ls.Has(labels.MetricName) {
			badRequestErrors = errors.Join(badRequestErrors, fmt.Errorf("missing metric name in labels"))
			continue
		} else if duplicateLabel, hasDuplicate := ls.HasDuplicateLabelNames(); hasDuplicate {
			badRequestErrors = errors.Join(badRequestErrors, fmt.Errorf("duplicate label %q in labels", duplicateLabel))
			continue
		}

		var rm pmetric.ResourceMetrics
		hashedLabels := xxhash.Sum64String(ls.Get("job") + string([]byte{'\xff'}) + ls.Get("instance"))
		intraCacheEntry, ok := intraRequestCache[hashedLabels]
		if ok {
			// We found the same time series in the same request, so we should append to the same OTLP metric.
			rm = intraCacheEntry
		} else {
			rm = otelMetrics.ResourceMetrics().AppendEmpty()
			parseJobAndInstance(rm.Resource().Attributes(), ls.Get("job"), ls.Get("instance"))
			intraRequestCache[hashedLabels] = rm
		}

		scopeName, scopeVersion := prw.extractScopeInfo(ls)
		metricName := ls.Get(labels.MetricName)
		if ts.Metadata.UnitRef >= uint32(len(req.Symbols)) {
			badRequestErrors = errors.Join(badRequestErrors, fmt.Errorf("unit ref %d is out of bounds of symbolsTable", ts.Metadata.UnitRef))
			continue
		}

		if ts.Metadata.HelpRef >= uint32(len(req.Symbols)) {
			badRequestErrors = errors.Join(badRequestErrors, fmt.Errorf("help ref %d is out of bounds of symbolsTable", ts.Metadata.HelpRef))
			continue
		}

		unit := req.Symbols[ts.Metadata.UnitRef]
		description := req.Symbols[ts.Metadata.HelpRef]

		resourceID := identity.OfResource(rm.Resource())

		metricIdentity := createMetricIdentity(
			resourceID.String(), // Resource identity
			scopeName,           // Scope name
			scopeVersion,        // Scope version
			metricName,          // Metric name
			unit,                // Unit
			ts.Metadata.Type,    // Metric type
		)

		metricKey := metricIdentity.Hash()

		var scope pmetric.ScopeMetrics
		var foundScope bool
		for i := 0; i < rm.ScopeMetrics().Len(); i++ {
			s := rm.ScopeMetrics().At(i)
			if s.Scope().Name() == scopeName && s.Scope().Version() == scopeVersion {
				scope = s
				foundScope = true
				break
			}
		}
		if !foundScope {
			scope = rm.ScopeMetrics().AppendEmpty()
			scope.Scope().SetName(scopeName)
			scope.Scope().SetVersion(scopeVersion)
		}

		metric, exists := metricCache[metricKey]
		// If the metric does not exist, we create an empty metric and add it to the cache.
		if !exists {
			fmt.Println("Creating new metric for key:", metricKey)
			metric = scope.Metrics().AppendEmpty()
			metric.SetName(metricName)
			metric.SetUnit(unit)
			metric.SetDescription(description)
			fmt.Println("Type:", ts.Metadata.Type)
			switch ts.Metadata.Type {
			case writev2.Metadata_METRIC_TYPE_GAUGE:
				metric.SetEmptyGauge()
			case writev2.Metadata_METRIC_TYPE_COUNTER:
				sum := metric.SetEmptySum()
				sum.SetIsMonotonic(true)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			case writev2.Metadata_METRIC_TYPE_HISTOGRAM:
				metric.SetEmptyHistogram()
			case writev2.Metadata_METRIC_TYPE_SUMMARY:
				metric.SetEmptySummary()
			default:
				metric.SetEmptyGauge()
			}

			metricCache[metricKey] = metric
		}

		// When the new description is longer than the existing one, we should update the metric description.
		// Reference to this behavior: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#opentelemetry-protocol-data-model-producer-recommendations
		if len(metric.Description()) < len(description) {
			metric.SetDescription(description)
		}

		// Otherwise, we append the samples to the existing metric.
		switch ts.Metadata.Type {
		case writev2.Metadata_METRIC_TYPE_GAUGE:
			addNumberDatapoints(metric.Gauge().DataPoints(), ls, ts)
		case writev2.Metadata_METRIC_TYPE_COUNTER:
			addNumberDatapoints(metric.Sum().DataPoints(), ls, ts)
		case writev2.Metadata_METRIC_TYPE_HISTOGRAM:
			addHistogramDatapoints(metric.Histogram().DataPoints(), ls, ts)
		case writev2.Metadata_METRIC_TYPE_SUMMARY:
			addSummaryDatapoints(metric.Summary().DataPoints(), ls, ts)
		default:
			dp := metric.Gauge().DataPoints().AppendEmpty()
			dp.SetStartTimestamp(pcommon.Timestamp(ts.CreatedTimestamp * int64(time.Millisecond)))
			dp.SetTimestamp(pcommon.Timestamp(ts.Samples[0].Timestamp * int64(time.Millisecond))) // Use the sample's timestamp
			dp.SetDoubleValue(ts.Samples[0].Value)                                                // Set the value of the sample as the gauge value

			// Set the labels as attributes of the datapoint
			attributes := dp.Attributes()
			for _, label := range ls {
				attributes.PutStr(label.Name, label.Value)
			}
		}
	}

	return otelMetrics, stats, badRequestErrors
}

// parseJobAndInstance turns the job and instance labels service resource attributes.
// Following the specification at https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/
func parseJobAndInstance(dest pcommon.Map, job, instance string) {
	if instance != "" {
		dest.PutStr("service.instance.id", instance)
	}
	if job != "" {
		parts := strings.Split(job, "/")
		if len(parts) == 2 {
			dest.PutStr("service.namespace", parts[0])
			dest.PutStr("service.name", parts[1])
			return
		}
		dest.PutStr("service.name", job)
	}
}

// addNumberDatapoints adds the labels to the datapoints attributes.
func addNumberDatapoints(datapoints pmetric.NumberDataPointSlice, ls labels.Labels, ts writev2.TimeSeries) {
	// Add samples from the timeseries
	for _, sample := range ts.Samples {
		dp := datapoints.AppendEmpty()
		dp.SetStartTimestamp(pcommon.Timestamp(ts.CreatedTimestamp * int64(time.Millisecond)))
		// Set timestamp in nanoseconds (Prometheus uses milliseconds)
		dp.SetTimestamp(pcommon.Timestamp(sample.Timestamp * int64(time.Millisecond)))
		dp.SetDoubleValue(sample.Value)

		attributes := dp.Attributes()
		for _, l := range ls {
			if l.Name == "instance" || l.Name == "job" || // Become resource attributes
				l.Name == labels.MetricName || // Becomes metric name
				l.Name == "otel_scope_name" || l.Name == "otel_scope_version" { // Becomes scope name and version
				continue
			}
			attributes.PutStr(l.Name, l.Value)
		}
	}
}

func addSummaryDatapoints(_ pmetric.SummaryDataPointSlice, _ labels.Labels, _ writev2.TimeSeries) {
	// TODO: Implement this function
}

func addHistogramDatapoints(_ pmetric.HistogramDataPointSlice, _ labels.Labels, _ writev2.TimeSeries) {
	// TODO: Implement this function
}

// extractScopeInfo extracts the scope name and version from the labels. If the labels do not contain the scope name/version,
// it will use the default values from the settings.
func (prw *prometheusRemoteWriteReceiver) extractScopeInfo(ls labels.Labels) (string, string) {
	scopeName := prw.settings.BuildInfo.Description
	scopeVersion := prw.settings.BuildInfo.Version

	if sName := ls.Get("otel_scope_name"); sName != "" {
		scopeName = sName
	}

	if sVersion := ls.Get("otel_scope_version"); sVersion != "" {
		scopeVersion = sVersion
	}
	return scopeName, scopeVersion
}
