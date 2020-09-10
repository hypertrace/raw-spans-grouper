package org.hypertrace.core.rawspansgrouper.keyvaluemappers;

import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class RawSpansHolderToStructuredTraceMapper implements
    KeyValueMapper<Windowed<String>, RawSpans, KeyValue<String, StructuredTrace>> {

  double dataflowSamplingPercent = -1;
  private final String DATAFLOW_SAMPLING_PERCENT = "dataflow.metriccollection.sampling.percent";
  static final String TRACE_CREATION_TIME = "trace.creation.time";
  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());


  @Override
  public KeyValue<String, StructuredTrace> apply(Windowed<String> key,
      RawSpans accumulator) {

    // These raw spans are by Customer ID and Trace ID.
    // So the raw spans will belong to the same customer and trace
    ByteBuffer traceId = null;
    String customerId = null;
    if (!accumulator.getRawSpans().isEmpty()) {
      RawSpan firstSpan = accumulator.getRawSpans().get(0);
      traceId = firstSpan.getTraceId();
      customerId = firstSpan.getCustomerId();
    }
    List<RawSpan> rawSpanList = new ArrayList<>();
    for (IndexedRecord r : accumulator.getRawSpans()) {
      rawSpanList.add((RawSpan) r);
    }

    Timestamps timestamps = null;
    if (!accumulator.getRawSpans().isEmpty() && Math.random() * 100 <= dataflowSamplingPercent) {
      long currentTime = System.currentTimeMillis();
      long firstSpanArrivalTime = accumulator.getRawSpans().get(0).getReceivedTimeMillis();
      spansGrouperArrivalLagTimer
          .record(currentTime - firstSpanArrivalTime, TimeUnit.MILLISECONDS);
      Map<String, TimestampRecord> records = new HashMap<>();
      records.put(DataflowMetricUtils.SPAN_ARRIVAL_TIME,
          new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME,
              firstSpanArrivalTime));
      records
          .put(TRACE_CREATION_TIME,
              new TimestampRecord(TRACE_CREATION_TIME, currentTime));
      timestamps = new Timestamps(records);
    }

    StructuredTrace trace = StructuredTraceBuilder
        .buildStructuredTraceFromRawSpans(rawSpanList, traceId, customerId, timestamps);

    return new KeyValue<>(null, trace);
  }
}
