package org.hypertrace.core.rawspansgrouper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.shared.DataFlowMetrics;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpanToStructuredTraceAvroGroupAggregator implements
    AggregateFunction<RawSpan, List<RawSpan>, StructuredTrace> {

  private double creationTimeSamplingPercent = -1;
  private final String TIME_SAMPLING_PERCENTAGE = "timestamp.sampling.percent";

  public RawSpanToStructuredTraceAvroGroupAggregator(Config config) {
    if (config.getInt(TIME_SAMPLING_PERCENTAGE) > 0 && config.getInt(TIME_SAMPLING_PERCENTAGE) <= 100) {
      this.creationTimeSamplingPercent = config.getDouble(TIME_SAMPLING_PERCENTAGE);
    }
  }

  @Override
  public List<RawSpan> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<RawSpan> add(RawSpan value, List<RawSpan> accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public StructuredTrace getResult(List<RawSpan> accumulator) {
    // These raw spans are by Customer ID and Trace ID.
    // So the raw spans will belong to the same customer and trace
    ByteBuffer traceId = null;
    String customerId = null;
    if (!accumulator.isEmpty()) {
      RawSpan firstSpan = accumulator.get(0);
      traceId = firstSpan.getTraceId();
      customerId = firstSpan.getCustomerId();
    }
    List<RawSpan> rawSpanList = new ArrayList<>();
    for (IndexedRecord r : accumulator) {
      rawSpanList.add((RawSpan) r);
    }

    TimestampRecord timestampRecord = null;
    if (Math.random()*100 <= creationTimeSamplingPercent) {
      timestampRecord = new TimestampRecord();
      timestampRecord.setName(DataFlowMetrics.CREATION_TIME.toString());
      timestampRecord.setTimestamp(System.currentTimeMillis());
    }

    return StructuredTraceBuilder
        .buildStructuredTraceFromRawSpans(rawSpanList, traceId, customerId, timestampRecord);
  }

  @Override
  public List<RawSpan> merge(List<RawSpan> a, List<RawSpan> b) {
    List<RawSpan> res = createAccumulator();
    res.addAll(a);
    res.addAll(b);
    return res;
  }
}
