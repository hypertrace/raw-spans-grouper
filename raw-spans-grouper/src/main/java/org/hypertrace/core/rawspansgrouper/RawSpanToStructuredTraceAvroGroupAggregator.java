package org.hypertrace.core.rawspansgrouper;

import org.apache.kafka.streams.kstream.Aggregator;
import org.hypertrace.core.datamodel.RawSpan;

public class RawSpanToStructuredTraceAvroGroupAggregator implements
    Aggregator<String, RawSpan, RawSpansHolder> {

  @Override
  public RawSpansHolder apply(String key, RawSpan value, RawSpansHolder aggregate) {
    aggregate.getRawSpans().add(value);
    return aggregate;
  }
}
