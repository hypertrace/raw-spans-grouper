package org.hypertrace.core.rawspansgrouper;

import org.apache.kafka.streams.kstream.Aggregator;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;

public class RawSpanToStructuredTraceAvroGroupAggregator implements
    Aggregator<String, RawSpan, RawSpans> {

  @Override
  public RawSpans apply(String key, RawSpan value, RawSpans aggregate) {
    aggregate.getRawSpans().add(value);
    return aggregate;
  }
}
