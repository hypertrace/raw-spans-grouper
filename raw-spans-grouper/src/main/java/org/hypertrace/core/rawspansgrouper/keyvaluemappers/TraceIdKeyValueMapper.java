package org.hypertrace.core.rawspansgrouper.keyvaluemappers;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.shared.HexUtils;

/**
 * Extracts a groupBy key from {@link RawSpan}
 */
public class TraceIdKeyValueMapper implements KeyValueMapper<String, RawSpan, String> {

  @Override
  public String apply(String key, RawSpan value) {
    return HexUtils.getHex(value.getTraceId());
  }
}
