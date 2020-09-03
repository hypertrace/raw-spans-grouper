package org.hypertrace.core.rawspansgrouper.keyvaluemappers;

import static org.hypertrace.core.rawspansgrouper.keyvaluemappers.RawSpansHolderToStructuredTraceMapper.TRACE_CREATION_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceAvroGroupAggregator;
import org.hypertrace.core.rawspansgrouper.RawSpansHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RawSpansHolderToStructuredTraceMapperTest {

  private RawSpansHolderToStructuredTraceMapper underTest;

  @BeforeEach
  public void setUp() {
    underTest = new RawSpansHolderToStructuredTraceMapper();
  }

  @Test
  public void whenRawSpansAreAggregatedVerifyThatTheStructuredTraceHasThem() {
    RawSpan rawSpan1 = mock(RawSpan.class);
    RawSpansHolder rawSpansHolder = RawSpansHolder.newBuilder().build();
    RawSpanToStructuredTraceAvroGroupAggregator aggregator =
        new RawSpanToStructuredTraceAvroGroupAggregator();
    aggregator.apply(null, rawSpan1, rawSpansHolder);

    ByteBuffer buffer = mock(ByteBuffer.class);
    Event event = mock(Event.class);
    when(rawSpan1.getTraceId()).thenReturn(buffer);
    when(rawSpan1.getCustomerId()).thenReturn("customer1");
    when(event.getEventId()).thenReturn(buffer);
    when(rawSpan1.getEvent()).thenReturn(event);

    underTest.dataflowSamplingPercent = 100;
    KeyValue<String, StructuredTrace> actual = underTest
        .apply(mock(Windowed.class), rawSpansHolder);
    assertNull(actual.key);

    StructuredTrace trace = actual.value;

    assertEquals(event, trace.getEventList().get(0));
    assertTrue(
        trace.getTimestamps().getRecords().containsKey(DataflowMetricUtils.SPAN_ARRIVAL_TIME));
    assertTrue(trace.getTimestamps().getRecords().containsKey(TRACE_CREATION_TIME));
  }
}