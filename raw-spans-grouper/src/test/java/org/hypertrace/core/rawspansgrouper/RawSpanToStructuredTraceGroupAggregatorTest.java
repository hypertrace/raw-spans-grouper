package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;

import org.hypertrace.core.datamodel.RawSpan;
import org.junit.jupiter.api.Test;

public class RawSpanToStructuredTraceGroupAggregatorTest {

  private final String DATAFLOW_SAMPLING_PERCENT = "dataflow.metriccollection.sampling.percent";
  private static final String TRACE_CREATION_TIME = "trace.creation.time";

  @Test
  public void testRawSpanToStructuredTraceGroupAggregatorSimpleMethods() {
    Config config = mock(Config.class);
    when(config.hasPath(DATAFLOW_SAMPLING_PERCENT)).thenReturn(true);
    when(config.getDouble(DATAFLOW_SAMPLING_PERCENT)).thenReturn(100.0);
    RawSpanToStructuredTraceAvroGroupAggregator aggregator =
        new RawSpanToStructuredTraceAvroGroupAggregator();
//    List<RawSpan> rawSpanList = aggregator.createAccumulator();
//    assertNotNull(rawSpanList);
//    assertTrue(rawSpanList.isEmpty());

    RawSpan rawSpan1 = mock(RawSpan.class);
    RawSpan rawSpan2 = mock(RawSpan.class);


    RawSpansHolder rawSpansHolder = RawSpansHolder.newBuilder().build();
    aggregator.apply(null, rawSpan1, rawSpansHolder);
    aggregator.apply(null, rawSpan2, rawSpansHolder);

    assertFalse(rawSpansHolder.getRawSpans().isEmpty());
    assertEquals(rawSpan1, rawSpansHolder.getRawSpans().get(0));
    assertEquals(rawSpan2, rawSpansHolder.getRawSpans().get(1));

//    ByteBuffer buffer = mock(ByteBuffer.class);
//    Event event = mock(Event.class);
//    when(rawSpan1.getTraceId()).thenReturn(buffer);
//    when(rawSpan1.getCustomerId()).thenReturn("customer1");
//    when(event.getEventId()).thenReturn(buffer);
//    when(rawSpan1.getEvent()).thenReturn(event);
//    StructuredTrace trace = aggregator.getResult(List.of(rawSpan1));
//    assertEquals("customer1", trace.getCustomerId());
//    assertTrue(trace.getTimestamps().getRecords().containsKey(DataflowMetricUtils.SPAN_ARRIVAL_TIME));
//    assertTrue(trace.getTimestamps().getRecords().containsKey(TRACE_CREATION_TIME));
  }
}
