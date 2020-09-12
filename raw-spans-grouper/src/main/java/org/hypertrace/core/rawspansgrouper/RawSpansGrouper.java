package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.rawspansgrouper.keyvaluemappers.RawSpansToStructuredTraceMapper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpansGrouper extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory
      .getLogger(RawSpansGrouper.class);

  public RawSpansGrouper(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    SchemaRegistryBasedAvroSerde<RawSpan> rawSpanSerde = new SchemaRegistryBasedAvroSerde<>(
        RawSpan.class);
    rawSpanSerde.configure(properties, false);

    SchemaRegistryBasedAvroSerde<StructuredTrace> structuredTraceSerde = new SchemaRegistryBasedAvroSerde<>(
        StructuredTrace.class);
    structuredTraceSerde.configure(properties, false);

    SchemaRegistryBasedAvroSerde<RawSpans> rawSpansSerde = new SchemaRegistryBasedAvroSerde<>(
        RawSpans.class);
    rawSpansSerde.configure(properties, false);

    String inputTopic = getAppConfig().getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = getAppConfig().getString(OUTPUT_TOPIC_CONFIG_KEY);

    int groupbySessionWindowInterval = (Integer) getAppConfig()
        .getInt(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY);

    KStream<String, RawSpan> inputStream = (KStream<String, RawSpan>) inputStreams.get(inputTopic);
    if (inputStream == null) {

      inputStream = streamsBuilder
          // read the input topic
          .stream(inputTopic,
              Consumed.with(Serdes.String(), Serdes.serdeFrom(rawSpanSerde, rawSpanSerde)));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        // group by trace_id
        // currently this results in a repartition topic - ideally we want to avoid this
//        .groupBy(new TraceIdKeyValueMapper(),
//            Grouped.with(Serdes.String(), Serdes.serdeFrom(rawSpanSerde, rawSpanSerde)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.serdeFrom(rawSpanSerde, rawSpanSerde)))
        // aggregate for 'groupbySessionWindowInterval' secs
        // note that if the grace period is not added then by default it is 24 hrs !
        .windowedBy(TimeWindows.of(Duration.ofSeconds(groupbySessionWindowInterval))
            .grace(Duration.ofMillis(200)))
        // aggregate
        .aggregate(RawSpans.newBuilder()::build,
            new RawSpanToStructuredTraceAvroGroupAggregator(),
            Materialized
                .with(Serdes.String(), Serdes.serdeFrom(rawSpansSerde, rawSpansSerde)))
        // the preceding operation creates a KTable and each aggregate operation generates a new record i.e the current aggregate
        // we only care about the final aggregate when the 'groupbySessionWindowInterval' is done. To achieve that
        // we need to suppress the updates
        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(groupbySessionWindowInterval),
            BufferConfig.unbounded()))
        //.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))  [todo - not sure why this isn't working]
        // convert the window aggregated objects (RawSpans) to a stream
        .toStream()
        // convert RawSpans to StructuredTrace
        .map(new RawSpansToStructuredTraceMapper())
        // write to the output topic
        .to(outputTopic,
            Produced.with(Serdes.String(),
                Serdes.serdeFrom(structuredTraceSerde, structuredTraceSerde)));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config config) {
    Map<String, Object> properties = new HashMap<>(
        ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));
    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }


  public List<String> getInputTopics() {
    return List.of(getAppConfig().getString(INPUT_TOPIC_CONFIG_KEY));
  }


  public List<String> getOutputTopics() {
    return List.of(getAppConfig().getString(OUTPUT_TOPIC_CONFIG_KEY));
  }
}
