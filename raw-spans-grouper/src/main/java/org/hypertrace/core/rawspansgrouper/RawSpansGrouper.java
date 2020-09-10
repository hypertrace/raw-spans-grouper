package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SCHEMA_REGISTRY_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_TYPE_CONFIG_KEY;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
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
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.rawspansgrouper.keyvaluemappers.RawSpansHolderToStructuredTraceMapper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpansGrouper extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory
      .getLogger(RawSpansGrouper.class);

  private Map<String, String> schemaRegistryConfig;

  public RawSpansGrouper(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Properties properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    SchemaRegistryBasedAvroSerde<RawSpan> rawSpanSerde = new SchemaRegistryBasedAvroSerde<>(
        RawSpan.class);
    rawSpanSerde.configure(schemaRegistryConfig, false);

    SchemaRegistryBasedAvroSerde<StructuredTrace> structuredTraceSerde = new SchemaRegistryBasedAvroSerde<>(
        StructuredTrace.class);
    structuredTraceSerde.configure(schemaRegistryConfig, false);

    SchemaRegistryBasedAvroSerde<RawSpans> rawSpansHolderSerde = new SchemaRegistryBasedAvroSerde<>(
        RawSpans.class);
    rawSpansHolderSerde.configure(schemaRegistryConfig, false);

    String inputTopic = properties.getProperty(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = properties.getProperty(OUTPUT_TOPIC_CONFIG_KEY);
    int groupbySessionWindowInterval = (Integer) properties
        .get(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY);

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
                .with(Serdes.String(), Serdes.serdeFrom(rawSpansHolderSerde, rawSpansHolderSerde)))
        // the preceding operation creates a KTable and each aggregate operation generates a new record i.e the current aggregate
        // we only care about the final aggregate when the 'groupbySessionWindowInterval' is done. To achieve that
        // we need to suppress the updates
        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(groupbySessionWindowInterval),
            BufferConfig.unbounded()))
        //.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))  [todo - not sure why this isn't working]
        // convert the window aggregated objects (RawSpansHolder) to a stream
        .toStream()
        // convert RawSpansHolder to StructuredTrace
        .map(new RawSpansHolderToStructuredTraceMapper())
        // write to the output topic
        .to(outputTopic,
            Produced.with(Serdes.String(),
                Serdes.serdeFrom(structuredTraceSerde, structuredTraceSerde)));

    return streamsBuilder;
  }

  @Override
  public Properties getStreamsConfig(Config config) {
    Properties properties = new Properties();

    schemaRegistryConfig = ConfigUtils.getFlatMapConfig(config, SCHEMA_REGISTRY_CONFIG_KEY);
    properties.putAll(schemaRegistryConfig);

    properties.put(SPAN_TYPE_CONFIG_KEY, config.getString(SPAN_TYPE_CONFIG_KEY));
    properties.put(INPUT_TOPIC_CONFIG_KEY, config.getString(INPUT_TOPIC_CONFIG_KEY));
    properties.put(OUTPUT_TOPIC_CONFIG_KEY, config.getString(OUTPUT_TOPIC_CONFIG_KEY));
    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));
    properties.put(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY,
        config.getInt(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY));

    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    properties.put(JOB_CONFIG, config);

    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }
}
