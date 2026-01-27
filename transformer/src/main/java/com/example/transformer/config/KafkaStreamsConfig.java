package com.example.transformer.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams configuration for the Event Transformer.
 *
 * <h2>Runtime Configuration</h2>
 *
 * <h3>Parallelism & Threading</h3>
 * <ul>
 *   <li><b>SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS</b> (default: 1) -
 *       Number of stream threads per instance. Set to number of input topic partitions
 *       for maximum parallelism. Each thread processes one partition.</li>
 * </ul>
 *
 * <h3>Batching & Latency</h3>
 * <ul>
 *   <li><b>SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS</b> (default: 1000) -
 *       How often to commit offsets. Lower = less duplicate processing on failure,
 *       higher = better throughput.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_CACHE_MAX_BYTES_BUFFERING</b> (default: 10485760 / 10MB) -
 *       Record cache size per topology. Higher = more batching, lower latency impact.
 *       Set to 0 for real-time processing with no caching.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_POLL_MS</b> (default: 100) -
 *       Max time to block waiting for input. Lower = more responsive, higher CPU.</li>
 * </ul>
 *
 * <h3>Consumer Tuning</h3>
 * <ul>
 *   <li><b>SPRING_KAFKA_STREAMS_MAX_POLL_RECORDS</b> (default: 1000) -
 *       Max records per poll. Higher = better throughput, more memory usage.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_FETCH_MAX_BYTES</b> (default: 52428800 / 50MB) -
 *       Max data per fetch request across all partitions.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_FETCH_MAX_WAIT_MS</b> (default: 500) -
 *       Max time broker waits before responding if fetch.min.bytes not satisfied.</li>
 * </ul>
 *
 * <h3>Producer Tuning</h3>
 * <ul>
 *   <li><b>SPRING_KAFKA_STREAMS_BATCH_SIZE</b> (default: 16384 / 16KB) -
 *       Batch size for producer. Higher = better throughput, more latency.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_LINGER_MS</b> (default: 100) -
 *       Time to wait for batch to fill. Higher = more batching, more latency.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_BUFFER_MEMORY</b> (default: 33554432 / 32MB) -
 *       Total memory for producer buffering.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_COMPRESSION_TYPE</b> (default: none) -
 *       Compression: none, gzip, snappy, lz4, zstd. Reduces network I/O.</li>
 * </ul>
 *
 * <h3>Reliability</h3>
 * <ul>
 *   <li><b>SPRING_KAFKA_STREAMS_PROCESSING_GUARANTEE</b> (default: at_least_once) -
 *       Options: at_least_once, exactly_once, exactly_once_v2.
 *       exactly_once_v2 requires Kafka 2.5+ and adds overhead.</li>
 *   <li><b>SPRING_KAFKA_STREAMS_REPLICATION_FACTOR</b> (default: 1) -
 *       Replication for internal topics (changelog, repartition). Set to 3 for production.</li>
 * </ul>
 *
 * <h3>Scaling</h3>
 * <p>To scale horizontally, run multiple transformer instances with the same application-id.
 * Kafka Streams will distribute partitions across instances automatically.
 * Max parallelism = number of input topic partitions.</p>
 *
 * <h3>Example Docker Compose Override</h3>
 * <pre>
 * transformer:
 *   environment:
 *     SPRING_KAFKA_STREAMS_NUM_STREAM_THREADS: 4
 *     SPRING_KAFKA_STREAMS_COMMIT_INTERVAL_MS: 500
 *     SPRING_KAFKA_STREAMS_PROCESSING_GUARANTEE: exactly_once_v2
 *     SPRING_KAFKA_STREAMS_COMPRESSION_TYPE: lz4
 * </pre>
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    // Threading
    @Value("${spring.kafka.streams.num-stream-threads:1}")
    private int numStreamThreads;

    // Batching & Latency
    @Value("${spring.kafka.streams.commit-interval-ms:1000}")
    private long commitIntervalMs;

    @Value("${spring.kafka.streams.cache-max-bytes-buffering:10485760}")
    private long cacheMaxBytesBuffering;

    @Value("${spring.kafka.streams.poll-ms:100}")
    private long pollMs;

    // Consumer tuning
    @Value("${spring.kafka.streams.max-poll-records:1000}")
    private int maxPollRecords;

    @Value("${spring.kafka.streams.fetch-max-bytes:52428800}")
    private int fetchMaxBytes;

    @Value("${spring.kafka.streams.fetch-max-wait-ms:500}")
    private int fetchMaxWaitMs;

    // Producer tuning
    @Value("${spring.kafka.streams.batch-size:16384}")
    private int batchSize;

    @Value("${spring.kafka.streams.linger-ms:100}")
    private long lingerMs;

    @Value("${spring.kafka.streams.buffer-memory:33554432}")
    private long bufferMemory;

    @Value("${spring.kafka.streams.compression-type:none}")
    private String compressionType;

    // Reliability
    @Value("${spring.kafka.streams.processing-guarantee:at_least_once}")
    private String processingGuarantee;

    @Value("${spring.kafka.streams.replication-factor:1}")
    private int replicationFactor;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();

        // Core settings
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Threading
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);

        // Batching & Latency
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, cacheMaxBytesBuffering);
        props.put(StreamsConfig.POLL_MS_CONFIG, pollMs);

        // Consumer tuning (prefixed with consumer.)
        props.put(StreamsConfig.consumerPrefix("max.poll.records"), maxPollRecords);
        props.put(StreamsConfig.consumerPrefix("fetch.max.bytes"), fetchMaxBytes);
        props.put(StreamsConfig.consumerPrefix("fetch.max.wait.ms"), fetchMaxWaitMs);

        // Producer tuning (prefixed with producer.)
        props.put(StreamsConfig.producerPrefix("batch.size"), batchSize);
        props.put(StreamsConfig.producerPrefix("linger.ms"), lingerMs);
        props.put(StreamsConfig.producerPrefix("buffer.memory"), bufferMemory);
        props.put(StreamsConfig.producerPrefix("compression.type"), compressionType);

        // Reliability
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);

        return new KafkaStreamsConfiguration(props);
    }
}
