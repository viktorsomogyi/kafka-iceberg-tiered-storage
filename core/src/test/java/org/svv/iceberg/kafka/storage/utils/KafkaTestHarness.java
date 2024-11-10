package org.svv.iceberg.kafka.storage.utils;

import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.svv.iceberg.kafka.schemas.AvroSerDe;
import scala.jdk.CollectionConverters;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class KafkaTestHarness extends IcebergMinioDockerTestHarness {

  private EmbeddedK embeddedK;

  protected Map<String, String> extraBrokerConfigs;

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    EmbeddedKafkaConfig cfg = EmbeddedKafkaConfig.defaultConfig();
    extraBrokerConfigs = new HashMap<>();
    extraBrokerConfigs.putAll(extraBrokerConfigs());
    extraBrokerConfigs.putAll(CollectionConverters.MapHasAsJava(cfg.customBrokerProperties()).asJava());
    var actualCfg = EmbeddedKafkaConfig$.MODULE$.apply(9092, 2181,
        scala.collection.immutable.Map.from(CollectionConverters.MapHasAsScala(extraBrokerConfigs).asScala()),
        cfg.customProducerProperties(), cfg.customConsumerProperties());
    embeddedK = EmbeddedKafka.start(actualCfg);
  }

  @AfterEach
  public void tearDown() {
    if (embeddedK != null) {
      embeddedK.stop(true);
    }
    super.tearDown();
  }

  public Map<String, String> extraBrokerConfigs() {
    return new HashMap<>();
  }

  public <T> void publishToKafka(String topic, Stream<T> values, Class<T> clazz) {
    var topicConfig = new HashMap<String, String>();
    topicConfig.put("remote.storage.enable", "true");
    topicConfig.put("local.retention.ms", "1000");
    topicConfig.put("cleanup.policy", "delete");
    topicConfig.put("segment.bytes", "1048576");
    topicConfig.put("file.delete.delay.ms", "1000");
    var scalaTopicConfig = scala.collection.immutable.Map.from(CollectionConverters.MapHasAsScala(topicConfig).asScala());
    var success = EmbeddedKafka$.MODULE$.createCustomTopic(topic, scalaTopicConfig, 1, 1, embeddedK.config());
    assert success.isSuccess();

    EmbeddedKafka$.MODULE$.<String, T, T>withProducer(producer -> {
      values.forEach(value -> {
        var record = new ProducerRecord<String, T>(topic, value);
        producer.send(record);
      });
      return null;
    }, embeddedK.config(), new StringSerializer(), new AvroSerDe<>(clazz));
    var topicDir = Paths.get(embeddedK.logsDirs().toString(), topic + "-0");
    var partitionDirs = Objects.requireNonNull(topicDir.toFile().listFiles());
    System.out.println("Files in " + topicDir);
    for (var file : Arrays.stream(partitionDirs).sorted().toList()) {
      System.out.println(file);
    }
  }

  public <T> void consumeFromKafka(String topic, Class<T> clazz) {
    var deserializer = new AvroSerDe<>(clazz);
    EmbeddedKafka$.MODULE$.withConsumer(consumer -> {
      if (consumer.subscription().isEmpty()) {
        consumer.subscribe(java.util.Collections.singletonList(topic));
      }
      return consumer.poll(Duration.ofMinutes(1));
    }, embeddedK.config(), new StringDeserializer(), deserializer).records("fanout_device").forEach(record -> {
      System.out.println(record.value());
    });
  }
}
