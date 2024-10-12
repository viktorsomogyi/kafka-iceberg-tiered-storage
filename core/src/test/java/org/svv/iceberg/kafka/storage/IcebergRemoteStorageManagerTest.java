package org.svv.iceberg.kafka.storage;

import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.IcebergTieredStorageConfig;
import org.svv.iceberg.kafka.schemas.Device;
import org.svv.iceberg.kafka.schemas.DeviceAvroSerDe;
import org.svv.iceberg.kafka.schemas.FileBasedSchemaRegistry;
import org.svv.iceberg.kafka.schemas.ResourcesBasedSchemaRegistry;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergRemoteStorageManagerTest {

  @Test
  public void testReadAllRecords() {
    EmbeddedKafkaConfig cfg = EmbeddedKafkaConfig.defaultConfig();
    var embeddedK = EmbeddedKafka.start(cfg);

    ProducerRecord<String, Device> record = new ProducerRecord<>("devices", "key", new Device(1, "device1", 1, 1));
    DeviceAvroSerDe serde = new DeviceAvroSerDe();
    EmbeddedKafka$.MODULE$.publishToKafka(record, cfg, serde);
    var messages = EmbeddedKafka$.MODULE$.consumeNumberMessagesFrom("devices", 1, false,
        Duration.apply(5, TimeUnit.MINUTES), cfg, serde);
    assertEquals(1, messages.length());
    var devices0 = Path.of(embeddedK.logsDirs().toString(), "devices-0").toFile();
    Arrays.stream(Objects.requireNonNull(devices0.listFiles())).iterator().forEachRemaining(System.out::println);

    try (var icebergRemoteStorageManager = new IcebergRemoteStorageManager()) {
      var config = new HashMap<String, Object>();
      config.put("log." + TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 10 * 1024 * 1024);
      config.put("log." + TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 4096);
      config.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, 0L);
      config.put(IcebergTieredStorageConfig.SCHEMA_REGISTRY_CLASS_CONFIG, ResourcesBasedSchemaRegistry.class.getName());
      config.put(IcebergTieredStorageConfig.SCHEMA_REGISTRY_CONFIGS_PREFIX + ResourcesBasedSchemaRegistry.SCHEMA_RESOURCES_DIRECTORY,
          "schemas/");
      icebergRemoteStorageManager.configure(config);
      var logSegmentData = new LogSegmentData(
          Path.of(devices0.toString(), "00000000000000000000.log"),
          Path.of(devices0.toString(), "00000000000000000000.index"),
          Path.of(devices0.toString(), "00000000000000000000.timeindex"),
          Optional.empty(),
          Path.of(devices0.toString(), "00000000000000000000.snapshot"),
          ByteBuffer.allocate(1000));
      var records = icebergRemoteStorageManager.readAll(logSegmentData);
      assertTrue(records.records().iterator().hasNext());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    embeddedK.stop(true);
  }

  @Test
  public void parseLongTest() {
    assertEquals(0, IcebergRemoteStorageManager.baseOffset(Path.of("/tmp/00000000000000000000.log")));
  }
}
