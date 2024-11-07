package org.svv.iceberg.kafka.storage;

import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.IcebergTieredStorageConfig;
import org.svv.iceberg.kafka.schemas.Device;
import org.svv.iceberg.kafka.schemas.AvroSerDe;
import org.svv.iceberg.kafka.schemas.FanoutDevice;
import org.svv.iceberg.kafka.storage.utils.NoOpIcebergReader;
import org.svv.iceberg.kafka.storage.utils.NoOpSchemaRegistry;
import org.svv.iceberg.kafka.schemas.ResourcesBasedSchemaRegistry;
import org.svv.iceberg.kafka.storage.utils.NoOpIcebergWriter;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergRemoteStorageManagerTest {

  @Test
  public void testReadAllRecords() {
    EmbeddedKafkaConfig cfg = EmbeddedKafkaConfig.defaultConfig();
    var embeddedK = EmbeddedKafka.start(cfg);

    ProducerRecord<String, Device> record = new ProducerRecord<>("devices", "key", new Device(1, "device1", 1, 1));
    AvroSerDe serde = new AvroSerDe(Device.class);
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
  public void testCreateLogSegment() throws IOException {
    org.apache.kafka.storage.internals.log.LogSegment logSegment;
    try (IcebergRemoteStorageManager icebergRemoteStorageManager = new IcebergRemoteStorageManager()) {
      logSegment = icebergRemoteStorageManager.createLogSegment(0);
    }
    assertNotNull(logSegment);
  }

  @Test
  public void testPopulateLogSegment() throws IOException {
    org.apache.kafka.storage.internals.log.LogSegment logSegment;
    var topicId = UUID.randomUUID().toString();
    try (IcebergRemoteStorageManager icebergRemoteStorageManager = new IcebergRemoteStorageManager()) {
      icebergRemoteStorageManager.configure(defaultConfig());
      logSegment = icebergRemoteStorageManager.createLogSegment(0);
      var device1 = new FanoutDevice(topicId, 0, 0, 1, 1, "device1", 1, System.currentTimeMillis());
      var device2 = new FanoutDevice(topicId, 0, 0, 2, 2, "device2", 1, System.currentTimeMillis());
      var devices = Arrays.asList(device1.toIceberg(), device2.toIceberg());
      icebergRemoteStorageManager.populateLogSegment(logSegment, devices, Device.avroSchema());
    }
    var records = new ArrayList<>();
    logSegment.log().records().iterator().forEachRemaining(records::add);
    assertEquals(2, records.size());
  }

  @Test
  public void parseLongTest() {
    assertEquals(0, IcebergRemoteStorageManager.baseOffset(Path.of("/tmp/00000000000000000000.log")));
  }

  private static Map<String, Object> defaultConfig() {
    var rawConfigs = new HashMap<String, Object>();
    rawConfigs.put(IcebergTieredStorageConfig.SCHEMA_REGISTRY_CLASS_CONFIG, NoOpSchemaRegistry.class.getName());
    rawConfigs.put(IcebergTieredStorageConfig.ICEBERG_WRITER_CLASS_CONFIG, NoOpIcebergWriter.class.getName());
    rawConfigs.put(IcebergTieredStorageConfig.ICEBERG_READER_CLASS_CONFIG, NoOpIcebergReader.class.getName());
    rawConfigs.put("iceberg." + IcebergConfig.CATALOG_NAME_CONFIG, "test-in-memory");
    rawConfigs.put("iceberg." + IcebergConfig.CATALOG_IMPL_CONFIG, InMemoryCatalog.class.getName());
    return rawConfigs;
  }
}
