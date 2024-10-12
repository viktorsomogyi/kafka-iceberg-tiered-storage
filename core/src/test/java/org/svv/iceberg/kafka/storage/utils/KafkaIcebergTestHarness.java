package org.svv.iceberg.kafka.storage.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.storage.IcebergRemoteStorageManager;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

public class KafkaIcebergTestHarness extends IcebergMinioTestHarness {

  private KafkaContainer kafka;

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    kafka = new KafkaContainer("apache/kafka-native:3.8.0");
    kafka.withNetwork(network);
    kafka.withNetworkAliases("kafka");

    // init remote storage
    kafka.addEnv("KAFKA_REMOTE_LOG_STORAGE_SYSTEM_ENABLE", "true");
//    kafka.addEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH", "/opt/tiered-storage/*");
    kafka.addEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_IMPL_PREFIX", "rsm.config.");
    kafka.addEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME", IcebergRemoteStorageManager.class.getName());

    kafka.addEnv("KAFKA_REMOTE_LOG_METADATA_MANAGER_IMPL_PREFIX", "rlmm.config.");
    kafka.addEnv("KAFKA_REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME", "PLAINTEXT");
    kafka.addEnv("KAFKA_RLMM_CONFIG_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR", "1");


    kafka.withCopyToContainer(Transferable.of(""), "/opt/tiered-storage/");
    Startables.deepStart(kafka).join();
  }

  @AfterEach
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testKafkaSetup() {
    System.out.println("Kafka setup test");
  }
}
