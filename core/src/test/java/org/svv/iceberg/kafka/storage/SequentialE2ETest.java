package org.svv.iceberg.kafka.storage;

import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.schemas.Device;
import org.svv.iceberg.kafka.storage.utils.IcebergKafkaTestHarness;
import org.svv.iceberg.kafka.storage.utils.NoOpIcebergReader;
import org.svv.iceberg.kafka.storage.writers.IcebergSequentialWriter;

import java.util.stream.Stream;

import static org.svv.iceberg.kafka.utils.TestUtils.randomString;

public class SequentialE2ETest extends IcebergKafkaTestHarness {

  private static long deviceId = 0;

  @Test
  public void test() {
    produceToKafka();
  }

  private void produceToKafka() {
    publishToKafka("device", randomDeviceStream().limit(15_000L), Device.class);
    System.out.println("Produced 15_000 messages to Kafka");
  }

  private static Stream<Device> randomDeviceStream() {
    return Stream.generate(() -> new Device(deviceId++, randomString(100_000), 1, System.currentTimeMillis()));
  }

  @Override
  public String icebergWriterClassName() {
    return IcebergSequentialWriter.class.getName();
  }

  @Override
  public String icebergReaderClassName() {
    return NoOpIcebergReader.class.getName();
  }
}
