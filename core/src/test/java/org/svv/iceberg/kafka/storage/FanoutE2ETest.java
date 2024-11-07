package org.svv.iceberg.kafka.storage;

import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.schemas.FanoutDevice;
import org.svv.iceberg.kafka.storage.readers.IcebergFanoutReader;
import org.svv.iceberg.kafka.storage.utils.IcebergKafkaTestHarness;
import org.svv.iceberg.kafka.storage.writers.IcebergFanOutWriter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.svv.iceberg.kafka.utils.TestUtils.randomString;

public class FanoutE2ETest extends IcebergKafkaTestHarness {

  private static AtomicLong deviceId = new AtomicLong(0);

  @Test
  public void test() {
    produceToKafka();
    consumeFromKafka();
  }

  private void produceToKafka() {
    publishToKafka("fanout_device", randomDeviceStream().limit(5_000L), FanoutDevice.class);
    System.out.println("Produced 5_000 messages to Kafka");
  }

  private void consumeFromKafka() {
    consumeFromKafka("fanout_device", FanoutDevice.class);
    System.out.println("Produced 15_000 messages to Kafka");
  }

  private static Stream<FanoutDevice> randomDeviceStream() {
    return Stream.generate(() -> new FanoutDevice("", 0, 0, 0, deviceId.getAndIncrement(), randomString(100_000), 1, System.currentTimeMillis()));
  }

  @Override
  public String icebergWriterClassName() {
    return IcebergFanOutWriter.class.getName();
  }

  @Override
  public String icebergReaderClassName() {
    return IcebergFanoutReader.class.getName();
  }
}
