package org.svv.iceberg.kafka.storage;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.svv.iceberg.kafka.schemas.Device;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IcebergConverterTest {

  @Test
  public void testToIcebergRecord() throws IOException {
    var device = new Device(1, "device-1", 1, System.currentTimeMillis());
    Schema schema = new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("schemas/device.avsc"));
    var genericRecord = device.toAvro(schema);
    var icebergRecord = IcebergConverter.toIcebergRecord(schema, genericRecord);
    assertEquals(device, Device.fromIceberg(icebergRecord));
  }
}
