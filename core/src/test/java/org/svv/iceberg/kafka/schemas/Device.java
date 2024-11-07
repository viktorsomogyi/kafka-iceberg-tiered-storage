package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.types.Types;

import java.io.IOException;

public record Device(long xid, String name, int version, long timestamp) {

  public static org.apache.iceberg.Schema icebergSchema() {
    return new org.apache.iceberg.Schema(
        Types.NestedField.optional(4, "xid", Types.LongType.get()),
        Types.NestedField.optional(5, "name", Types.StringType.get()),
        Types.NestedField.optional(6, "version", Types.LongType.get()),
        Types.NestedField.optional(7, "timestamp", Types.LongType.get()));
  }

  public static Schema avroSchema() {
    try {
      return new Schema.Parser().parse(Device.class.getClassLoader().getResourceAsStream("schemas/device.avsc"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public org.apache.iceberg.data.GenericRecord toIceberg() {
    org.apache.iceberg.data.GenericRecord record = org.apache.iceberg.data.GenericRecord.create(icebergSchema());
    record.setField("xid", xid);
    record.setField("name", name);
    record.setField("version", version);
    record.setField("timestamp", timestamp);
    return record;
  }
}
