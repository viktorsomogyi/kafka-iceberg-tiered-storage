package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public record Device(long xid, String name, int version, long timestamp) {

  public GenericRecord toAvro(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("xid", xid);
    record.put("name", name);
    record.put("version", version);
    record.put("timestamp", timestamp);
    return record;
  }

  public org.apache.iceberg.data.GenericRecord toIceberg(org.apache.iceberg.Schema schema) {
    org.apache.iceberg.data.GenericRecord record = org.apache.iceberg.data.GenericRecord.create(schema);
    record.setField("xid", xid);
    record.setField("name", name);
    record.setField("version", version);
    record.setField("timestamp", timestamp);
    return record;
  }

  public static Device fromAvro(GenericRecord record) {
    return new Device(
        (Long) record.get("xid"),
        ((Utf8) record.get("name")).toString(),
        (Integer) record.get("version"),
        (Long) record.get("timestamp")
    );
  }

  public static Device fromIceberg(org.apache.iceberg.data.GenericRecord record) {
    return new Device(
        (Long) record.getField("xid"),
        (String) record.getField("name"),
        (Integer) record.getField("version"),
        (Long) record.getField("timestamp")
    );
  }
}
