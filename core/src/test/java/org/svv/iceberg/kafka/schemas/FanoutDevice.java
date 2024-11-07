package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.types.Types;

import java.io.IOException;

public class FanoutDevice {

  private String topicId;
  private int partition;
  private long segmentBaseOffset;
  private long recordOffset;
  private long xid;
  private String name;
  private int version;
  private long timestamp;

  public FanoutDevice() {
  }

  public FanoutDevice(String topicId, int partition, long segmentBaseOffset, long recordOffset, long xid, String name, int version, long timestamp) {
    this.topicId = topicId;
    this.partition = partition;
    this.segmentBaseOffset = segmentBaseOffset;
    this.recordOffset = recordOffset;
    this.xid = xid;
    this.name = name;
    this.version = version;
    this.timestamp = timestamp;
  }

  public String getTopicId() {
    return topicId;
  }

  public void setTopicId(String topicId) {
    this.topicId = topicId;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getSegmentBaseOffset() {
    return segmentBaseOffset;
  }

  public void setSegmentBaseOffset(long segmentBaseOffset) {
    this.segmentBaseOffset = segmentBaseOffset;
  }

  public long getRecordOffset() {
    return recordOffset;
  }

  public void setRecordOffset(long recordOffset) {
    this.recordOffset = recordOffset;
  }

  public long getXid() {
    return xid;
  }

  public void setXid(long xid) {
    this.xid = xid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public static org.apache.iceberg.Schema icebergSchema() {
    return new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "topic_id", Types.StringType.get()),
        Types.NestedField.required(2, "partition", Types.IntegerType.get()),
        Types.NestedField.required(3, "logsegment_base_offset", Types.LongType.get()),
        Types.NestedField.required(4, "record_offset", Types.LongType.get()),
        Types.NestedField.optional(5, "xid", Types.LongType.get()),
        Types.NestedField.optional(6, "name", Types.StringType.get()),
        Types.NestedField.optional(7, "version", Types.LongType.get()),
        Types.NestedField.optional(8, "timestamp", Types.LongType.get()));
  }

  public static Schema avroSchema() {
    try {
      return new Schema.Parser().parse(FanoutDevice.class.getClassLoader().getResourceAsStream("schemas/fanout_device.avsc"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public GenericRecord toAvro(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("topic_id", topicId);
    record.put("partition", partition);
    record.put("logsegment_base_offset", segmentBaseOffset);
    record.put("record_offset", recordOffset);
    record.put("xid", xid);
    record.put("name", name);
    record.put("version", version);
    record.put("timestamp", timestamp);
    return record;
  }

  public org.apache.iceberg.data.GenericRecord toIceberg() {
    org.apache.iceberg.data.GenericRecord record = org.apache.iceberg.data.GenericRecord.create(icebergSchema());
    record.setField("topic_id", topicId);
    record.setField("partition", partition);
    record.setField("logsegment_base_offset", segmentBaseOffset);
    record.setField("record_offset", recordOffset);
    record.setField("xid", xid);
    record.setField("name", name);
    record.setField("version", version);
    record.setField("timestamp", timestamp);
    return record;
  }

  public static FanoutDevice fromAvro(GenericRecord record) {
    return new FanoutDevice(
        ((Utf8) record.get("topic_id")).toString(),
        (Integer) record.get("partition"),
        (Long) record.get("logsegment_base_offset"),
        (Long) record.get("record_offset"),
        (Long) record.get("xid"),
        ((Utf8) record.get("name")).toString(),
        (Integer) record.get("version"),
        (Long) record.get("timestamp")
    );
  }

  public static FanoutDevice fromIceberg(org.apache.iceberg.data.GenericRecord record) {
    return new FanoutDevice(
        (String) record.getField("topic_id"),
        (Integer) record.getField("partition"),
        (Long) record.getField("logsegment_base_offset"),
        (Long) record.getField("record_offset"),
        (Long) record.getField("xid"),
        (String) record.getField("name"),
        (Integer) record.getField("version"),
        (Long) record.getField("timestamp")
    );
  }
}
