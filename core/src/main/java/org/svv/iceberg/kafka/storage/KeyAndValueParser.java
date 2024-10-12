package org.svv.iceberg.kafka.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.kafka.common.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class KeyAndValueParser {

  private final Schema keySchema;
  private final Schema valueSchema;
  private final org.apache.iceberg.Schema icebergKeySchema;
  private final org.apache.iceberg.Schema icebergValueSchema;

  public KeyAndValueParser(Schema keySchema, Schema valueSchema) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.icebergKeySchema = AvroSchemaUtil.toIceberg(keySchema);
    this.icebergValueSchema = AvroSchemaUtil.toIceberg(valueSchema);
  }

  public KeyAndValue parse(Record record) {
    if (record.hasKey()) {
      return new KeyAndValue(record.key().array(), record.value());
    }
    return new KeyAndValue(null, record.value());
  }

  public org.apache.iceberg.data.GenericRecord mapToIceberg(GenericRecord avroRecord, Schema schema) {
    return null;
  }
}
