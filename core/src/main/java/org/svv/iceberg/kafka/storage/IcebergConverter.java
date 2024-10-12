package org.svv.iceberg.kafka.storage;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class IcebergConverter {

  public static GenericRecord parseAvro(byte[] data, org.apache.avro.Schema schema) {
    try {
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static org.apache.iceberg.data.GenericRecord toIcebergRecord(org.apache.avro.Schema schema, GenericRecord record) {
    Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    org.apache.iceberg.data.GenericRecord icebergRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    schema.getFields().forEach(field -> {
      icebergRecord.setField(field.name(), record.get(field.name()));
    });
    return icebergRecord;
  }
}
