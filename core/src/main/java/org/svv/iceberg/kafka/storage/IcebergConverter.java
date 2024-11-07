package org.svv.iceberg.kafka.storage;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.kafka.common.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class IcebergConverter {

  public static GenericRecord parseAvro(Record record, org.apache.avro.Schema schema) {
    try {
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(record.value().array()), null);
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

  public static GenericRecord toAvroRecord(org.apache.avro.Schema schema, org.apache.iceberg.data.GenericRecord record) {
    var avroRecord = new GenericData.Record(schema);
    schema.getFields().forEach(field -> avroRecord.put(field.name(), record.getField(field.name())));
    return avroRecord;
  }
}
