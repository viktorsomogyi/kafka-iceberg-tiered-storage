package org.svv.iceberg.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.svv.iceberg.kafka.errors.MetadataSchemaReadException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtils {

  public static byte[] serialize(GenericRecord record) {
    // Serialize the `record to a byte array
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    try {
      writer.write(record, encoder);
      encoder.flush();
      out.close();
      return out.toByteArray();
    } catch (IOException e) {
      throw new MetadataSchemaReadException(e);
    }
  }
}
