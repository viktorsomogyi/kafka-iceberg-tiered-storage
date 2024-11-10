package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.svv.iceberg.kafka.AvroUtils;
import org.svv.iceberg.kafka.errors.MetadataSchemaReadException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public record IcebergMetadata(List<String> filePaths, Schema originalSchema) {

  private static final Schema schema;

  static {
    try {
      schema = new Schema.Parser().parse(IcebergMetadata.class.getClassLoader().getResourceAsStream("iceberg_metadata.avsc"));
    } catch (IOException e) {
      throw new MetadataSchemaReadException(e);
    }
  }

  public GenericRecord toAvro() {
    GenericRecord record = new GenericData.Record(schema);
    record.put("filepaths", filePaths);
    record.put("original_schema", originalSchema.toString());
    return record;
  }

  public static IcebergMetadata fromAvro(GenericRecord record) {
    var filepaths = (List<Utf8>) record.get("filepaths");
    return new IcebergMetadata(
        filepaths.stream().map(Utf8::toString).toList(),
        new Schema.Parser().parse(((Utf8) record.get("original_schema")).toString())
    );
  }

  public byte[] serialize() {
    return AvroUtils.serialize(toAvro());
  }

  public static IcebergMetadata deserialize(byte[] data) {
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    try {
      GenericRecord result = reader.read(null, decoder);
      return IcebergMetadata.fromAvro(result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
