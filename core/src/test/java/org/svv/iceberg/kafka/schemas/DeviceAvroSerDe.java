package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class DeviceAvroSerDe implements Serializer<Device>, Deserializer<Device> {

  private final DeviceAvroSerializer serializer;
  private final DeviceAvroDeserializer deserializer;
  private final Schema schema;

  public DeviceAvroSerDe() {
    try {
      schema = new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("schemas/device.avsc"));
      serializer = new DeviceAvroSerializer(schema);
      deserializer = new DeviceAvroDeserializer(schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Device deserialize(String topic, byte[] data) {
    return deserializer.deserialize(topic, data);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Device data) {
    return serializer.serialize(topic, data);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  private class DeviceAvroSerializer implements Serializer<Device> {

    private Schema schema;

    public DeviceAvroSerializer(Schema schema) {
      this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, Device data) {
      // Serialize the `record to a byte array
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

      try {
        writer.write(data.toAvro(schema), encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class DeviceAvroDeserializer implements Deserializer<Device> {

    private Schema schema;

    public DeviceAvroDeserializer(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Device deserialize(String topic, byte[] data) {
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

      try {
        GenericRecord result = reader.read(null, decoder);
        return Device.fromAvro(result);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
