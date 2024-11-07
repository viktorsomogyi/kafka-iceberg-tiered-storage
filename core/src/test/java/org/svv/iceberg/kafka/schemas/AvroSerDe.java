package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class AvroSerDe<T> implements Serializer<T>, Deserializer<T> {

  private final AvroSerializer serializer;
  private final AvroDeserializer deserializer;

  public AvroSerDe(Class<T> clazz) {
    serializer = new AvroSerializer();
    deserializer = new AvroDeserializer(clazz);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return deserializer.deserialize(topic, data);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serializer.serialize(topic, data);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  private class AvroSerializer implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
      // Serialize the `record to a byte array
      var schema = ReflectData.get().getSchema(data.getClass());
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
      DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);

      try {
        dataFileWriter.create(schema, outputStream);
        dataFileWriter.append(data);
        dataFileWriter.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return outputStream.toByteArray();
    }

//    public byte[] serialize2(String topic, T data) {
//      // Serialize the `record to a byte array
//      ByteArrayOutputStream out = new ByteArrayOutputStream();
//      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
//      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//
//      try {
//        var schema = ReflectData.get().getSchema(data.getClass());
//        writer.write(toAvro(schema, data), encoder);
//        encoder.flush();
//        out.close();
//        return out.toByteArray();
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
  }

  private class AvroDeserializer implements Deserializer<T> {

    private Class<T> clazz;

    public AvroDeserializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      Schema schema = ReflectData.get().getSchema(clazz);
      SeekableByteArrayInput inputStream = new SeekableByteArrayInput(data);
      DatumReader<T> datumReader = new ReflectDatumReader<>(schema);
      DataFileReader<T> dataFileReader = null;
      try {
        dataFileReader = new DataFileReader<>(inputStream, datumReader);
        T object = null;
        if (dataFileReader.hasNext()) {
          object = dataFileReader.next();
        }
        dataFileReader.close();
        return object;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (dataFileReader != null) {
          try {
            dataFileReader.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

//    @Override
//    public T deserialize(String topic, byte[] data) {
//      ByteArrayInputStream in = new ByteArrayInputStream(data);
//      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
//      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
//
//      try {
//        GenericRecord result = reader.read(null, decoder);
//        return Device.fromAvro(result);
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
  }
}
