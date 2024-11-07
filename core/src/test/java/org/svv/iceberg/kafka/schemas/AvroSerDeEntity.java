package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface AvroSerDeEntity<T> {

  GenericRecord toAvro();

  Schema schema();

  T fromAvro(GenericRecord record);
}
