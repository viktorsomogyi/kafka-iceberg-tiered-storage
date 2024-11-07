package org.svv.iceberg.kafka.storage.utils;

import org.apache.avro.Schema;
import org.svv.iceberg.kafka.schemas.SchemaContext;
import org.svv.iceberg.kafka.schemas.SchemaRegistry;

import java.io.IOException;
import java.util.Map;

public class NoOpSchemaRegistry implements SchemaRegistry {

  @Override
  public Schema valueSchemaByContext(SchemaContext context) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
