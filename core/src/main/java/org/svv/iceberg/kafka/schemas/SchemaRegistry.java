package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * Interface for a schema registry that can provide Avro schemas by name. It will be used to parse data from log
 * segments and transform them to Iceberg schemas that can be written to the Iceberg table.
 */
public interface SchemaRegistry extends Configurable, Closeable {

  /**
   * Get the Avro schema by a context extracted from the Kafka log segment. The context can contain information
   * about the topic and partition, and the headers of the record.
   * @param context is the context extracted from the Kafka log segment.
   * @return
   */
  Schema valueSchemaByContext(SchemaContext context);
}
