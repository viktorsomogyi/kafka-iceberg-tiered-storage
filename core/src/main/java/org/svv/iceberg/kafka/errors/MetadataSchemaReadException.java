package org.svv.iceberg.kafka.errors;

public class MetadataSchemaReadException extends RuntimeException {

  public MetadataSchemaReadException(Throwable cause) {
    super(cause);
  }
}
