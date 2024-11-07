package org.svv.iceberg.kafka.errors;

public class IcebergWriterException extends RuntimeException {

  public IcebergWriterException(String message) {
    super(message);
  }

  public IcebergWriterException(Throwable cause) {
    super(cause);
  }

  public IcebergWriterException(String message, Throwable cause) {
    super(message, cause);
  }
}
