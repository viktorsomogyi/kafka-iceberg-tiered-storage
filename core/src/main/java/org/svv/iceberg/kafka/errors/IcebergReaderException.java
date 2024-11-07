package org.svv.iceberg.kafka.errors;

public class IcebergReaderException extends RuntimeException {

  public IcebergReaderException(String message) {
    super(message);
  }

  public IcebergReaderException(Throwable cause) {
    super(cause);
  }
}
