package org.svv.iceberg.kafka.storage;

public class IcebergRemoteStorageException extends RuntimeException {

  public IcebergRemoteStorageException(String message) {
    super(message);
  }

  public IcebergRemoteStorageException(Throwable cause) {
    super(cause);
  }
}
