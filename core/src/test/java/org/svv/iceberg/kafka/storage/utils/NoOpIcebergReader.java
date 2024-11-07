package org.svv.iceberg.kafka.storage.utils;

import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.svv.iceberg.kafka.storage.readers.IcebergReader;

import java.util.List;
import java.util.Map;

public class NoOpIcebergReader implements IcebergReader {

  @Override
  public List<GenericRecord> read(RemoteLogSegmentMetadata metadata) {
    return List.of();
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
