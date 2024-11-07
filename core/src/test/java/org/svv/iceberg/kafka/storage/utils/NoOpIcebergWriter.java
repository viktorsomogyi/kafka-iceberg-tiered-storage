package org.svv.iceberg.kafka.storage.utils;

import org.apache.avro.Schema;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.svv.iceberg.kafka.storage.writers.IcebergWriter;

import java.io.IOException;
import java.util.Map;

public class NoOpIcebergWriter implements IcebergWriter {

  @Override
  public void configure(Map<String, ?> map) {
    // No-op
  }

  @Override
  public WriteResult write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records) {
    // No-op
    return WriteResult.builder().build();
  }

  @Override
  public void close() throws IOException {

  }
}
