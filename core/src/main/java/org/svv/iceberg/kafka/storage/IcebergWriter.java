package org.svv.iceberg.kafka.storage;

import org.apache.avro.Schema;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * Responsible for writing records to an Iceberg table.
 */
public interface IcebergWriter extends Configurable {
  @Override
  void configure(Map<String, ?> map);

  /**
   * Writes the records to the Iceberg table.
   * @param remoteLogSegmentMetadata The metadata of the remote log segment.
   * @param schema The schema of the records.
   * @param records The records to write.
   */
  void write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records);
}
