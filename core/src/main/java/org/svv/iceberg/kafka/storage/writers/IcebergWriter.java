package org.svv.iceberg.kafka.storage.writers;

import org.apache.avro.Schema;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.Closeable;
import java.util.Map;

/**
 * Responsible for writing records to an Iceberg table.
 */
public interface IcebergWriter extends Configurable, Closeable {

  @Override
  void configure(Map<String, ?> map);

  /**
   * Writes the records to an Iceberg table.
   *
   * @param remoteLogSegmentMetadata The metadata of the remote log segment.
   * @param schema                   The schema of the records.
   * @param records                  The records to write.
   * @return The path to the written file.
   */
  WriteResult write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records);
}
