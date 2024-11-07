package org.svv.iceberg.kafka.storage.readers;

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.util.List;

public interface IcebergReader extends Configurable {

  List<GenericRecord> read(RemoteLogSegmentMetadata metadata);
}
