package org.svv.iceberg.kafka.storage.readers;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.svv.iceberg.kafka.errors.IcebergReaderException;
import org.svv.iceberg.kafka.schemas.IcebergMetadata;
import org.svv.iceberg.kafka.storage.CatalogSingleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergFanoutReader implements IcebergReader {

  private final static String RECORD_OFFSET_KEY = "record_offset";
  private final static String SEGMENT_BASE_OFFSET_KEY = "segment_base_offset";
  private final static String TOPIC_ID_KEY = "topic_id";

  @Override
  public void configure(Map<String, ?> map) {

  }

  @Override
  public List<GenericRecord> read(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    var catalog = CatalogSingleton.catalog();
    if (remoteLogSegmentMetadata.customMetadata().isEmpty()) {
      throw new IcebergReaderException("Custom metadata is missing");
    }
    var customMetadata = IcebergMetadata.deserialize(remoteLogSegmentMetadata.customMetadata().get().value());
    var tableName = TableIdentifier.of(customMetadata.originalSchema().getNamespace(), customMetadata.originalSchema().getName());
    var table = catalog.loadTable(tableName);
    try (var result = IcebergGenerics.read(table)
        .where(Expressions.equal(SEGMENT_BASE_OFFSET_KEY, remoteLogSegmentMetadata.startOffset()))
        .build()) {
      List<GenericRecord> records = new ArrayList<>();
      try (CloseableIterator<Record> iterator = result.iterator()) {
        while (iterator.hasNext()) {
          records.add((GenericRecord) iterator.next());
        }
      }
      return records;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
