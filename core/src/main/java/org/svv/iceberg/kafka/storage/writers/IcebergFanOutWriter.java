package org.svv.iceberg.kafka.storage.writers;

import org.apache.avro.Schema;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.svv.iceberg.kafka.errors.IcebergWriterException;
import org.svv.iceberg.kafka.storage.CatalogSingleton;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.svv.iceberg.kafka.storage.IcebergConverter.parseAvro;
import static org.svv.iceberg.kafka.storage.IcebergConverter.toIcebergRecord;

/**
 * <p>
 * Iceberg writer that writes records to Iceberg tables using a fan-out strategy. It would be
 * more efficient in terms of storage, as it would allow for merging Parquet files. It is also
 * less efficient in terms of writing and reading from the Iceberg tables as records need to be
 * searched.
 * </p>
 * <p>It also expects that every schema has the following fields that will be populated by the writer:
 * <ul>
 *   <li>topic_id - the topic id of the topic being written out</li>
 *   <li>partition - the partition of the topic being written out</li>
 *   <li>logsegment_base_offset - the base offset of the original log segment</li>
 *   <li>record_offset - the offset of the record</li>
 * </ul>
 * These will be needed by the reader to reassemble log segments.
 * </p>
 */
public class IcebergFanOutWriter implements IcebergWriter {

  private final static String RECORD_OFFSET_KEY = "record_offset";
  private final static String SEGMENT_BASE_OFFSET_KEY = "logsegment_base_offset";
  private final static String PARTITION_KEY = "partition";
  private final static String TOPIC_ID_KEY = "topic_id";

  private Map<Table, TaskWriter<org.apache.iceberg.data.Record>> tableWriters;

  @Override
  public void configure(Map<String, ?> map) {
    tableWriters = new HashMap<>();
  }

  @Override
  public WriteResult write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records) {
    if (schema.getField(RECORD_OFFSET_KEY) == null) {
      throw new IcebergWriterException("Schema must contain a field named '" + RECORD_OFFSET_KEY + "'");
    }
    if (schema.getField(SEGMENT_BASE_OFFSET_KEY) == null) {
      throw new IcebergWriterException("Schema must contain a field named '" + SEGMENT_BASE_OFFSET_KEY + "'");
    }
    if (schema.getField(PARTITION_KEY) == null) {
      throw new IcebergWriterException("Schema must contain a field named '" + PARTITION_KEY + "'");
    }
    if (schema.getField(TOPIC_ID_KEY) == null) {
      throw new IcebergWriterException("Schema must contain a field named '" + TOPIC_ID_KEY + "'");
    }
    var catalog = CatalogSingleton.catalog();
    // Write the schema to the Iceberg table
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    var tableName = TableIdentifier.of(schema.getNamespace(), schema.getName());
    var partitionSpec = PartitionSpec.builderFor(icebergSchema)
        .identity(SEGMENT_BASE_OFFSET_KEY)
        .build();
    if (!catalog.tableExists(tableName)) {
      catalog.createTable(tableName, icebergSchema, partitionSpec);
    }
    Table table = catalog.loadTable(tableName);
    return writeToIceberg(remoteLogSegmentMetadata, schema, records, table, icebergSchema);
  }

  private WriteResult writeToIceberg(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                     Schema schema,
                                     Iterable<Record> records,
                                     Table table,
                                     org.apache.iceberg.Schema icebergSchema) {
    var writer = tableWriters.getOrDefault(table, createWriter(table, remoteLogSegmentMetadata, icebergSchema));
    try {
      for (Record record : records) {
        var avroRecord = parseAvro(record, schema);
        avroRecord.put(RECORD_OFFSET_KEY, record.offset());
        avroRecord.put(SEGMENT_BASE_OFFSET_KEY, remoteLogSegmentMetadata.startOffset());
        avroRecord.put(PARTITION_KEY, remoteLogSegmentMetadata.topicIdPartition().partition());
        avroRecord.put(TOPIC_ID_KEY, remoteLogSegmentMetadata.topicIdPartition().topicId().toString());
        writer.write(toIcebergRecord(schema, avroRecord));
      }
      return writer.complete();
    } catch (IOException e) {
      throw new IcebergWriterException(e);
    }
  }

  private TaskWriter<org.apache.iceberg.data.Record> createWriter(Table table,
                                                                  RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                                  org.apache.iceberg.Schema icebergSchema) {
    var partitionId = remoteLogSegmentMetadata.remoteLogSegmentId().id().hashCode();
    var outputFileFactory = OutputFileFactory.builderFor(table, partitionId, System.currentTimeMillis()).build();
    var fileAppenderFactory = new GenericAppenderFactory(icebergSchema);
    return new IcebergPartitionedFanoutWriter(table, FileFormat.PARQUET,
        fileAppenderFactory, outputFileFactory, remoteLogSegmentMetadata.segmentSizeInBytes());
  }

  @Override
  public void close() throws IOException {
    tableWriters.forEach((table, writer) -> {
      try {
        writer.close();
      } catch (IOException e) {
        throw new IcebergWriterException(e);
      }
    });
  }

  private static class IcebergPartitionedFanoutWriter extends PartitionedFanoutWriter<org.apache.iceberg.data.Record> {

    private final Table table;

    protected IcebergPartitionedFanoutWriter(Table table,
                                             FileFormat format,
                                             FileAppenderFactory<org.apache.iceberg.data.Record> appenderFactory,
                                             OutputFileFactory fileFactory,
                                             long targetFileSize) {
      super(table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
      this.table = table;
    }

    @Override
    protected PartitionKey partition(org.apache.iceberg.data.Record row) {
      var partitionKey = new PartitionKey(spec(), table.schema());
      partitionKey.partition(row);
      return partitionKey;
    }
  }
}
