package org.svv.iceberg.kafka.storage;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.svv.iceberg.kafka.IcebergTieredStorageConfig;
import org.svv.iceberg.kafka.schemas.SchemaContext;
import org.svv.iceberg.kafka.schemas.SchemaRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class IcebergRemoteStorageManager implements RemoteStorageManager {

  private IcebergWriter icebergWriter;
  private SchemaRegistry schemaRegistry;
  private int maxIndexSize;
  private int indexIntervalBytes;
  private long rollJitterMs;


  @Override
  public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData) throws RemoteStorageException {
    var records = readAll(logSegmentData);
    var schemaContext = new SchemaContext(remoteLogSegmentMetadata.topicIdPartition(), new RecordHeaders(new Header[] {}));
    var schema = schemaRegistry.valueSchemaByContext(schemaContext);
    icebergWriter.write(remoteLogSegmentMetadata, schema, records.records());
    return Optional.empty();
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int i) throws RemoteStorageException {
    // Reassemble a Kafka log segment from the Iceberg table and return it as an InputStream
    return null;
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int i, int i1) throws RemoteStorageException {
    // Reassemble a Kafka log segment from the Iceberg table and return it as an InputStream
    return null;
  }

  @Override
  public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) throws RemoteStorageException {
    return null;
  }

  @Override
  public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
    // Delete the log segment data from the Iceberg table
  }

  @Override
  public void close() throws IOException {
    // Close the Iceberg connection
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
  }

  @Override
  public void configure(Map<String, ?> map) {
    IcebergTieredStorageConfig configs = new IcebergTieredStorageConfig(map);
    // Schema registry initialization
    Class schemaRegistryClass = configs.getClass(IcebergTieredStorageConfig.SCHEMA_REGISTRY_CLASS_CONFIG);
    try {
      schemaRegistry = (SchemaRegistry) schemaRegistryClass.getDeclaredConstructor().newInstance();
      schemaRegistry.configure(configs.schemaRegistryConfigs());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate schema registry class", e);
    } catch (InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    // Iceberg writer initialization
    icebergWriter = new IcebergWriterImpl();
    icebergWriter.configure(configs.icebergConfigs());
    // Log segment reader initialization
    maxIndexSize = (int) configs.originals().get("log." + TopicConfig.SEGMENT_INDEX_BYTES_CONFIG);
    indexIntervalBytes = (int) configs.originals().get("log." + TopicConfig.INDEX_INTERVAL_BYTES_CONFIG);
    rollJitterMs = (long) configs.originals().get(TopicConfig.SEGMENT_JITTER_MS_CONFIG);
  }

  Records readAll(LogSegmentData logSegmentData) {
    FileRecords log = null;
    try {
      log = FileRecords.open(logSegmentData.logSegment().toFile());
      long logBaseOffset = baseOffset(logSegmentData.logSegment());
      LogSegment logSegment = new LogSegment(
          log,
          LazyIndex.forOffset(logSegmentData.offsetIndex().toFile(), baseOffset(logSegmentData.offsetIndex()), maxIndexSize),
          LazyIndex.forTime(logSegmentData.offsetIndex().toFile(), baseOffset(logSegmentData.timeIndex()), maxIndexSize),
          logSegmentData.transactionIndex().isPresent()
              ? new TransactionIndex(baseOffset(logSegmentData.transactionIndex().get()), logSegmentData.transactionIndex().get().toFile())
              : new TransactionIndex(logBaseOffset, LogFileUtils.transactionIndexFile(logSegmentData.logSegment().getParent().toFile(), logBaseOffset, "")),
          logBaseOffset,
          indexIntervalBytes,
          rollJitterMs,
          Time.SYSTEM);
      return logSegment.read(logBaseOffset, logSegment.size()).records;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static long baseOffset(Path path) {
    String filename = path.toFile().getName();
    return Long.parseLong(path.toFile().getName().substring(0, filename.indexOf('.')));
  }
}
