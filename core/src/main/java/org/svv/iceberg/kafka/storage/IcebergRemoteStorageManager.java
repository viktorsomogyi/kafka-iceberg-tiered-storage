package org.svv.iceberg.kafka.storage;

import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.common.compress.NoCompression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.svv.iceberg.kafka.AvroUtils;
import org.svv.iceberg.kafka.IcebergTieredStorageConfig;
import org.svv.iceberg.kafka.schemas.IcebergMetadata;
import org.svv.iceberg.kafka.schemas.SchemaContext;
import org.svv.iceberg.kafka.schemas.SchemaRegistry;
import org.svv.iceberg.kafka.storage.readers.IcebergReader;
import org.svv.iceberg.kafka.storage.writers.IcebergWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class IcebergRemoteStorageManager implements RemoteStorageManager {

  private static final Logger log = LoggerFactory.getLogger(IcebergRemoteStorageManager.class);

  private IcebergWriter icebergWriter;
  private IcebergReader icebergReader;
  private SchemaRegistry schemaRegistry;
  private int maxIndexSize;
  private int indexIntervalBytes;
  private long rollJitterMs;


  @Override
  public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData) throws RemoteStorageException {
    log.info("Writing log segment data to Iceberg table: {}", remoteLogSegmentMetadata);
    var records = readAll(logSegmentData);
    var schemaContext = new SchemaContext(remoteLogSegmentMetadata.topicIdPartition());
    var schema = schemaRegistry.valueSchemaByContext(schemaContext);
    var writeResult = icebergWriter.write(remoteLogSegmentMetadata, schema, records.records());
    var icebergMetadata = new IcebergMetadata(Arrays.stream(writeResult.dataFiles())
        .map(df -> df.path().toString())
        .toList(), schema);
    log.info("Wrote log segment data to Iceberg table: {}", icebergMetadata);
    return Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(icebergMetadata.serialize()));
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startOffset) throws RemoteStorageException {
    // Reassemble a Kafka log segment from the Iceberg table and return it as an InputStream
    var avroRecords = icebergReader.read(remoteLogSegmentMetadata);
    return recordsToLogSegment(remoteLogSegmentMetadata, avroRecords);
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int i, int i1) throws RemoteStorageException {
    // Reassemble a Kafka log segment from the Iceberg table and return it as an InputStream
    var avroRecords = icebergReader.read(remoteLogSegmentMetadata);
    return recordsToLogSegment(remoteLogSegmentMetadata, avroRecords);
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
    CatalogSingleton.configure(configs.icebergConfigs());
    initSchemaRegistry(configs);
    initIcebergWriter(configs);
    initIcebergReader(configs);
    // Log segment reader initialization
    maxIndexSize = (int) configs.originals().getOrDefault("log." + TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 10 * 1024 * 1024);
    indexIntervalBytes = (int) configs.originals().getOrDefault("log." + TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 4096);
    rollJitterMs = (long) configs.originals().getOrDefault(TopicConfig.SEGMENT_JITTER_MS_CONFIG, 1000L);
  }

  private void initSchemaRegistry(IcebergTieredStorageConfig configs) {
    Class<?> schemaRegistryClass = configs.getClass(IcebergTieredStorageConfig.SCHEMA_REGISTRY_CLASS_CONFIG);
    try {
      schemaRegistry = (SchemaRegistry) schemaRegistryClass.getDeclaredConstructor().newInstance();
      schemaRegistry.configure(configs.schemaRegistryConfigs());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate schema registry class", e);
    } catch (InvocationTargetException | NoSuchMethodException e) {
      throw new IcebergRemoteStorageException(e);
    }
  }

  private void initIcebergWriter(IcebergTieredStorageConfig configs) {
    Class<?> icebergWriterClass = configs.getIcebergWriterClass();
    try {
      icebergWriter = (IcebergWriter) icebergWriterClass.getDeclaredConstructor().newInstance();
      icebergWriter.configure(configs.icebergConfigs().values());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate schema registry class", e);
    } catch (InvocationTargetException | NoSuchMethodException e) {
      throw new IcebergRemoteStorageException(e);
    }
  }

  private void initIcebergReader(IcebergTieredStorageConfig configs) {
    Class<?> icebergReaderClass = configs.getIcebergReaderClass();
    try {
      icebergReader = (IcebergReader) icebergReaderClass.getDeclaredConstructor().newInstance();
      icebergReader.configure(configs.icebergConfigs().values());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate schema registry class", e);
    } catch (InvocationTargetException | NoSuchMethodException e) {
      throw new IcebergRemoteStorageException(e);
    }
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
      throw new IcebergRemoteStorageException(e);
    }
  }

  InputStream recordsToLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, List<GenericRecord> records) {
    try {
      if (remoteLogSegmentMetadata.customMetadata().isEmpty()) {
        throw new IcebergRemoteStorageException("Custom metadata is missing");
      }
      var logSegment = createLogSegment(remoteLogSegmentMetadata.startOffset());
      var customMetadata = IcebergMetadata.deserialize(remoteLogSegmentMetadata.customMetadata().get().value());
      populateLogSegment(logSegment, records, customMetadata.originalSchema());
      return new FileInputStream(logSegment.log().file());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  LogSegment createLogSegment(long startOffset) {
    try {
      var prefix = UUID.randomUUID().toString();
      var tmpLogFile = Files.createTempFile(prefix, ".log");
      var tmpOffsetIdxFile = Files.createTempFile(prefix, ".index");
      var tmpTimeIdxFile = Files.createTempFile(prefix, ".timeindex");
      var log = FileRecords.open(tmpLogFile.toFile());
      return new LogSegment(
          log,
          LazyIndex.forOffset(tmpOffsetIdxFile.toFile(), startOffset, maxIndexSize),
          LazyIndex.forTime(tmpTimeIdxFile.toFile(), startOffset, maxIndexSize),
          new TransactionIndex(startOffset, LogFileUtils.transactionIndexFile(tmpLogFile.getParent().toFile(), startOffset, "")),
          startOffset,
          indexIntervalBytes,
          rollJitterMs,
          Time.SYSTEM);
    } catch (IOException e) {
      throw new IcebergRemoteStorageException(e);
    }
  }

  void populateLogSegment(LogSegment logSegment, List<GenericRecord> icebergRecords, Schema avroSchema) {
    try {
      for (var icebergRecord : icebergRecords) {
        var avroRecord = IcebergConverter.toAvroRecord(avroSchema, icebergRecord);
        var simpleRecord = new SimpleRecord(AvroUtils.serialize(avroRecord));
        var memoryRecord = MemoryRecords.withRecords(NoCompression.NONE, simpleRecord);
        long recordOffset = (Long) icebergRecord.getField("record_offset");
        logSegment.append(recordOffset, System.currentTimeMillis(), recordOffset, memoryRecord);
      }
    } catch (IOException e) {
      throw new IcebergRemoteStorageException(e);
    }
  }

  static long baseOffset(Path path) {
    String filename = path.toFile().getName();
    return Long.parseLong(path.toFile().getName().substring(0, filename.indexOf('.')));
  }

  // visible for testing
  void maxIndexSize(int maxIndexSize) {
    this.maxIndexSize = maxIndexSize;
  }

  // visible for testing
  void indexIntervalBytes(int indexIntervalBytes) {
    this.indexIntervalBytes = indexIntervalBytes;
  }
}
