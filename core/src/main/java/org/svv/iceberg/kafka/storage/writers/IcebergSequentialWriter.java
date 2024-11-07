package org.svv.iceberg.kafka.storage.writers;

import org.apache.avro.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.svv.iceberg.kafka.errors.IcebergWriterException;
import org.svv.iceberg.kafka.storage.CatalogSingleton;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.svv.iceberg.kafka.storage.IcebergConverter.parseAvro;
import static org.svv.iceberg.kafka.storage.IcebergConverter.toIcebergRecord;

/**
 * <p>
 * Iceberg writer that writes records to Iceberg tables sequentially.
 * It maps Parquet files to log segments one to one. It may be quicker to write to Iceberg tables sequentially
 * and quicker to read from them sequentially, but the Parquet files can't be merged, therefore less efficient
 * in storage.
 * </p>
 * <p>
 * It doesn't require any specific fields in the schema, although reading isn't agnostic to the storage.
 * </p>
 */
public class IcebergSequentialWriter implements IcebergWriter {

  @Override
  public void configure(Map<String, ?> map) {
  }

  @Override
  public WriteResult write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records) {
    var catalog = CatalogSingleton.catalog();
    // Write the schema to the Iceberg table
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    var tableName = TableIdentifier.of(schema.getNamespace(), schema.getName());
    if (!catalog.tableExists(tableName)) {
      catalog.createTable(tableName, icebergSchema);
    }
    Table table = catalog.loadTable(tableName);
    return writeToIceberg(remoteLogSegmentMetadata, schema, records, table, icebergSchema);
  }

  private static WriteResult writeToIceberg(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records, Table table, org.apache.iceberg.Schema icebergSchema) {
    String filepath = getFilepath(remoteLogSegmentMetadata, table);
    OutputFile file = table.io().newOutputFile(filepath);
    DataWriter<GenericRecord> dataWriter;
    try {
      dataWriter = Parquet.writeData(file)
          .schema(icebergSchema)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .overwrite()
          .withSpec(PartitionSpec.unpartitioned())
          .build();
    } catch (IOException e) {
      throw new IcebergWriterException(e);
    }
    // write the record
    try (dataWriter) {
      for (Record record : records) {
        var avroRecord = parseAvro(record, schema);
        dataWriter.write(toIcebergRecord(schema, avroRecord));
      }
    } catch (IOException e) {
      throw new IcebergWriterException(e);
    }
    var result = WriteResult.builder().addDataFiles(dataWriter.toDataFile()).build();
    Arrays.stream(result.dataFiles()).sequential().forEach(dataFile -> table.newAppend().appendFile(dataFile).commit());
    return result;
  }

  private static String getFilepath(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Table table) {
    var topicId = remoteLogSegmentMetadata.topicIdPartition().topicId();
    var topicName = remoteLogSegmentMetadata.topicIdPartition().topicPartition().topic();
    var partition = remoteLogSegmentMetadata.topicIdPartition().topicPartition().partition();
    return table.location()
        + "/" + topicName + "-" + partition + "-" + topicId
        + "/" + remoteLogSegmentMetadata.remoteLogSegmentId().id().toString()
        + "/" + remoteLogSegmentMetadata.startOffset();
  }

  @Override
  public void close() throws IOException {

  }
}
