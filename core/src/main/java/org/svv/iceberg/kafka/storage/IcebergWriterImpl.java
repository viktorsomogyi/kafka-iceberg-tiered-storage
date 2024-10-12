package org.svv.iceberg.kafka.storage;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.IOException;
import java.util.Map;

import static org.svv.iceberg.kafka.storage.IcebergConverter.parseAvro;
import static org.svv.iceberg.kafka.storage.IcebergConverter.toIcebergRecord;

public class IcebergWriterImpl implements IcebergWriter {

  private Catalog catalog;

  @Override
  public void configure(Map<String, ?> map) {
    IcebergConfig icebergConfig = new IcebergConfig(map);
    var configuration = new Configuration();
    catalog = CatalogUtil.loadCatalog(icebergConfig.catalogImpl(),
                                      icebergConfig.catalogName(),
                                      icebergConfig.catalogProperties(),
                                      configuration);
  }

  @Override
  public void write(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records) {
    // Write the schema to the Iceberg table
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    var tableName = TableIdentifier.of(schema.getNamespace(), schema.getName());
    if (!catalog.tableExists(tableName)) {
      catalog.createTable(tableName, icebergSchema);
    }
    Table table = catalog.loadTable(tableName);
    writeToFile(remoteLogSegmentMetadata, schema, records, table, icebergSchema);
  }

  private static void writeToFile(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Schema schema, Iterable<Record> records, Table table, org.apache.iceberg.Schema icebergSchema) {
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
        dataWriter.write(toIcebergRecord(schema, parseAvro(record.value().array(), schema)));
      }
      table.newAppend().appendFile(dataWriter.toDataFile()).commit();
    } catch (IOException e) {
      throw new IcebergWriterException(e);
    }
  }

  private static String getFilepath(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Table table) {
    var topicId = remoteLogSegmentMetadata.topicIdPartition().topicId();
    var topicName = remoteLogSegmentMetadata.topicIdPartition().topicPartition().topic();
    var partition = remoteLogSegmentMetadata.topicIdPartition().topicPartition().partition();
    String filepath = table.location()
        + "/" + topicId
        + "/" + topicName + "-" + partition
        + "/" + remoteLogSegmentMetadata.remoteLogSegmentId().toString()
        + "/" + remoteLogSegmentMetadata.startOffset();
    return filepath;
  }
}
