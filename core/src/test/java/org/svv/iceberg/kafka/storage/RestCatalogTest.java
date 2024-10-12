package org.svv.iceberg.kafka.storage;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.avro.Schema;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.svv.iceberg.kafka.RESTCatalogContainer;
import org.svv.iceberg.kafka.schemas.Device;
import org.svv.iceberg.kafka.schemas.ResourcesBasedSchemaRegistry;
import org.svv.iceberg.kafka.schemas.SchemaContext;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestCatalogTest {

  private static final String ICEBERG_BUCKET = "iceberg-test";
  private static final String AWS_ACCESS_KEY = "minioadmin";
  private static final String AWS_SECRET_KEY = "minioadmin";
  private static final String AWS_REGION = "us-east-1";
  private static final int MINIO_PORT = 9000;
  private static final int MINIO_UI_PORT = 9001;
  private static final int REST_PORT = 8181;
  private static final Logger minioLogger = LoggerFactory.getLogger("minio-logger");
  private static final Logger restLogger = LoggerFactory.getLogger("rest-logger");
  private static final String MINIO_IMAGE = "minio/minio:RELEASE.2024-09-13T20-26-02Z";

  private MinIOContainer minIOContainer;
  private RESTCatalogContainer restCatalogContainer;
  private MinioClient minioClient;

  @BeforeEach
  public void setUp() throws Exception {
    Runtime.getRuntime().addShutdownHook(new Thread(this::tearDown));

    // create a common network
    Network network = Network.newNetwork();

    // create minio container
    minIOContainer = new MinIOContainer(MINIO_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("minio")
        .withLogConsumer(new Slf4jLogConsumer(minioLogger));
    minIOContainer.start();

    // create a minio client
    minioClient = MinioClient
        .builder()
        .endpoint(minIOContainer.getS3URL())
        .credentials(minIOContainer.getUserName(), minIOContainer.getPassword())
        .build();
    System.out.println("MinIO URL: http://localhost:" + minIOContainer.getMappedPort(MINIO_UI_PORT));
    System.out.println("MinIO S3 URL: " + minIOContainer.getS3URL());

    // create test bucket
    var bucketArgs = MakeBucketArgs.builder().bucket(ICEBERG_BUCKET).build();
    minioClient.makeBucket(bucketArgs);

    // create the rest catalog container
    restCatalogContainer = new RESTCatalogContainer()
        .withWarehouse("s3a://" + ICEBERG_BUCKET + "/warehouse")
        .withCatalogIOImpl(S3FileIO.class.getName())
        .withS3Endpoint("http://minio:9000")
        .withAccessKey(minIOContainer.getUserName())
        .withSecretAccessKey(minIOContainer.getPassword())
        .withS3PathStyleAccess(true)
        .withRegion(AWS_REGION);
    // set generic properties
    restCatalogContainer
        .withNetwork(network)
        .dependsOn(minIOContainer);
    restCatalogContainer.withLogConsumer(new Slf4jLogConsumer(restLogger));
    restCatalogContainer.start();

    Startables.deepStart(Stream.of(minIOContainer, restCatalogContainer)).join();

    System.out.println("REST Catalog URL: " + restCatalogContainer.getRestURL());
    System.out.println("Setup done");
  }

  @AfterEach
  public void tearDown() {
    if (restCatalogContainer != null) {
      restCatalogContainer.stop();
    }
    try {
      if (minioClient != null) {
        minioClient.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (minIOContainer != null) {
      minIOContainer.stop();
      minIOContainer.close();
    }
  }

  @Test
  public void testCreateBucket() throws Exception {
    // create test bucket
    var bucketArgs = MakeBucketArgs.builder().bucket(ICEBERG_BUCKET).build();
    minioClient.makeBucket(bucketArgs);
    assertTrue(minioClient.bucketExists(BucketExistsArgs.builder().bucket(ICEBERG_BUCKET).build()));
  }

  @Test
  public void testS3ClientCreateBucket() {
    try (S3Client s3 = initLocalS3Client()) {
      s3.createBucket(req -> req.bucket("bucket"));
    }
  }

  @Test
  public void testResourcesSchemaRegistry() throws IOException {
    try (var registry = new ResourcesBasedSchemaRegistry()) {
      registry.configure(new HashMap<>() {{
        put(ResourcesBasedSchemaRegistry.SCHEMA_RESOURCES_DIRECTORY, "schemas");
      }});
      var context = new SchemaContext(new TopicIdPartition(Uuid.randomUuid(), 0, "devices"), Collections.emptyList());
      assertNotNull(registry.valueSchemaByContext(context));
    }
  }

  @Test
  public void testDataWrite() throws Exception {
    // create catalog
    try (RESTCatalog catalog = createCatalog()) {
      // load schema
      Schema deviceAvroSchema = null;
      try (var registry = new ResourcesBasedSchemaRegistry()) {
        registry.configure(new HashMap<>() {{
          put(ResourcesBasedSchemaRegistry.SCHEMA_RESOURCES_DIRECTORY, "schemas");
        }});
        var context = new SchemaContext(new TopicIdPartition(Uuid.randomUuid(), 0, "devices"), Collections.emptyList());
        deviceAvroSchema = registry.valueSchemaByContext(context);
      }

      // convert to iceberg schema
      org.apache.iceberg.Schema deviceSchema = AvroSchemaUtil.toIceberg(deviceAvroSchema);

      // create namespace
      Namespace namespace = Namespace.of("kafka-logs");
      try {
        if (!catalog.namespaceExists(namespace)) {
          catalog.createNamespace(namespace);
        }

        // create table
        TableIdentifier tableName = TableIdentifier.of(namespace, "device");
        try {
          if (!catalog.tableExists(tableName)) {
            catalog.createTable(tableName, deviceSchema);
          }
          assertEquals(1, catalog.listTables(namespace).size());
          // test data
          var device = new Device(1, "device-1", 1, System.currentTimeMillis());
          var icebergRecord = device.toIceberg(deviceSchema);
          // get table
          var table = catalog.loadTable(tableName);
          // create a new file
          String filepath = table.location() + "/" + UUID.randomUUID();
//          GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
//          var outputFileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
//          var dataWriter = new UnpartitionedWriter<>(table.spec(), FileFormat.PARQUET, appenderFactory, outputFileFactory, table.io(), 1024 * 1024 * 1024);
          OutputFile file = table.io().newOutputFile(filepath);
          DataWriter<org.apache.iceberg.data.GenericRecord> dataWriter =
              Parquet.writeData(file)
                  .schema(deviceSchema)
                  .createWriterFunc(GenericParquetWriter::buildWriter)
                  .overwrite()
                  .withSpec(PartitionSpec.unpartitioned())
                  .build();
          // write the record
          try (dataWriter) {
            dataWriter.write(icebergRecord);
          }
          table.newAppend().appendFile(dataWriter.toDataFile()).commit();
          // commit the file
//          Arrays.stream(dataWriter.dataFiles()).forEach(file -> {
//            table.newAppend().appendFile(file).commit();
//          });
        } finally {
          catalog.dropTable(tableName);
        }
      } finally {
        catalog.dropNamespace(namespace);
      }
    }
  }

  private static String readResourceAsString(String resourceName) throws IOException {
    try (InputStream is = RestCatalogTest.class.getClassLoader().getResourceAsStream(resourceName)) {
      return new String(Objects.requireNonNull(is).readAllBytes());
    }
  }

  public RESTCatalog createCatalog() {
    RESTCatalog result = new RESTCatalog();
    String catalogUrl = "http://" + restCatalogContainer.getHost() + ":" + restCatalogContainer.getMappedPort(REST_PORT);
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, catalogUrl)
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put("s3.endpoint", "http://localhost:" + getHostMinioPort())
            .put("s3.access-key-id", AWS_ACCESS_KEY)
            .put("s3.secret-access-key", AWS_SECRET_KEY)
            .put("s3.path-style-access", "true")
            .put("client.region", AWS_REGION)
            .build());
    return result;
  }

  private int getHostMinioPort() {
    return minIOContainer.getMappedPort(MINIO_PORT);
  }

  public S3Client initLocalS3Client() {
    try {
      return S3Client.builder()
          .endpointOverride(new URI("http://localhost:" + getHostMinioPort()))
          .region(Region.of(AWS_REGION))
          .forcePathStyle(true)
          .credentialsProvider(
              StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
          .build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
