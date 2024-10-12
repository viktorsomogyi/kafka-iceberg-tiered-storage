package org.svv.iceberg.kafka.storage.utils;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.svv.iceberg.kafka.RESTCatalogContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;

public class IcebergMinioTestHarness {

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

  protected Network network;

  @BeforeEach
  public void setup() throws Exception {
    // create a common network
    network = Network.newNetwork();

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
    if (network != null) {
      network.close();
    }
  }

  public Catalog createRestCatalog() {
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

  public S3Client createLocalS3Client() {
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
