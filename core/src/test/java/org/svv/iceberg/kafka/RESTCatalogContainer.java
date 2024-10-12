package org.svv.iceberg.kafka;

import org.testcontainers.containers.GenericContainer;

public class RESTCatalogContainer extends GenericContainer {

  public static final String ENV_REST_PORT = "REST_PORT";
  public static final String ENV_AWS_ACCESS_KEY_ID = "CATALOG_S3_ACCESS__KEY__ID";
  public static final String ENV_AWS_SECRET_ACCESS_KEY = "CATALOG_S3_SECRET__ACCESS__KEY";
  public static final String ENV_AWS_REGION = "AWS_REGION";
  public static final String ENV_CATALOG_WAREHOUSE = "CATALOG_WAREHOUSE";
  public static final String ENV_CATALOG_IO__IMPL = "CATALOG_IO__IMPL";
  public static final String ENV_CATALOG_S3_ENDPOINT = "CATALOG_S3_ENDPOINT";
  public static final String ENV_CATALOG_S3_PATH__STYLE__ACCESS = "CATALOG_S3_PATH__STYLE__ACCESS";

  private static final int REST_PORT = 8181;
  private static final String IMAGE_NAME = "tabulario/iceberg-rest";

  public RESTCatalogContainer() {
    super(IMAGE_NAME);
    addExposedPort(REST_PORT);
  }

  public String getRestURL() {
    return "http://localhost:" + getMappedPort(REST_PORT);
  }

  public String getWarehouse() {
    return (String) getEnvMap().get(ENV_CATALOG_WAREHOUSE);
  }

  public RESTCatalogContainer withAccessKey(String accessKey) {
    addEnv(ENV_AWS_ACCESS_KEY_ID, accessKey);
    return this;
  }

  public RESTCatalogContainer withSecretAccessKey(String secretAccessKey) {
    addEnv(ENV_AWS_SECRET_ACCESS_KEY, secretAccessKey);
    return this;
  }

  public RESTCatalogContainer withRegion(String region) {
    addEnv(ENV_AWS_REGION, region);
    return this;
  }

  public RESTCatalogContainer withWarehouse(String warehouse) {
    addEnv(ENV_CATALOG_WAREHOUSE, warehouse);
    return this;
  }

  public RESTCatalogContainer withCatalogIOImpl(String catalogIOImpl) {
    addEnv(ENV_CATALOG_IO__IMPL, catalogIOImpl);
    return this;
  }

  public RESTCatalogContainer withS3Endpoint(String s3Endpoint) {
    addEnv(ENV_CATALOG_S3_ENDPOINT, s3Endpoint);
    return this;
  }

  public RESTCatalogContainer withS3PathStyleAccess(boolean pathStyleAccess) {
    addEnv(ENV_CATALOG_S3_PATH__STYLE__ACCESS, String.valueOf(pathStyleAccess));
    return this;
  }
}
