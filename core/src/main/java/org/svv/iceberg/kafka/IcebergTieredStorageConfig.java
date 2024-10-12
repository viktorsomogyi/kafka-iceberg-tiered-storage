package org.svv.iceberg.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

public class IcebergTieredStorageConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String SCHEMA_REGISTRY_CLASS_CONFIG = "schema.registry.class";
  public static final String SCHEMA_REGISTRY_CLASS_DOCS = "Specifies the implementation of the schema registry to use.";

  public static final String SCHEMA_REGISTRY_CONFIGS_PREFIX = "schema.registry.config.";
  public static final String ICEBERG_CONFIGS_PREFIX = "iceberg.config.";
  public static final String ICEBERG_CATALOG_CONFIGS_PREFIX = "iceberg.catalog.config.";

  public IcebergTieredStorageConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
  }

  public IcebergTieredStorageConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
  }

  static {
    CONFIG = new ConfigDef()
        .define(SCHEMA_REGISTRY_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.HIGH,
            SCHEMA_REGISTRY_CLASS_DOCS);
  }

  public static Set<String> configNames() {
    return CONFIG.names();
  }

  public static ConfigDef configDef() {
    return  new ConfigDef(CONFIG);
  }

  public Map<String, Object> schemaRegistryConfigs() {
    return originalsWithPrefix(SCHEMA_REGISTRY_CONFIGS_PREFIX);
  }

  public Map<String, Object> icebergConfigs() {
    return originalsWithPrefix(ICEBERG_CONFIGS_PREFIX);
  }

  public Map<String, Object> icebergCatalogConfigs() {
    return originalsWithPrefix(ICEBERG_CATALOG_CONFIGS_PREFIX);
  }
}
