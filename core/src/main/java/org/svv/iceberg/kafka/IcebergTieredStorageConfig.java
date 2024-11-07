package org.svv.iceberg.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.svv.iceberg.kafka.storage.IcebergConfig;
import org.svv.iceberg.kafka.storage.readers.IcebergFanoutReader;
import org.svv.iceberg.kafka.storage.writers.IcebergSequentialWriter;

import java.util.Map;
import java.util.Set;

public class IcebergTieredStorageConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String SCHEMA_REGISTRY_CLASS_CONFIG = "schema.registry.class";
  public static final String SCHEMA_REGISTRY_CLASS_DOCS = "Specifies the implementation of the schema registry to use.";

  public static final String SCHEMA_REGISTRY_CONFIGS_PREFIX = "schema.registry.";
  public static final String ICEBERG_CONFIGS_PREFIX = "iceberg.";

  public static final String ICEBERG_WRITER_CLASS_CONFIG = "iceberg.writer.class";
  public static final String ICEBERG_WRITER_CLASS_DOCS = "Specifies the implementation of the Iceberg writer to use.";

  public static final String ICEBERG_READER_CLASS_CONFIG = "iceberg.reader.class";
  public static final String ICEBERG_READER_CLASS_DOCS = "Specifies the implementation of the Iceberg reader to use.";

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
            SCHEMA_REGISTRY_CLASS_DOCS)
        .define(ICEBERG_WRITER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            IcebergSequentialWriter.class.getName(),
            ConfigDef.Importance.HIGH,
            ICEBERG_WRITER_CLASS_DOCS)
        .define(ICEBERG_READER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            IcebergFanoutReader.class.getName(),
            ConfigDef.Importance.HIGH,
            ICEBERG_READER_CLASS_DOCS);
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

  public IcebergConfig icebergConfigs() {
    return new IcebergConfig(originalsWithPrefix(ICEBERG_CONFIGS_PREFIX));
  }

  public Class<?> getIcebergWriterClass() {
    return getClass(ICEBERG_WRITER_CLASS_CONFIG);
  }

  public Class<?> getIcebergReaderClass() {
    return getClass(ICEBERG_READER_CLASS_CONFIG);
  }
}
