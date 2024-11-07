package org.svv.iceberg.kafka.storage.utils;

import org.apache.iceberg.rest.RESTCatalog;
import org.svv.iceberg.kafka.IcebergTieredStorageConfig;
import org.svv.iceberg.kafka.schemas.ResourcesBasedSchemaRegistry;
import org.svv.iceberg.kafka.storage.IcebergConfig;
import org.svv.iceberg.kafka.storage.IcebergRemoteStorageManager;

import java.util.HashMap;
import java.util.Map;

public abstract class IcebergKafkaTestHarness extends KafkaTestHarness {

  private static final String RSM_CONFIG_PREFIX = "rsm.config.";
  private static final String SR_CONFIG_PREFIX = "rsm.config." + IcebergTieredStorageConfig.SCHEMA_REGISTRY_CONFIGS_PREFIX;
  private static final String IB_CONFIG_PREFIX = "rsm.config." + IcebergTieredStorageConfig.ICEBERG_CONFIGS_PREFIX;
  private static final String CAT_CONFIG_PREFIX = IB_CONFIG_PREFIX + IcebergConfig.ICEBERG_CATALOG_CONFIGS_PREFIX;

  @Override
  public Map<String, String> extraBrokerConfigs() {
    var configs = super.extraBrokerConfigs();
    configs.putAll(tieredStorageGenericConfigs());
    configs.putAll(baseKafkaConfigs());
    configs.putAll(icebergPluginConfigs());
    return configs;
  }

  private Map<String, String> tieredStorageGenericConfigs() {
    var brokerConfigs = new HashMap<String, String>();
    brokerConfigs.put("remote.log.storage.system.enable", "true");
    brokerConfigs.put("remote.log.storage.manager.impl.prefix", RSM_CONFIG_PREFIX);
    brokerConfigs.put("remote.log.manager.task.interval.ms", "1000");
    brokerConfigs.put("remote.log.metadata.manager.impl.prefix", "rlmm.config.");
    brokerConfigs.put("remote.log.metadata.manager.listener.name", "PLAINTEXT");
    brokerConfigs.put("rlmm.config.remote.log.metadata.topic.replication.factor", "1");
    return brokerConfigs;
  }

  private Map<String, String> baseKafkaConfigs() {
    var brokerConfigs = new HashMap<String, String>();
    brokerConfigs.put("log.retention.check.interval.ms", "1000");
    brokerConfigs.put("log.local.retention.ms", "2000");
    brokerConfigs.put("log.segment.delete.delay.ms", "1000");
//    brokerConfigs.put("file.delete.delay.ms", "5000");
//    brokerConfigs.put("log.local.retention.bytes", "10485760");
    brokerConfigs.put("log.segment.bytes", "1048576");
    return brokerConfigs;
  }

  private Map<String, String> icebergPluginConfigs() {
    Map<String, String> catalogConfigs = new HashMap<>();
    catalogConfigs.put("remote.log.storage.manager.class.name", IcebergRemoteStorageManager.class.getName());
    catalogConfigs.put(RSM_CONFIG_PREFIX + "schema.registry.class", ResourcesBasedSchemaRegistry.class.getName());
    catalogConfigs.put(SR_CONFIG_PREFIX + ResourcesBasedSchemaRegistry.SCHEMA_RESOURCES_DIRECTORY, "schemas");
    catalogConfigs.put(RSM_CONFIG_PREFIX + IcebergTieredStorageConfig.ICEBERG_WRITER_CLASS_CONFIG, icebergWriterClassName());
    catalogConfigs.put(RSM_CONFIG_PREFIX + IcebergTieredStorageConfig.ICEBERG_READER_CLASS_CONFIG, icebergReaderClassName());
    catalogConfigs.put(IB_CONFIG_PREFIX + IcebergConfig.CATALOG_IMPL_CONFIG, RESTCatalog.class.getName());
    catalogConfigs.put(IB_CONFIG_PREFIX + IcebergConfig.CATALOG_NAME_CONFIG, "test-catalog");
    catalogConfigs.putAll(catalogConfigs());
    return catalogConfigs;
  }

  private Map<String, String> catalogConfigs() {
    Map<String, String> catalogConfigs = new HashMap<>();
    getCatalogConfigs().forEach((k, v) -> {
      catalogConfigs.put(CAT_CONFIG_PREFIX + k, v);
    });
    return catalogConfigs;
  }

  public abstract String icebergWriterClassName();

  public abstract String icebergReaderClassName();
}
