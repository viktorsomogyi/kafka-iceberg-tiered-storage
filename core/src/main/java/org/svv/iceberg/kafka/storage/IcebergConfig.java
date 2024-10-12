package org.svv.iceberg.kafka.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class IcebergConfig extends AbstractConfig {

  public static final String CATALOG_IMPL_CONFIG = "catalog.impl";
  public static final String CATALOG_NAME_CONFIG = "catalog.name";
  public static final String CATALOG_PROPERTIES_CONFIG_PREFIX = "catalog.properties.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CATALOG_IMPL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Catalog implementation class")
      .define(CATALOG_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The name of the catalog");

  public IcebergConfig(Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);
  }

  public String catalogImpl() {
    return getString(CATALOG_IMPL_CONFIG);
  }

  public String catalogName() {
    return getString(CATALOG_NAME_CONFIG);
  }

  public Map<String, String> catalogProperties() {
    var catalogPropsOriginals = originalsWithPrefix(CATALOG_PROPERTIES_CONFIG_PREFIX);
    var catalogProps = new HashMap<String, String>();
    for (var entry : catalogPropsOriginals.entrySet()) {
      catalogProps.put(entry.getKey(), (String) entry.getValue());
    }
    return catalogProps;
  }
}
