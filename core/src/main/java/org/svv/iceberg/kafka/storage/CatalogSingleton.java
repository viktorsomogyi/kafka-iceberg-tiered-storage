package org.svv.iceberg.kafka.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

public class CatalogSingleton {

    private static Catalog catalog;

    public static void configure(IcebergConfig icebergConfig) {
      if (catalog != null) {
        return;
      }

      var configuration = new Configuration();
      catalog = CatalogUtil.loadCatalog(icebergConfig.catalogImpl(),
          icebergConfig.catalogName(),
          icebergConfig.catalogProperties(),
          configuration);
    }

    public static Catalog catalog() {
      return catalog;
    }
}
