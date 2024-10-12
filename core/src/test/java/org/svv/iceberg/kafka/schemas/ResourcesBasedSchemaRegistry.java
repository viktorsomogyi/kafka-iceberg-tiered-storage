package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ResourcesBasedSchemaRegistry implements SchemaRegistry {

  public static final String SCHEMA_RESOURCES_DIRECTORY = "schema.resources.directory";

  private Map<String, Schema> schemas;

  @Override
  public Schema valueSchemaByContext(SchemaContext context) {
    return schemas.get(context.topicIdPartition().topic() + ".avsc");
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      var schemaUrls = ResourcesBasedSchemaRegistry.class.getClassLoader().getResources((String) configs.get(SCHEMA_RESOURCES_DIRECTORY));
      schemas = new HashMap<>();
      schemaUrls.asIterator().forEachRemaining(url -> {
        var schemasDirectory = Path.of(url.getPath()).toFile();
        for (var file : Objects.requireNonNull(schemasDirectory.listFiles())) {
          // Load the schema from the file
          InputStream schemaResourceStream = null;
          try {
            schemaResourceStream = Files.newInputStream(file.toPath());
          } catch (IOException e) {
            throw new RuntimeException("Schema file [" + file.getName() + "] can't be read", e);
          }
          try {
            schemas.put(file.getName(), new Schema.Parser().parse(schemaResourceStream));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
