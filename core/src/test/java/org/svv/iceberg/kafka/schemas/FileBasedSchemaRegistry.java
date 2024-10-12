package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FileBasedSchemaRegistry implements SchemaRegistry {

  public static final String SCHEMA_FILES_DIRECTORY = "schema.files.directory";

  private Map<String, Schema> schemas;


  @Override
  public Schema valueSchemaByContext(SchemaContext context) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    var schemaFilesDirectory = Path.of((String) configs.get(SCHEMA_FILES_DIRECTORY));
    schemas = new HashMap<>();
    // Load the Avro schemas from the provided files
    var files = schemaFilesDirectory.toFile().listFiles();
    if (files == null) {
      throw new IllegalArgumentException("Given schema files directory [" + schemaFilesDirectory + "] does not exist");
    }
    for (var file : files) {
      // Load the schema from the file
      InputStream schemaResourceStream = null;
      try {
        schemaResourceStream = Files.newInputStream(file.toPath());
      } catch (IOException e) {
        throw new RuntimeException("Given schema file [" + file.getName() + "] can't be read", e);
      }
      try {
        schemas.put(file.getName(), new Schema.Parser().parse(schemaResourceStream));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
