package org.svv.iceberg.kafka.schemas;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class IcebergMetadataTest {

  @Test
  public void testCreateIcebergMetadata() {
    var metadata = new IcebergMetadata(Arrays.stream(new String[] { "file1", "file2" }).toList(), null);
    assertNotNull(metadata);
  }

  @Test
  public void testSerializeIcebergMetadata() throws IOException {
    var testSchema = new Schema.Parser().parse(IcebergMetadata.class.getClassLoader().getResourceAsStream("iceberg_metadata.avsc"));
    var metadata = new IcebergMetadata(Arrays.stream(new String[] { "file1", "file2" }).toList(), testSchema);
    var serialized = metadata.serialize();
    assertNotNull(serialized);
  }
}
