package org.svv.iceberg.kafka.schemas;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.header.Header;

public record SchemaContext(TopicIdPartition topicIdPartition) {
}
