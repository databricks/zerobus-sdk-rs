package com.databricks.zerobus;

import com.google.protobuf.Message;

/**
 * Table properties for the stream, describes the table to ingest records into.
 *
 * @param <RecordType> The type of records to be ingested (must extend Message).
 */
public class TableProperties<RecordType extends Message> {
  private final String tableName;
  private final Message defaultInstance;

  /**
   * Creates a new TableProperties instance.
   *
   * @param tableName The name of the table to ingest records into.
   * @param defaultInstance The default instance of the record type (used to get the descriptor).
   */
  public TableProperties(String tableName, RecordType defaultInstance) {
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("tableName must not be null or blank");
    }
    if (defaultInstance == null) {
      throw new IllegalArgumentException("defaultInstance must not be null");
    }
    this.tableName = tableName;
    this.defaultInstance = defaultInstance;
  }

  /**
   * Returns the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the default instance.
   *
   * @return the default instance
   */
  public Message getDefaultInstance() {
    return defaultInstance;
  }

  /**
   * Gets the DescriptorProto for the record type. This is used to send the schema to the server.
   *
   * @return the DescriptorProto
   */
  com.google.protobuf.DescriptorProtos.DescriptorProto getDescriptorProto() {
    return defaultInstance.getDescriptorForType().toProto();
  }
}
