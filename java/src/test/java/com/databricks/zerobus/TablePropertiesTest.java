package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TableProperties}. */
public class TablePropertiesTest {

  @Test
  void testTablePropertiesCreation() {
    String tableName = "catalog.schema.test_table";
    CityPopulationTableRow defaultInstance = CityPopulationTableRow.getDefaultInstance();

    TableProperties<CityPopulationTableRow> props =
        new TableProperties<>(tableName, defaultInstance);

    assertEquals(tableName, props.getTableName());
    assertEquals(defaultInstance, props.getDefaultInstance());
  }

  @Test
  void testGetDescriptorProto() {
    TableProperties<CityPopulationTableRow> props =
        new TableProperties<>("catalog.schema.table", CityPopulationTableRow.getDefaultInstance());

    DescriptorProtos.DescriptorProto descriptorProto = props.getDescriptorProto();

    assertNotNull(descriptorProto);
    // The descriptor proto should have the fields from the message
    assertTrue(descriptorProto.getFieldCount() > 0);
  }

  @Test
  void testTableNameWithThreeParts() {
    String tableName = "main.default.my_table";
    TableProperties<CityPopulationTableRow> props =
        new TableProperties<>(tableName, CityPopulationTableRow.getDefaultInstance());

    assertEquals(tableName, props.getTableName());
  }

  @Test
  void testTableNameFormats() {
    // Test various table name formats (the SDK should accept them)
    String[] validNames = {
      "catalog.schema.table",
      "main.default.air_quality",
      "my_catalog.my_schema.my_table",
      "catalog123.schema456.table789"
    };

    for (String name : validNames) {
      TableProperties<CityPopulationTableRow> props =
          new TableProperties<>(name, CityPopulationTableRow.getDefaultInstance());
      assertEquals(name, props.getTableName());
    }
  }

  @Test
  void testDescriptorProtoCanBeSerialized() {
    TableProperties<CityPopulationTableRow> props =
        new TableProperties<>("cat.sch.tbl", CityPopulationTableRow.getDefaultInstance());

    DescriptorProtos.DescriptorProto descriptorProto = props.getDescriptorProto();
    byte[] serialized = descriptorProto.toByteArray();

    // Should produce non-empty serialized form
    assertNotNull(serialized);
    assertTrue(serialized.length > 0);
  }

  @Test
  void testRejectsMissingArguments() {
    CityPopulationTableRow defaultInstance = CityPopulationTableRow.getDefaultInstance();

    assertThrows(
        IllegalArgumentException.class, () -> new TableProperties<>(null, defaultInstance));
    assertThrows(
        IllegalArgumentException.class, () -> new TableProperties<>("  ", defaultInstance));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TableProperties<CityPopulationTableRow>("catalog.schema.table", null));
  }
}
