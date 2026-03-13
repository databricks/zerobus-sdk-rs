namespace Databricks.Zerobus;

/// <summary>
/// Contains information about the target Databricks Delta table.
/// </summary>
/// <param name="TableName">
/// Fully qualified table name in the form <c>catalog.schema.table</c>.
/// </param>
/// <param name="DescriptorProto">
/// Protocol buffer descriptor (required for <see cref="RecordType.Proto"/> streams, null for JSON).
/// This should be a serialized <c>DescriptorProto</c>.
/// </param>
public sealed record TableProperties(
    string TableName,
    byte[]? DescriptorProto = null);
