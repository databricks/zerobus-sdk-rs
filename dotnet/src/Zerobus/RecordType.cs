namespace Databricks.Zerobus;

/// <summary>
/// Represents the type of records to ingest.
/// </summary>
public enum RecordType
{
    /// <summary>
    /// No specific record type.
    /// </summary>
    Unspecified = 0,

    /// <summary>
    /// Protocol Buffer encoded records.
    /// </summary>
    Proto = 1,

    /// <summary>
    /// JSON encoded records.
    /// </summary>
    Json = 2,
}
