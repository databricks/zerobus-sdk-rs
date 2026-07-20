namespace Databricks.Zerobus;

/// <summary>
/// Represents the properties of a Unity Catalog table as returned by the
/// Databricks Unity Catalog API. Used for proto schema generation.
/// </summary>
public sealed class TableProperties
{
    /// <summary>
    /// The fully qualified table name (catalog.schema.table).
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The table's columns as a list of column definitions.
    /// </summary>
    public IReadOnlyList<ColumnDefinition> Columns { get; }

    /// <summary>
    /// Creates a new TableProperties.
    /// </summary>
    public TableProperties(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
    }
}

/// <summary>
/// Represents a column definition from a Unity Catalog table.
/// </summary>
public sealed class ColumnDefinition
{
    /// <summary>The column name.</summary>
    public string Name { get; }

    /// <summary>The SQL data type string (e.g., "INT", "STRING", "BIGINT").</summary>
    public string TypeName { get; }

    /// <summary>Whether the column is nullable.</summary>
    public bool Nullable { get; }

    /// <summary>Optional column comment.</summary>
    public string? Comment { get; }

    /// <summary>
    /// Creates a new ColumnDefinition.
    /// </summary>
    public ColumnDefinition(string name, string typeName, bool nullable, string? comment = null)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        TypeName = typeName ?? throw new ArgumentNullException(nameof(typeName));
        Nullable = nullable;
        Comment = comment;
    }
}
