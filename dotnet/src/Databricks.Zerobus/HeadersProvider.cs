namespace Databricks.Zerobus;

/// <summary>
/// Delegate for providing custom authentication headers for stream creation.
/// The returned dictionary should include keys like "authorization" and
/// "x-databricks-zerobus-table-name".
/// </summary>
/// <returns>A dictionary of header name to header value.</returns>
public delegate IReadOnlyDictionary<string, string> HeadersProviderDelegate();
