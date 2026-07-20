namespace Databricks.Zerobus;

/// <summary>
/// Interface for providing custom authentication headers.
/// Implement this interface to supply custom authentication logic
/// (e.g. fetching tokens from a vault, using managed identity, etc.).
/// </summary>
/// <example>
/// <code>
/// public class CustomHeadersProvider : IHeadersProvider
/// {
///     public IDictionary&lt;string, string&gt; GetHeaders()
///     {
///         return new Dictionary&lt;string, string&gt;
///         {
///             ["authorization"] = "Bearer " + GetToken(),
///             ["x-databricks-zerobus-table-name"] = "catalog.schema.table",
///         };
///     }
/// }
/// </code>
/// </example>
public interface IHeadersProvider
{
    /// <summary>
    /// Returns the headers to be used for authentication.
    /// This method is called by the SDK whenever authentication is needed.
    /// </summary>
    /// <returns>A dictionary of header key-value pairs.</returns>
    /// <exception cref="Exception">If the headers cannot be obtained.</exception>
    IDictionary<string, string> GetHeaders();
}
