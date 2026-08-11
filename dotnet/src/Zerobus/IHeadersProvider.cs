namespace Databricks.Zerobus;

/// <summary>
/// Interface for providing custom authentication headers.
/// Implement this interface to supply custom authentication logic
/// (e.g. fetching tokens from a vault, using managed identity, etc.).
/// </summary>
/// <remarks>
/// Lifetime: the SDK owns the provider for the stream's lifetime and releases it
/// only after any in-flight <see cref="GetHeaders"/> call (including one during
/// connection recovery) has returned, so you do not need to keep your own
/// reference alive past stream creation. <see cref="GetHeaders"/> may be invoked
/// from an internal SDK worker thread, not the thread that created the stream, so
/// implementations must be safe to call from any thread.
/// </remarks>
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
