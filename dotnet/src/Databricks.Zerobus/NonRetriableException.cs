namespace Databricks.Zerobus;

/// <summary>
/// Represents a non-retryable Zerobus error. This exception is thrown when the SDK
/// encounters an error that will not succeed on retry (e.g., invalid schema, auth failure).
/// </summary>
public class NonRetriableException : ZerobusException
{
    /// <summary>
    /// Creates a new NonRetriableException.
    /// </summary>
    public NonRetriableException(string message) : base(message, isRetryable: false)
    {
    }

    /// <summary>
    /// Creates a new NonRetriableException with an inner exception.
    /// </summary>
    public NonRetriableException(string message, Exception? innerException)
        : base(message, isRetryable: false, innerException)
    {
    }
}
