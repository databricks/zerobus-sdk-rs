namespace Databricks.Zerobus;

/// <summary>
/// Base exception class for all Zerobus SDK errors.
/// </summary>
public class ZerobusException : Exception
{
    /// <summary>
    /// Whether this error is retryable. Retryable errors can be automatically
    /// recovered from if stream recovery is enabled.
    /// </summary>
    public bool IsRetryable { get; }

    /// <summary>
    /// Creates a new ZerobusException.
    /// </summary>
    public ZerobusException(string message) : base(message)
    {
    }

    /// <summary>
    /// Creates a new ZerobusException with retryability information.
    /// </summary>
    public ZerobusException(string message, bool isRetryable) : base(message)
    {
        IsRetryable = isRetryable;
    }

    /// <summary>
    /// Creates a new ZerobusException with retryability and inner exception.
    /// </summary>
    public ZerobusException(string message, bool isRetryable, Exception? innerException)
        : base(message, innerException)
    {
        IsRetryable = isRetryable;
    }
}
