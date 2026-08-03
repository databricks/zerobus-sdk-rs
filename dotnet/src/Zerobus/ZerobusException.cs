namespace Databricks.Zerobus;

/// <summary>
/// Represents an error from the Zerobus SDK.
/// Errors are categorised as retryable (transient) or non-retryable (fatal).
/// </summary>
/// <remarks>
/// The SDK automatically recovers from retryable errors when
/// <see cref="StreamConfigurationOptions.Recovery"/> is enabled.
/// Non-retryable errors require manual intervention (e.g. fixing permissions or schema).
/// </remarks>
public sealed class ZerobusException : Exception
{
    /// <summary>
    /// Initialises a new <see cref="ZerobusException"/>.
    /// </summary>
    /// <param name="message">The error message from the native layer.</param>
    /// <param name="isRetryable">Whether this error is transient and may succeed on retry.</param>
    public ZerobusException(string message, bool isRetryable)
        : base(FormatMessage(message, isRetryable))
    {
        IsRetryable = isRetryable;
        RawMessage = message;
    }

    /// <summary>
    /// Whether this error is transient and the operation may succeed if retried.
    /// When <see cref="StreamConfigurationOptions.Recovery"/> is enabled the SDK
    /// handles retryable errors automatically.
    /// </summary>
    public bool IsRetryable { get; }

    /// <summary>
    /// The raw error message from the native layer, without the retryable prefix.
    /// </summary>
    public string RawMessage { get; }

    private static string FormatMessage(string message, bool isRetryable)
        => isRetryable
            ? $"ZerobusException (retryable): {message}"
            : $"ZerobusException: {message}";
}
