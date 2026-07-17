namespace Databricks.Zerobus;

/// <summary>
/// Callback delegate for successful acknowledgment of records up to the given offset.
/// </summary>
/// <param name="offsetId">
/// The offset ID that has been durably acknowledged by the server.
/// All records with offset IDs less than or equal to this value have been durably stored.
/// </param>
public delegate void AckOnAckDelegate(long offsetId);

/// <summary>
/// Callback delegate for errors affecting a specific offset.
/// </summary>
/// <param name="offsetId">The offset ID that encountered an error.</param>
/// <param name="errorMessage">A description of the error that occurred.</param>
public delegate void AckOnErrorDelegate(long offsetId, string errorMessage);
