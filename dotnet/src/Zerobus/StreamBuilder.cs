namespace Databricks.Zerobus;

/// <summary>
/// Fluent builder for creating Zerobus ingestion streams.
/// Provides a type-safe, self-documenting API for configuring and building
/// JSON, Protocol Buffer, and Arrow Flight streams.
/// </summary>
/// <remarks>
/// <para>
/// Obtain an instance via <see cref="ZerobusSdk.StreamBuilder"/>.
/// After configuring common options (table, auth, etc.), call one of the
/// terminal methods — <see cref="Json()"/>, <see cref="CompiledProto(byte[])"/>,
/// or <see cref="Arrow(byte[])"/> — to select the stream type and build.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// await using var stream = await sdk.StreamBuilder()
///     .Table("catalog.schema.table")
///     .OAuth("client-id", "client-secret")
///     .MaxInflightRequests(50_000)
///     .Recovery(false)
///     .Json()
///     .BuildAsync();
/// </code>
/// </example>
public sealed class StreamBuilder
{
    private readonly ZerobusSdk? _sdk = null!;
    private string? _tableName;
    private string? _clientId;
    private string? _clientSecret;
    private ulong? _maxInflightRequests;
    private bool? _recovery;
    private ulong? _recoveryTimeoutMs;
    private ulong? _recoveryBackoffMs;
    private uint? _recoveryRetries;
    private ulong? _serverLackOfAckTimeoutMs;
    private ulong? _flushTimeoutMs;
    private ulong? _streamPausedMaxWaitTimeMs;

    internal StreamBuilder(ZerobusSdk? sdk)
    {
        _sdk = sdk;
    }

    // ──── Common configuration ─────────────────────────────────────────────

    /// <summary>
    /// Sets the fully qualified Unity Catalog table name.
    /// Format: <c>catalog.schema.table</c>.
    /// </summary>
    /// <param name="tableName">The fully qualified table name.</param>
    /// <returns>This builder instance for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown if <paramref name="tableName"/> is null or empty.</exception>
    public StreamBuilder Table(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name must not be empty", nameof(tableName));
        _tableName = tableName;
        return this;
    }

    /// <summary>
    /// Sets OAuth 2.0 client credentials for authentication.
    /// </summary>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <returns>This builder instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if either parameter is null.</exception>
    public StreamBuilder OAuth(string clientId, string clientSecret)
    {
        _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
        _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
        return this;
    }

    /// <summary>
    /// Sets the maximum number of requests that can be in-flight (pending acknowledgment) at once.
    /// </summary>
    /// <param name="maxInflightRequests">Maximum in-flight requests. Must be greater than 0.</param>
    /// <returns>This builder instance for chaining.</returns>
    public StreamBuilder MaxInflightRequests(ulong maxInflightRequests)
    {
        _maxInflightRequests = maxInflightRequests;
        return this;
    }

    /// <summary>
    /// Enables or disables automatic stream recovery on retryable failures.
    /// </summary>
    /// <param name="recovery">Whether to enable automatic recovery.</param>
    /// <returns>This builder instance for chaining.</returns>
    public StreamBuilder Recovery(bool recovery)
    {
        _recovery = recovery;
        return this;
    }

    /// <summary>
    /// Sets the timeout for each recovery attempt in milliseconds.
    /// </summary>
    public StreamBuilder RecoveryTimeoutMs(ulong recoveryTimeoutMs)
    {
        _recoveryTimeoutMs = recoveryTimeoutMs;
        return this;
    }

    /// <summary>
    /// Sets the backoff delay between recovery attempts in milliseconds.
    /// </summary>
    public StreamBuilder RecoveryBackoffMs(ulong recoveryBackoffMs)
    {
        _recoveryBackoffMs = recoveryBackoffMs;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of recovery retry attempts.
    /// </summary>
    public StreamBuilder RecoveryRetries(uint recoveryRetries)
    {
        _recoveryRetries = recoveryRetries;
        return this;
    }

    /// <summary>
    /// Sets the server acknowledgment timeout in milliseconds.
    /// </summary>
    public StreamBuilder ServerLackOfAckTimeoutMs(ulong timeoutMs)
    {
        _serverLackOfAckTimeoutMs = timeoutMs;
        return this;
    }

    /// <summary>
    /// Sets the flush operation timeout in milliseconds.
    /// </summary>
    public StreamBuilder FlushTimeoutMs(ulong flushTimeoutMs)
    {
        _flushTimeoutMs = flushTimeoutMs;
        return this;
    }

    /// <summary>
    /// Sets the maximum time to wait during graceful stream close when the server
    /// sends a CloseStreamSignal.
    /// </summary>
    public StreamBuilder StreamPausedMaxWaitTimeMs(ulong? ms)
    {
        _streamPausedMaxWaitTimeMs = ms;
        return this;
    }

    // ──── Terminal methods ─────────────────────────────────────────────────

    /// <summary>
    /// Selects JSON ingestion and returns a builder for creating the stream.
    /// </summary>
    /// <returns>A <see cref="JsonStreamBuilder"/> to finalize and build.</returns>
    /// <exception cref="InvalidOperationException">Thrown if Table or OAuth are not configured.</exception>
    public JsonStreamBuilder Json()
    {
        ValidateRequired();
        return new JsonStreamBuilder(this);
    }

    /// <summary>
    /// Selects protobuf ingestion with a compiled descriptor and returns a builder.
    /// </summary>
    /// <param name="descriptorProtoBytes">The serialized protobuf descriptor bytes.</param>
    /// <returns>A <see cref="ProtoStreamBuilder"/> to finalize and build.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="descriptorProtoBytes"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if Table or OAuth are not configured.</exception>
    public ProtoStreamBuilder CompiledProto(byte[] descriptorProtoBytes)
    {
        if (descriptorProtoBytes == null)
            throw new ArgumentNullException(nameof(descriptorProtoBytes));
        ValidateRequired();
        return new ProtoStreamBuilder(this, descriptorProtoBytes);
    }

    /// <summary>
    /// Selects Arrow Flight ingestion with an IPC schema and returns a builder.
    /// </summary>
    /// <param name="schemaIpcBytes">The Arrow schema serialized as IPC format bytes.</param>
    /// <returns>An <see cref="ArrowStreamBuilder"/> to finalize and build.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="schemaIpcBytes"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if Table or OAuth are not configured.</exception>
    /// <remarks><b>Beta:</b> Arrow Flight ingestion is in beta and may change.</remarks>
    public ArrowStreamBuilder Arrow(byte[] schemaIpcBytes)
    {
        if (schemaIpcBytes == null)
            throw new ArgumentNullException(nameof(schemaIpcBytes));
        if (schemaIpcBytes.Length == 0)
            throw new ArgumentException("Schema IPC bytes must not be empty.", nameof(schemaIpcBytes));
        ValidateRequired();
        return new ArrowStreamBuilder(this, schemaIpcBytes);
    }

    /// <summary>
    /// Selects protobuf ingestion with a compiled descriptor and returns a builder.
    /// </summary>
    /// <param name="descriptorProtoBytes">The serialized protobuf descriptor bytes.</param>
    /// <returns>A <see cref="ProtoStreamBuilder"/> to finalize and build.</returns>
    public ProtoStreamBuilder Proto(byte[] descriptorProtoBytes) => CompiledProto(descriptorProtoBytes);

    // ──── Internal helpers ─────────────────────────────────────────────────

    private void ValidateRequired()
    {
        if (string.IsNullOrWhiteSpace(_tableName))
            throw new InvalidOperationException("Table name is required. Call .Table() before building.");
        if (string.IsNullOrWhiteSpace(_clientId) || string.IsNullOrWhiteSpace(_clientSecret))
            throw new InvalidOperationException("OAuth credentials are required. Call .OAuth() before building.");
    }

    internal StreamConfigurationOptions BuildOptions()
    {
        var opts = StreamConfigurationOptions.Default;

        if (_maxInflightRequests.HasValue)
            opts = opts with { MaxInflightRequests = _maxInflightRequests };
        if (_recovery.HasValue)
            opts = opts with { Recovery = _recovery.Value };
        if (_recoveryTimeoutMs.HasValue)
            opts = opts with { RecoveryTimeoutMs = _recoveryTimeoutMs };
        if (_recoveryBackoffMs.HasValue)
            opts = opts with { RecoveryBackoffMs = _recoveryBackoffMs };
        if (_recoveryRetries.HasValue)
            opts = opts with { RecoveryRetries = _recoveryRetries };
        if (_serverLackOfAckTimeoutMs.HasValue)
            opts = opts with { ServerLackOfAckTimeoutMs = _serverLackOfAckTimeoutMs };
        if (_flushTimeoutMs.HasValue)
            opts = opts with { FlushTimeoutMs = _flushTimeoutMs };
        if (_streamPausedMaxWaitTimeMs.HasValue)
            opts = opts with { StreamPausedMaxWaitTimeMs = _streamPausedMaxWaitTimeMs };

        return opts;
    }

    internal ArrowStreamConfigurationOptions BuildArrowOptions()
    {
        var opts = ArrowStreamConfigurationOptions.Default;

        if (_recovery.HasValue)
            opts = opts with { Recovery = _recovery.Value };
        if (_recoveryTimeoutMs.HasValue)
            opts = opts with { RecoveryTimeoutMs = _recoveryTimeoutMs };
        if (_recoveryBackoffMs.HasValue)
            opts = opts with { RecoveryBackoffMs = _recoveryBackoffMs };
        if (_recoveryRetries.HasValue)
            opts = opts with { RecoveryRetries = _recoveryRetries };
        if (_serverLackOfAckTimeoutMs.HasValue)
            opts = opts with { ServerLackOfAckTimeoutMs = _serverLackOfAckTimeoutMs };
        if (_flushTimeoutMs.HasValue)
            opts = opts with { FlushTimeoutMs = _flushTimeoutMs };

        return opts;
    }

    internal string TableName => _tableName!;
    internal string ClientId => _clientId!;
    internal string ClientSecret => _clientSecret!;
    internal ZerobusSdk Sdk => _sdk!;
}

/// <summary>
/// Builder for creating JSON ingestion streams.
/// Returned by <see cref="StreamBuilder.Json()"/>.
/// </summary>
public sealed class JsonStreamBuilder
{
    private readonly StreamBuilder _base;

    internal JsonStreamBuilder(StreamBuilder @base)
    {
        _base = @base;
    }

    /// <summary>
    /// Builds and opens the JSON ingestion stream.
    /// </summary>
    /// <returns>A ready-to-use <see cref="JsonZerobusStream"/>.</returns>
    public JsonZerobusStream Build()
    {
        return _base.Sdk.CreateJsonStream(
            _base.TableName,
            _base.ClientId,
            _base.ClientSecret,
            _base.BuildOptions());
    }

    /// <summary>
    /// Builds and opens the JSON ingestion stream asynchronously.
    /// </summary>
    /// <returns>A task resolving to a ready-to-use <see cref="JsonZerobusStream"/>.</returns>
    public Task<JsonZerobusStream> BuildAsync()
    {
        return _base.Sdk.CreateJsonStreamAsync(
            _base.TableName,
            _base.ClientId,
            _base.ClientSecret,
            _base.BuildOptions());
    }
}

/// <summary>
/// Builder for creating protobuf ingestion streams.
/// Returned by <see cref="StreamBuilder.CompiledProto(byte[])"/>.
/// </summary>
public sealed class ProtoStreamBuilder
{
    private readonly StreamBuilder _base;
    private readonly byte[] _descriptorProtoBytes;

    internal ProtoStreamBuilder(StreamBuilder @base, byte[] descriptorProtoBytes)
    {
        _base = @base;
        _descriptorProtoBytes = descriptorProtoBytes;
    }

    /// <summary>
    /// Builds and opens the protobuf ingestion stream.
    /// </summary>
    /// <returns>A ready-to-use <see cref="ProtoZerobusStream"/>.</returns>
    public ProtoZerobusStream Build()
    {
        return _base.Sdk.CreateProtoStream(
            _base.TableName,
            _descriptorProtoBytes,
            _base.ClientId,
            _base.ClientSecret,
            _base.BuildOptions());
    }

    /// <summary>
    /// Builds and opens the protobuf ingestion stream asynchronously.
    /// </summary>
    /// <returns>A task resolving to a ready-to-use <see cref="ProtoZerobusStream"/>.</returns>
    public Task<ProtoZerobusStream> BuildAsync()
    {
        return _base.Sdk.CreateProtoStreamAsync(
            _base.TableName,
            _descriptorProtoBytes,
            _base.ClientId,
            _base.ClientSecret,
            _base.BuildOptions());
    }
}

/// <summary>
/// Builder for creating Arrow Flight ingestion streams.
/// Returned by <see cref="StreamBuilder.Arrow(byte[])"/>.
/// </summary>
/// <remarks><b>Beta:</b> Arrow Flight ingestion is in beta and may change.</remarks>
public sealed class ArrowStreamBuilder
{
    private readonly StreamBuilder _base;
    private readonly byte[] _schemaIpcBytes;
    private uint? _maxInflightBatches;
    private ulong? _connectionTimeoutMs;
    private IPCCompressionType _ipcCompression = IPCCompressionType.None;
    private long _streamPausedMaxWaitTimeMs = -1;

    internal ArrowStreamBuilder(StreamBuilder @base, byte[] schemaIpcBytes)
    {
        _base = @base;
        _schemaIpcBytes = schemaIpcBytes;
    }

    /// <summary>
    /// Sets the maximum number of in-flight Arrow batches.
    /// </summary>
    public ArrowStreamBuilder MaxInflightBatches(uint maxInflightBatches)
    {
        _maxInflightBatches = maxInflightBatches;
        return this;
    }

    /// <summary>
    /// Sets the connection timeout in milliseconds.
    /// </summary>
    public ArrowStreamBuilder ConnectionTimeoutMs(ulong connectionTimeoutMs)
    {
        _connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /// <summary>
    /// Sets the IPC compression type for Arrow Flight messages.
    /// </summary>
    public ArrowStreamBuilder IpcCompression(IPCCompressionType compression)
    {
        _ipcCompression = compression;
        return this;
    }

    /// <summary>
    /// Sets the maximum time to wait during graceful stream close.
    /// A negative value means "wait the full server-specified duration."
    /// </summary>
    public ArrowStreamBuilder StreamPausedMaxWaitTimeMs(long ms)
    {
        _streamPausedMaxWaitTimeMs = ms;
        return this;
    }

    /// <summary>
    /// Builds and opens the Arrow Flight ingestion stream.
    /// </summary>
    /// <returns>A ready-to-use <see cref="ZerobusArrowStream"/>.</returns>
    public ZerobusArrowStream Build()
    {
        var opts = BuildArrowOptions();
        return _base.Sdk.CreateArrowStream(
            _base.TableName,
            _schemaIpcBytes,
            _base.ClientId,
            _base.ClientSecret,
            opts);
    }

    /// <summary>
    /// Builds and opens the Arrow Flight ingestion stream asynchronously.
    /// </summary>
    /// <returns>A task resolving to a ready-to-use <see cref="ZerobusArrowStream"/>.</returns>
    public Task<ZerobusArrowStream> BuildAsync()
    {
        var opts = BuildArrowOptions();
        return _base.Sdk.CreateArrowStreamAsync(
            _base.TableName,
            _schemaIpcBytes,
            _base.ClientId,
            _base.ClientSecret,
            opts);
    }

    private ArrowStreamConfigurationOptions BuildArrowOptions()
    {
        var opts = _base.BuildArrowOptions();

        if (_maxInflightBatches.HasValue)
            opts = opts with { MaxInflightBatches = _maxInflightBatches };
        if (_connectionTimeoutMs.HasValue)
            opts = opts with { ConnectionTimeoutMs = _connectionTimeoutMs };
        opts = opts with { IpcCompression = _ipcCompression };
        opts = opts with { StreamPausedMaxWaitTimeMs = _streamPausedMaxWaitTimeMs };

        return opts;
    }
}
