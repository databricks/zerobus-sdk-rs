using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Databricks.Zerobus;

/// <summary>
/// Fluent builder for creating Zerobus ingestion streams.
/// Mirrors the Rust SDK's stream builder API.
/// Obtain an instance via <see cref="ZerobusSdk.StreamBuilder"/>.
/// </summary>
/// <remarks>
/// Usage:
/// <code>
/// var stream = await sdk.StreamBuilder()
///     .Table("my_catalog.my_schema.my_table")
///     .OAuth("client-id", "client-secret")
///     .MaxInflightRecords(50000)
///     .Recovery(false)
///     .Json()
///     .BuildAsync();
/// </code>
/// </remarks>
public sealed class StreamBuilder
{
    private readonly ZerobusSdk? _sdk;
    private string? _tableName;
    private string? _clientId;
    private string? _clientSecret;
    private int? _maxInflightRecords;
    private bool? _recovery;
    private int? _recoveryTimeoutMs;
    private int? _recoveryBackoffMs;
    private int? _recoveryRetries;
    private int? _serverLackOfAckTimeoutMs;
    private int? _flushTimeoutMs;
    private AckOnAckDelegate? _onAck;
    private AckOnErrorDelegate? _onError;
    private object? _ackUserData;

    internal StreamBuilder(ZerobusSdk? sdk)
    {
        _sdk = sdk!;
    }

    /// <summary>
    /// Sets the fully qualified Unity Catalog table name (catalog.schema.table).
    /// </summary>
    public StreamBuilder Table(string tableName)
    {
        _tableName = RequireNonBlank(tableName, nameof(tableName));
        return this;
    }

    /// <summary>
    /// Sets the OAuth client credentials for authentication.
    /// </summary>
    public StreamBuilder OAuth(string clientId, string clientSecret)
    {
        _clientId = RequireNonBlank(clientId, nameof(clientId));
        _clientSecret = RequireNonBlank(clientSecret, nameof(clientSecret));
        return this;
    }

    /// <summary>
    /// Sets the maximum number of in-flight records. Must be positive.
    /// </summary>
    public StreamBuilder MaxInflightRecords(int maxInflightRecords)
    {
        _maxInflightRecords = RequirePositive(maxInflightRecords, nameof(maxInflightRecords));
        return this;
    }

    /// <summary>
    /// Enables or disables automatic stream recovery.
    /// </summary>
    public StreamBuilder Recovery(bool recovery)
    {
        _recovery = recovery;
        return this;
    }

    /// <summary>
    /// Sets the recovery timeout in milliseconds. Must be non-negative.
    /// </summary>
    public StreamBuilder RecoveryTimeoutMs(int recoveryTimeoutMs)
    {
        _recoveryTimeoutMs = RequireNonNegative(recoveryTimeoutMs, nameof(recoveryTimeoutMs));
        return this;
    }

    /// <summary>
    /// Sets the recovery backoff in milliseconds. Must be non-negative.
    /// </summary>
    public StreamBuilder RecoveryBackoffMs(int recoveryBackoffMs)
    {
        _recoveryBackoffMs = RequireNonNegative(recoveryBackoffMs, nameof(recoveryBackoffMs));
        return this;
    }

    /// <summary>
    /// Sets the maximum number of recovery retries. Must be non-negative.
    /// </summary>
    public StreamBuilder RecoveryRetries(int recoveryRetries)
    {
        _recoveryRetries = RequireNonNegative(recoveryRetries, nameof(recoveryRetries));
        return this;
    }

    /// <summary>
    /// Sets the server lack-of-ack timeout in milliseconds. Must be positive.
    /// </summary>
    public StreamBuilder ServerLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs)
    {
        _serverLackOfAckTimeoutMs = RequirePositive(serverLackOfAckTimeoutMs, nameof(serverLackOfAckTimeoutMs));
        return this;
    }

    /// <summary>
    /// Sets the flush timeout in milliseconds. Must be positive.
    /// </summary>
    public StreamBuilder FlushTimeoutMs(int flushTimeoutMs)
    {
        _flushTimeoutMs = RequirePositive(flushTimeoutMs, nameof(flushTimeoutMs));
        return this;
    }

    /// <summary>
    /// Sets the acknowledgment callback for this stream.
    /// </summary>
    public StreamBuilder AckCallback(AckOnAckDelegate? onAck, AckOnErrorDelegate? onError, object? userData = null)
    {
        _onAck = onAck;
        _onError = onError;
        _ackUserData = userData;
        return this;
    }

    /// <summary>
    /// Returns a JsonStreamBuilder to configure and create a JSON ingestion stream.
    /// </summary>
    public JsonStreamBuilder Json()
    {
        ValidateRequired();
        return new JsonStreamBuilder(this);
    }

    /// <summary>
    /// Returns a ProtoStreamBuilder for the given compiled protobuf descriptor.
    /// </summary>
    /// <param name="descriptor">
    /// A compiled FileDescriptorProto for the table schema. Obtain this via
    /// <see cref="ProtoSchema.FromUnityCatalogJson"/> and
    /// <see cref="ProtoSchema.GetDescriptorBytes"/>.
    /// </param>
    public ProtoStreamBuilder CompiledProto(FileDescriptorProto descriptor)
    {
        if (descriptor == null) throw new ArgumentNullException(nameof(descriptor));
        ValidateRequired();
        return new ProtoStreamBuilder(this, descriptor);
    }

    /// <summary>
    /// Returns a ProtoStreamBuilder for the given descriptor proto bytes.
    /// </summary>
    /// <param name="descriptorProtoBytes">Raw descriptor proto bytes.</param>
    public ProtoStreamBuilder CompiledProto(byte[] descriptorProtoBytes)
    {
        if (descriptorProtoBytes == null) throw new ArgumentNullException(nameof(descriptorProtoBytes));
        ValidateRequired();
        return new ProtoStreamBuilder(this, descriptorProtoBytes);
    }

    /// <summary>
    /// Returns an ArrowStreamBuilder for the given Arrow schema IPC bytes. (Beta)
    /// </summary>
    /// <param name="schemaIpcBytes">
    /// The Arrow schema serialized as IPC format bytes. Generate these from an
    /// Apache Arrow Schema using <c>ArrowStreamWriter</c> or equivalent.
    /// </param>
    public ArrowStreamBuilder Arrow(byte[] schemaIpcBytes)
    {
        if (schemaIpcBytes == null) throw new ArgumentNullException(nameof(schemaIpcBytes));
        if (schemaIpcBytes.Length == 0) throw new ArgumentException("Schema IPC bytes must not be empty.", nameof(schemaIpcBytes));
        ValidateRequired();
        return new ArrowStreamBuilder(this, schemaIpcBytes);
    }

    internal StreamConfigurationOptions ToStreamOptions()
    {
        var b = StreamConfigurationOptions.NewBuilder();

        if (_maxInflightRecords.HasValue) b.SetMaxInflightRecords(_maxInflightRecords.Value);
        if (_recovery.HasValue) b.SetRecovery(_recovery.Value);
        if (_recoveryTimeoutMs.HasValue) b.SetRecoveryTimeoutMs(_recoveryTimeoutMs.Value);
        if (_recoveryBackoffMs.HasValue) b.SetRecoveryBackoffMs(_recoveryBackoffMs.Value);
        if (_recoveryRetries.HasValue) b.SetRecoveryRetries(_recoveryRetries.Value);
        if (_serverLackOfAckTimeoutMs.HasValue) b.SetServerLackOfAckTimeoutMs(_serverLackOfAckTimeoutMs.Value);
        if (_flushTimeoutMs.HasValue) b.SetFlushTimeoutMs(_flushTimeoutMs.Value);

        if (_onAck != null || _onError != null)
            b.SetAckCallback(_onAck, _onError, _ackUserData);

        return b.Build();
    }

    private void ValidateRequired()
    {
        if (string.IsNullOrWhiteSpace(_tableName))
            throw new InvalidOperationException("Table name is required. Call .Table() before building.");
        if (string.IsNullOrWhiteSpace(_clientId) || string.IsNullOrWhiteSpace(_clientSecret))
            throw new InvalidOperationException("OAuth credentials are required. Call .OAuth() before building.");
    }

    // -- Inner builders --

    /// <summary>
    /// Builder for creating JSON ingestion streams.
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
        /// <returns>A ready-to-use JSON stream.</returns>
        public async Task<ZerobusJsonStream> BuildAsync()
        {
            var options = _base.ToStreamOptions();
            return await _base._sdk!.CreateJsonStreamAsync(
                _base._tableName!,
                _base._clientId!,
                _base._clientSecret!,
                options).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Builder for creating Protocol Buffer ingestion streams.
    /// </summary>
    public sealed class ProtoStreamBuilder
    {
        private readonly StreamBuilder _base;
        private readonly byte[] _descriptorProtoBytes;

        internal ProtoStreamBuilder(StreamBuilder @base, FileDescriptorProto descriptor)
        {
            _base = @base;
            _descriptorProtoBytes = descriptor.ToByteArray();
        }

        internal ProtoStreamBuilder(StreamBuilder @base, byte[] descriptorProtoBytes)
        {
            _base = @base;
            _descriptorProtoBytes = descriptorProtoBytes;
        }

        /// <summary>
        /// Builds and opens the protobuf ingestion stream for the specified message type.
        /// The type parameter must be a compiled protobuf message matching the descriptor.
        /// </summary>
        /// <typeparam name="T">The protobuf message type.</typeparam>
        /// <returns>A ready-to-use protobuf stream.</returns>
        public async Task<ZerobusProtoStream<T>> BuildAsync<T>() where T : Google.Protobuf.IMessage<T>
        {
            var options = _base.ToStreamOptions();
            return await _base._sdk!.CreateProtoStreamAsync<T>(
                _base._tableName!,
                _descriptorProtoBytes,
                _base._clientId!,
                _base._clientSecret!,
                options).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Builder for creating Arrow Flight ingestion streams. (Beta)
    /// </summary>
    public sealed class ArrowStreamBuilder
    {
        private readonly StreamBuilder _base;
        private readonly byte[] _schemaIpcBytes;
        private int? _maxInflightBatches;
        private int? _connectionTimeoutMs;
        private IPCCompressionType _ipcCompression = IPCCompressionType.None;
        private long? _streamPausedMaxWaitTimeMs;

        internal ArrowStreamBuilder(StreamBuilder @base, byte[] schemaIpcBytes)
        {
            _base = @base;
            _schemaIpcBytes = schemaIpcBytes;
        }

        /// <summary>
        /// Sets the maximum number of in-flight batches. Must be positive.
        /// </summary>
        public ArrowStreamBuilder MaxInflightBatches(int maxInflightBatches)
        {
            _maxInflightBatches = RequirePositive(maxInflightBatches, nameof(maxInflightBatches));
            return this;
        }

        /// <summary>
        /// Sets the connection timeout in milliseconds. Must be positive.
        /// </summary>
        public ArrowStreamBuilder ConnectionTimeoutMs(int connectionTimeoutMs)
        {
            _connectionTimeoutMs = RequirePositive(connectionTimeoutMs, nameof(connectionTimeoutMs));
            return this;
        }

        /// <summary>
        /// Sets the IPC compression type. Default: None.
        /// </summary>
        public ArrowStreamBuilder IpcCompression(IPCCompressionType compression)
        {
            _ipcCompression = compression;
            return this;
        }

        /// <summary>
        /// Sets the stream paused max wait time in milliseconds.
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
        /// <returns>A ready-to-use Arrow Flight stream.</returns>
        public async Task<ZerobusArrowStream> BuildAsync()
        {
            var opts = BuildOptions();
            return await _base._sdk!.CreateArrowStreamAsync(
                _base._tableName!,
                _schemaIpcBytes,
                _base._clientId!,
                _base._clientSecret!,
                opts).ConfigureAwait(false);
        }

        private ArrowStreamConfigurationOptions BuildOptions()
        {
            var b = ArrowStreamConfigurationOptions.NewBuilder();

            // Merge shared config from base StreamBuilder
            if (_base._recovery.HasValue) b.SetRecovery(_base._recovery.Value);
            if (_base._recoveryTimeoutMs.HasValue) b.SetRecoveryTimeoutMs(_base._recoveryTimeoutMs.Value);
            if (_base._recoveryBackoffMs.HasValue) b.SetRecoveryBackoffMs(_base._recoveryBackoffMs.Value);
            if (_base._recoveryRetries.HasValue) b.SetRecoveryRetries(_base._recoveryRetries.Value);
            if (_base._serverLackOfAckTimeoutMs.HasValue) b.SetServerLackOfAckTimeoutMs(_base._serverLackOfAckTimeoutMs.Value);
            if (_base._flushTimeoutMs.HasValue) b.SetFlushTimeoutMs(_base._flushTimeoutMs.Value);

            // Arrow-specific
            if (_maxInflightBatches.HasValue) b.SetMaxInflightBatches(_maxInflightBatches.Value);
            if (_connectionTimeoutMs.HasValue) b.SetConnectionTimeoutMs(_connectionTimeoutMs.Value);
            b.SetIpcCompression(_ipcCompression);
            if (_streamPausedMaxWaitTimeMs.HasValue) b.SetStreamPausedMaxWaitTimeMs(_streamPausedMaxWaitTimeMs.Value);

            return b.Build();
        }

    }

    // -- Validation helpers --

    private static int RequirePositive(int value, string name)
    {
        if (value <= 0)
            throw new ArgumentException($"{name} must be positive, got {value}", name);
        return value;
    }

    private static int RequireNonNegative(int value, string name)
    {
        if (value < 0)
            throw new ArgumentException($"{name} must be non-negative, got {value}", name);
        return value;
    }

    private static string RequireNonBlank(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException($"{name} must not be empty or whitespace.", name);
        return value;
    }
}
