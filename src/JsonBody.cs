using System.Buffers;
using System.Text.Json;

namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Writes a message body as JSON into an array rented from the shared pool instead of a new array per message.
/// </summary>
/// <remarks>
/// The writer and its buffer are kept per thread and only used while one payload is written; the array they filled
/// is then handed to the body, which gives it back to the pool once the publish that reads it has completed.
/// </remarks>
internal static class JsonBody
{
    private const int InitialSize = 512;

    [ThreadStatic]
    private static Utf8JsonWriter? _writer;

    [ThreadStatic]
    private static RentingWriter? _buffer;

    /// <summary>
    /// Writes the payload as its runtime type, as <see cref="JsonSerializer.SerializeToUtf8Bytes(object, Type, JsonSerializerOptions?)"/> would.
    /// </summary>
    /// <param name="payload">The message to write.</param>
    /// <param name="options">The options whose resolver describes the payload's type.</param>
    /// <returns>The body, holding a rented array until it is disposed.</returns>
    public static RentedBody Write(object payload, JsonSerializerOptions options)
    {
        var buffer = _buffer ??= new RentingWriter();
        var writer = _writer;

        if (writer is null)
            _writer = writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = options.Encoder, Indented = options.WriteIndented, SkipValidation = true });
        else
            writer.Reset(buffer);

        try
        {
            JsonSerializer.Serialize(writer, payload, options.GetTypeInfo(payload.GetType()));
            writer.Flush();
        }
        catch
        {
            buffer.Detach().Dispose();
            throw;
        }

        return buffer.Detach();
    }

    /// <summary>
    /// An <see cref="IBufferWriter{T}"/> over a rented array that it gives away when the payload is written.
    /// </summary>
    private sealed class RentingWriter : IBufferWriter<byte>
    {
        private byte[]? _array;
        private int _written;

        public void Advance(int count) => _written += count;

        public Memory<byte> GetMemory(int sizeHint = 0) => Ensure(sizeHint).AsMemory(_written);

        public Span<byte> GetSpan(int sizeHint = 0) => Ensure(sizeHint).AsSpan(_written);

        /// <summary>
        /// Hands the array written so far to a body and starts the next payload without one.
        /// </summary>
        public RentedBody Detach()
        {
            var body = new RentedBody(_array ?? [], _written, rented: _array is not null);

            _array = null;
            _written = 0;

            return body;
        }

        private byte[] Ensure(int sizeHint)
        {
            var needed = _written + Math.Max(sizeHint, 1);

            if (_array is { } array && array.Length >= needed)
                return array;

            var next = ArrayPool<byte>.Shared.Rent(Math.Max(needed, Math.Max(InitialSize, (_array?.Length ?? 0) * 2)));

            if (_array is { } previous)
            {
                previous.AsSpan(0, _written).CopyTo(next);
                ArrayPool<byte>.Shared.Return(previous);
            }

            _array = next;

            return next;
        }
    }
}

/// <summary>
/// A message body: an array the serializer made, or one rented from the shared pool and given back on dispose.
/// </summary>
/// <param name="array">The array holding the body.</param>
/// <param name="length">How many bytes of the array are the body.</param>
/// <param name="rented">Whether the array came from the shared pool.</param>
internal readonly struct RentedBody(byte[] array, int length, bool rented) : IDisposable
{
    /// <summary>
    /// Wraps a body a custom serializer made; nothing is given back.
    /// </summary>
    /// <param name="array">The serialized message.</param>
    public RentedBody(byte[] array) : this(array, array.Length, rented: false)
    {
    }

    /// <summary>
    /// Gets the bytes of the body.
    /// </summary>
    public ReadOnlyMemory<byte> Memory => array.AsMemory(0, length);

    /// <summary>
    /// Gives a rented array back to the pool; the body must not be read afterwards.
    /// </summary>
    public void Dispose()
    {
        if (rented)
            ArrayPool<byte>.Shared.Return(array);
    }
}
