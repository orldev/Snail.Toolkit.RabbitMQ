using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Channels;

/// <summary>
/// The message properties of one publish, kept per thread and filled again for the next publish once the one that
/// read them has completed.
/// </summary>
/// <remarks>
/// <para>
/// The client reads the properties only until the task of its publish completes, so a disposed instance can serve the
/// next publish. Each publish starts from properties with nothing set, as a new <see cref="BasicProperties"/> does, and
/// the declaration's property actions see exactly that.
/// </para>
/// <para>
/// RabbitMQ.Client 7.2 reads the properties after awaits of its own: the confirmation lock and flow control come
/// before it adds its headers and writes the header frame. So a publish holds its instance until the client's task
/// completed, and no other publish may see it meanwhile: <see cref="Rent"/> empties the thread's slot as it takes the
/// instance, a publish starting on the same thread while one is still in flight finds the slot empty and gets new
/// properties, and <see cref="Dispose"/> puts an instance back only into an empty slot. A test overlaps publishes on
/// one thread, with the client reading late and confirming out of order, to keep it that way.
/// </para>
/// <para>
/// On a channel with publisher confirmations the client gives properties without headers a new dictionary and adds the
/// publish sequence number to it. Such a channel would send headers anyway, so properties the actions left without
/// headers get the dictionary kept here, emptied after each publish: the same entries in the same order reach the
/// broker, without a dictionary per message. Headers an action set are the action's and are left alone.
/// </para>
/// </remarks>
internal sealed class PublishProperties : IDisposable
{
    [ThreadStatic]
    private static PublishProperties? _cached;

    private readonly BasicProperties _properties = new();

    private readonly Dictionary<string, object?> _headers = [];

    private PublishProperties()
    {
    }

    /// <summary>
    /// Takes this thread's unused properties, or new ones when a publish on this thread still holds them.
    /// </summary>
    /// <returns>Properties with nothing set, to be disposed once the publish that reads them has completed.</returns>
    public static PublishProperties Rent()
    {
        var rented = _cached ?? new PublishProperties();
        _cached = null;

        return rented;
    }

    /// <summary>
    /// Sets the properties the declaration describes.
    /// </summary>
    /// <param name="declaration">The producer declaration whose property actions run.</param>
    /// <param name="tracksConfirmations">Whether the channel publishes with publisher confirmations.</param>
    /// <returns>The properties to publish with.</returns>
    public BasicProperties Fill(RabbitProducerDeclaration declaration, bool tracksConfirmations)
    {
        var actions = declaration.Properties;

        for (var i = 0; i < actions.Count; i++)
        {
            actions[i](_properties);
        }

        if (tracksConfirmations && _properties.Headers is null)
            _properties.Headers = _headers;

        return _properties;
    }

    /// <summary>
    /// Clears every property and keeps the instance for the next publish on this thread.
    /// </summary>
    public void Dispose()
    {
        _properties.ClearContentType();
        _properties.ClearContentEncoding();
        _properties.ClearHeaders();
        _properties.ClearDeliveryMode();
        _properties.ClearPriority();
        _properties.ClearCorrelationId();
        _properties.ClearReplyTo();
        _properties.ClearExpiration();
        _properties.ClearMessageId();
        _properties.ClearTimestamp();
        _properties.ClearType();
        _properties.ClearUserId();
        _properties.ClearAppId();
        _properties.ClearClusterId();

        _headers.Clear();

        _cached ??= this;
    }
}
