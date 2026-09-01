using System.Collections.Concurrent;

namespace Snail.Toolkit.RabbitMQ.Extensions;

/// <summary>
/// Provides extension methods for <see cref="ConcurrentDictionary{TKey,TValue}"/> to support asynchronous operations.
/// </summary>
public static class ConcurrentDictionaryExtensions
{
    /// <summary>
    /// Adds a key/value pair to the dictionary asynchronously if the key does not already exist.
    /// Returns the new value from the async factory or the existing value if the key exists.
    /// </summary>
    /// <typeparam name="TKey">The type of the keys in the dictionary.</typeparam>
    /// <typeparam name="TResult">The type of the values in the dictionary.</typeparam>
    /// <param name="dict">The concurrent dictionary to operate on.</param>
    /// <param name="key">The key of the element to add or get.</param>
    /// <param name="asyncValueFactory">The asynchronous function used to generate a value for the key.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains the value for the key.
    /// This will be either the existing value for the key if it already exists, or the new value if it was added.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="key"/> or <paramref name="asyncValueFactory"/> is null.
    /// </exception>
    /// <remarks>
    /// This method provides thread-safe access to the dictionary while allowing asynchronous value generation.
    /// The async factory is only called if the key doesn't exist in the dictionary.
    /// </remarks>
    public static async Task<TResult> GetOrAddAsync<TKey, TResult>(
        this ConcurrentDictionary<TKey, TResult> dict,
        TKey key, 
        Func<TKey, Task<TResult>> asyncValueFactory) 
        where TKey : notnull  
    {
        if (dict.TryGetValue(key, out var resultingValue))
        {
            return resultingValue;
        }

        var newValue = await asyncValueFactory(key);
        return dict.GetOrAdd(key, newValue);
    }
}