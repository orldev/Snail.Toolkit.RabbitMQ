using System.Threading.Tasks.Sources;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>
/// Something that completes after the call that started it returned — a broker's confirm, a handler's work: the task
/// reports pending until <see cref="Arrive"/>, which runs whatever waits for it inline, so the awaiting code builds
/// what it builds for the real thing and no thread hop is timed.
/// </summary>
internal sealed class PendingConfirm : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = false };
    private bool _pending;

    /// <summary>A confirm that is still to come.</summary>
    public ValueTask Next()
    {
        _core.Reset();
        _pending = true;

        return new ValueTask(this, _core.Version);
    }

    /// <summary>The broker's confirm of the last publish, if one is still to come.</summary>
    public void Arrive()
    {
        if (!_pending)
            return;

        _pending = false;
        _core.SetResult(true);
    }

    public void GetResult(short token) => _core.GetResult(token);

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, ValueTaskSourceOnCompletedFlags.None);
}
