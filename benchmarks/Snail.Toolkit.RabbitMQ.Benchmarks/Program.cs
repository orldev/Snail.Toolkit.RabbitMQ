using BenchmarkDotNet.Running;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>Runs the benchmarks named on the command line, or offers all of them.</summary>
public static class Program
{
    public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
