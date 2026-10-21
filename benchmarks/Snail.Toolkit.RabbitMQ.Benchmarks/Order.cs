namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>A message of the size an order event usually has: a few scalars, a list of lines.</summary>
public sealed record Order(Guid Id, string Customer, DateTimeOffset PlacedAt, IReadOnlyList<OrderLine> Lines, decimal Total)
{
    public static Order Sample() => new(
        Guid.Parse("8c5f5d3e-1b2a-4c9d-9e7f-0a1b2c3d4e5f"),
        "customer-42",
        new DateTimeOffset(2026, 10, 12, 9, 30, 0, TimeSpan.Zero),
        [new OrderLine("sku-1", 2, 9.90m), new OrderLine("sku-2", 1, 24.50m), new OrderLine("sku-3", 4, 1.25m)],
        49.30m);
}

/// <summary>One line of an order.</summary>
public sealed record OrderLine(string Sku, int Quantity, decimal Price);
