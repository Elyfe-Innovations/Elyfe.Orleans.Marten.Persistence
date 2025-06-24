# Elyfe.Orleans.Marten.Persistence

[![NuGet Version](https://img.shields.io/nuget/v/Elyfe.Orleans.Marten.Persistence.svg)](https://www.nuget.org/packages/Elyfe.Orleans.Marten.Persistence/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Elyfe.Orleans.Marten.Persistence.svg)](https://www.nuget.org/packages/Elyfe.Orleans.Marten.Persistence/)

An Orleans 9.x grain persistence provider that uses [Marten](https://martendb.io/) as a storage backend. This allows you to store your Orleans grain state in a PostgreSQL database with the rich features provided by Marten.

## Features

* **Marten Integration**: Leverages Marten for robust and efficient data persistence in PostgreSQL.
* **Full `IGrainStorage` Implementation**: Supports reading, writing, and clearing of grain state.
* **Easy Configuration**: Simple `ISiloBuilder` extension methods for quick setup.
* **Named Provider Support**: Configure multiple Marten storage providers with different names.
* **Automatic Schema Migrations**: Automatically creates and applies database schema changes on startup, ideal for development. For production, it is recommended to use [Marten's schema migration tools](https://martendb.io/schema/migrations.html).
* **Schema-per-Provider**: Each named provider instance operates within its own database schema, providing strong data isolation.
* **Optimistic Concurrency**: Generates ETags to support optimistic concurrency control.

## Installation

Install the package from NuGet:

```powershell
Install-Package Elyfe.Orleans.Marten.Persistence
```

Or via the .NET CLI:

```bash
dotnet add package Elyfe.Orleans.Marten.Persistence
```

## Usage

To use the Marten persistence provider, you first need to configure Marten services in your dependency injection container. Then, use the extension methods on `ISiloBuilder` to add the grain storage.

```csharp
var host = new HostBuilder()
    .UseOrleans(siloBuilder =>
    {
        // 1. Configure Marten
        siloBuilder.ConfigureServices(services =>
        {
            services.AddMarten(options =>
            {
                options.Connection("your-postgres-connection-string");
                
                // Optional: Configure other Marten features
                options.Projections.Add<MyProjection>(ProjectionLifecycle.Inline);
            });
        });

        // 2. Add the Marten grain storage provider
        
        // As the default provider
        siloBuilder.AddMartenGrainStorageAsDefault();

        // Or as a named provider
        siloBuilder.AddMartenGrainStorage("MyMartenStore");
    })
    .Build();
```

You can then specify which storage provider to use in your grain with the `[StorageProvider]` attribute:

```csharp
[StorageProvider(ProviderName = "MyMartenStore")]
public class MyGrain : Grain<MyGrainState>, IMyGrain
{
    // ... grain logic
}
```

If you registered it as the default, no attribute is needed.

## License

This project is licensed under the [MIT License](LICENSE).
