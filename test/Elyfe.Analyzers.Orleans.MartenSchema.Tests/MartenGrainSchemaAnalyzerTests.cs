using Xunit;
using Verify =
    Microsoft.CodeAnalysis.CSharp.Testing.CSharpAnalyzerVerifier<
        Elyfe.Analyzers.Orleans.MartenSchema.MartenGrainSchemaAnalyzer, Microsoft.CodeAnalysis.Testing.DefaultVerifier>;

namespace Elyfe.Analyzers.Orleans.MartenSchema.Tests;

public class MartenGrainSchemaAnalyzerTests
{
    private const string TestCodeTemplate = """
    using System;

    // Dummy attributes and interfaces to simulate Orleans environment
    [AttributeUsage(AttributeTargets.Parameter)]
    public class PersistentStateAttribute : Attribute {
        public PersistentStateAttribute(string stateName,string storageName = "Default") { StorageName = storageName; StateName = stateName; }
        public string StorageName { get; }
        public string StateName { get; }
    }

    [AttributeUsage(AttributeTargets.Parameter)]
    public class OtherAttribute : Attribute { }

    public interface IPersistentState<T> { T State { get; set; } }
    public interface IServiceCollection {}
    public interface ISiloBuilder {
        public ISiloBuilder ConfigureServices(Action<IServiceCollection> configureServices);
    }

    // Dummy types for test
    public class MyState {}
    public class MartenGrainData<T> {}
    public class ServiceCollection : IServiceCollection {}
    public class SchemaOptions { public SchemaOptionsSchema Schema { get; } = new SchemaOptionsSchema(); }
    public class SchemaOptionsSchema { public void For<T>() {} }

    // Dummy extension method for registration
    public static class MartenSiloBuilderExtensions {
        public static IServiceCollection AddMartenGrainStorage(this IServiceCollection services, string storageName) => services;
        public static ISiloBuilder AddMartenGrainStorageAsDefault(this ISiloBuilder builder) => builder;
    }

    public static class Startup {
        public static void ConfigureServices() {
            var services = new ServiceCollection();
            {{StorageRegistrations}}

            var options = new SchemaOptions();
            {{SchemaRegistrations}}
        }
    }

    public class Grain {
        public Grain({{GrainParameters}}) {}
    }
    """;

    [Fact]
    public async Task ReportsDiagnostic_WhenMartenGrainDataForStateTypeNotRegistered()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"Marten\");")
            .Replace("{{SchemaRegistrations}}", "") // No schema registration
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\", \"Marten\")] IPersistentState<MyState> state");

        var expectedError = Verify.Diagnostic(MartenGrainSchemaAnalyzer.DiagnosticId)
            .WithSpan(44, 81, 44, 86)
            .WithArguments("MyState", "Marten");

        await Verify.VerifyAnalyzerAsync(testCode, expectedError);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_WhenMartenGrainDataForStateTypeIsRegistered()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"Marten\");")
            .Replace("{{SchemaRegistrations}}", "options.Schema.For<MartenGrainData<MyState>>();")
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\", \"Marten\")] IPersistentState<MyState> state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_WhenStorageNameIsNotMartenProvider()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "") // No Marten storage registered
            .Replace("{{SchemaRegistrations}}", "")
            .Replace("{{GrainParameters}}", "[PersistentState(\"other\")] IPersistentState<MyState> state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }

    [Fact]
    public async Task ReportsDiagnostic_ForCustomNamedMartenStorageProvider()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"MyMartenStorage\");")
            .Replace("{{SchemaRegistrations}}", "") // No schema registration
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\",\"MyMartenStorage\")] IPersistentState<MyState> state");

        var expectedError = Verify.Diagnostic(MartenGrainSchemaAnalyzer.DiagnosticId)
            .WithSpan(44, 89, 44, 94)
            .WithArguments("MyState", "MyMartenStorage");

        await Verify.VerifyAnalyzerAsync(testCode, expectedError);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_ForCustomNamedMartenStorageProvider_WhenRegistered()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"MyMartenStorage\");")
            .Replace("{{SchemaRegistrations}}", "options.Schema.For<MartenGrainData<MyState>>();")
            .Replace("{{GrainParameters}}", "[PersistentState(\"MyMartenStorage\")] IPersistentState<MyState> state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_WhenAttributeIsNotPersistentState()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "")
            .Replace("{{SchemaRegistrations}}", "")
            .Replace("{{GrainParameters}}", "[Other] IPersistentState<MyState> state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_WhenParameterIsNotIPersistentState()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"Marten\");")
            .Replace("{{SchemaRegistrations}}", "")
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\", \"Marten\")] MyState state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }

    [Fact]
    public async Task ReportsDiagnostic_WhenDefaultMartenStorageProvider_And_NoSchemaRegistration()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"Default\");")
            .Replace("{{SchemaRegistrations}}", "") // No schema registration
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\")] IPersistentState<MyState> state");

        var expectedError = Verify.Diagnostic(MartenGrainSchemaAnalyzer.DiagnosticId)
            .WithSpan(44, 71, 44, 76)
            .WithArguments("MyState", "Default");

        await Verify.VerifyAnalyzerAsync(testCode, expectedError);
    }

    [Fact]
    public async Task DoesNotReportDiagnostic_WhenDefaultMartenStorageProvider_And_SchemaIsRegistered()
    {
        var testCode = TestCodeTemplate
            .Replace("{{StorageRegistrations}}", "services.AddMartenGrainStorage(\"Marten\");")
            .Replace("{{SchemaRegistrations}}", "options.Schema.For<MartenGrainData<MyState>>();")
            .Replace("{{GrainParameters}}", "[PersistentState(\"state\")] IPersistentState<MyState> state");

        await Verify.VerifyAnalyzerAsync(testCode);
    }
}
