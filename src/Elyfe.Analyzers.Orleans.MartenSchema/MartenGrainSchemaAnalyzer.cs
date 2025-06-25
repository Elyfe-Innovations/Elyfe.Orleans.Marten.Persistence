using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Elyfe.Analyzers.Orleans.MartenSchema;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class MartenGrainSchemaAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "MARTEN001";
    private static readonly LocalizableString Title = "MartenGrainData schema missing for state type";

    private static readonly LocalizableString MessageFormat =
        "MartenGrainData<{0}> is not registered in Marten setup for parameter type '{0}' using Marten storage '{1}'";

    private static readonly LocalizableString Description =
        "For parameters P using a Marten storage provider, the type MartenGrainData<P> must be registered using For<MartenGrainData<P>>() in Marten setup.";

    private const string Category = "Usage";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId, Title, MessageFormat, Category, DiagnosticSeverity.Error, true,
        Description,
        customTags: "CompilationEnd");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(AnalyzeCompilationStart);
    }

    private void AnalyzeCompilationStart(CompilationStartAnalysisContext context)
    {
        var persistentStateAttributeSymbol = context.Compilation.GetTypeByMetadataName("PersistentStateAttribute") 
                                           ?? context.Compilation.GetTypeByMetadataName("Orleans.Runtime.PersistentStateAttribute");
        var iPersistentStateSymbol = context.Compilation.GetTypeByMetadataName("IPersistentState`1")
                                   ?? context.Compilation.GetTypeByMetadataName("Orleans.Runtime.IPersistentState`1");
        var martenGrainDataType = context.Compilation.GetTypeByMetadataName("MartenGrainData`1")
                                  ?? context.Compilation.GetTypeByMetadataName("Elyfe.Orleans.Marten.Persistence.GrainPersistence.MartenGrainData`1");

        if (persistentStateAttributeSymbol is null || iPersistentStateSymbol is null || martenGrainDataType is null)
        {
            return;
        }

        var martenStorageNames = new ConcurrentBag<string>();
        var registeredMartenGrainDataTypes = new ConcurrentBag<ITypeSymbol>();
        var persistentStateParameters = new ConcurrentBag<(IParameterSymbol Parameter, string StorageName)>();

        context.RegisterSyntaxNodeAction(
            c => AnalyzeInvocationExpression(c, martenGrainDataType, martenStorageNames, registeredMartenGrainDataTypes),
            SyntaxKind.InvocationExpression);

        context.RegisterSyntaxNodeAction(
            c => AnalyzeParameter(c, persistentStateAttributeSymbol, iPersistentStateSymbol, persistentStateParameters),
            SyntaxKind.Parameter);

        context.RegisterCompilationEndAction(
            c => ReportDiagnostics(c, martenGrainDataType, martenStorageNames, registeredMartenGrainDataTypes, persistentStateParameters));
    }

    private void AnalyzeInvocationExpression(
        SyntaxNodeAnalysisContext context, 
        INamedTypeSymbol martenGrainDataType, 
        ConcurrentBag<string> martenStorageNames, 
        ConcurrentBag<ITypeSymbol> registeredMartenGrainDataTypes)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        if (context.SemanticModel.GetSymbolInfo(invocation).Symbol is not IMethodSymbol methodSymbol)
        {
            return;
        }

        // Check for AddMartenGrainStorage
        if (methodSymbol.Name == "AddMartenGrainStorage" &&
            methodSymbol.IsExtensionMethod &&
            methodSymbol.Parameters.Length >= 1 &&
            invocation.ArgumentList.Arguments.Count == 1 &&
            invocation.ArgumentList.Arguments[0].Expression is LiteralExpressionSyntax literal &&
            literal.IsKind(SyntaxKind.StringLiteralExpression))
        {
            var storageName = literal.Token.ValueText;
            if (!martenStorageNames.Contains(storageName))
            {
                martenStorageNames.Add(storageName);
            }
        }

        if (methodSymbol.Name == "AddMartenGrainStorageAsDefault" &&
            methodSymbol.IsExtensionMethod &&
            methodSymbol.Parameters.Length == 0 &&
            invocation.ArgumentList.Arguments.Count == 1 &&
            invocation.ArgumentList.Arguments[0].Expression is LiteralExpressionSyntax defaultLiteral &&
            defaultLiteral.IsKind(SyntaxKind.StringLiteralExpression)
           )
        {
            if(!martenStorageNames.Contains("Default"))
            {
                martenStorageNames.Add("Default");
            }
            
        }
        // Check for Schema.For<MartenGrainData<T>>()
        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
            memberAccess.Name.Identifier.Text == "For" &&
            methodSymbol.IsGenericMethod &&
            methodSymbol.TypeArguments.Length == 1)
        {
            var typeArgument = methodSymbol.TypeArguments[0];
            if (typeArgument is INamedTypeSymbol namedTypeArgument &&
                namedTypeArgument.IsGenericType &&
                SymbolEqualityComparer.Default.Equals(namedTypeArgument.OriginalDefinition, martenGrainDataType))
            {
                var stateType = namedTypeArgument.TypeArguments[0];
                if (!registeredMartenGrainDataTypes.Contains(stateType, SymbolEqualityComparer.Default))
                {
                    registeredMartenGrainDataTypes.Add(stateType);
                }
            }
        }
    }

    private void AnalyzeParameter(
        SyntaxNodeAnalysisContext context, 
        INamedTypeSymbol persistentStateAttributeSymbol, 
        INamedTypeSymbol iPersistentStateSymbol, 
        ConcurrentBag<(IParameterSymbol Parameter, string StorageName)> persistentStateParameters)
    {
        var parameter = (ParameterSyntax)context.Node;
        if (context.SemanticModel.GetDeclaredSymbol(parameter) is not IParameterSymbol parameterSymbol) return;

        var persistentStateAttr = parameterSymbol.GetAttributes().FirstOrDefault(attr =>
            SymbolEqualityComparer.Default.Equals(attr.AttributeClass?.OriginalDefinition, persistentStateAttributeSymbol));

        if (persistentStateAttr?.ConstructorArguments.Length == 1 &&
            persistentStateAttr.ConstructorArguments[0].Value is string storageName)
        {
            if (parameterSymbol.Type is INamedTypeSymbol { IsGenericType: true } persistentStateType &&
                SymbolEqualityComparer.Default.Equals(persistentStateType.OriginalDefinition, iPersistentStateSymbol))
            {
                persistentStateParameters.Add((parameterSymbol, storageName));
            }
        }
    }

    private void ReportDiagnostics(
        CompilationAnalysisContext context, 
        INamedTypeSymbol martenGrainDataType, 
        ConcurrentBag<string> martenStorageNames, 
        ConcurrentBag<ITypeSymbol> registeredMartenGrainDataTypes, 
        ConcurrentBag<(IParameterSymbol Parameter, string StorageName)> persistentStateParameters)
    {
        var martenProviders = new HashSet<string>(martenStorageNames);
        var registeredTypes = new HashSet<ITypeSymbol>(registeredMartenGrainDataTypes, SymbolEqualityComparer.Default);

        foreach (var (parameter, storageName) in persistentStateParameters)
        {
            if (martenProviders.Contains(storageName))
            {
                var persistentStateType = (INamedTypeSymbol)parameter.Type;
                var stateTypeSymbol = persistentStateType.TypeArguments[0];

                if (!registeredTypes.Contains(stateTypeSymbol))

                {
                    var diagnostic = Diagnostic.Create(Rule, parameter.Locations.FirstOrDefault(), stateTypeSymbol.Name, storageName);
                    context.ReportDiagnostic(diagnostic);
                }
            }
        }
    }
}