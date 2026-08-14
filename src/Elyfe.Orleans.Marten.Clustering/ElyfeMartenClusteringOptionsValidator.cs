using Microsoft.Extensions.Options;

namespace Elyfe.Orleans.Marten.Clustering;

internal sealed class ElyfeMartenClusteringOptionsValidator : IValidateOptions<ElyfeMartenClusteringOptions>
{
    public ValidateOptionsResult Validate(string? name, ElyfeMartenClusteringOptions options)
    {
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.DatabaseSchemaName))
        {
            failures.Add($"{nameof(options.DatabaseSchemaName)} is required.");
        }

        if (string.IsNullOrWhiteSpace(options.MembershipDocumentAlias))
        {
            failures.Add($"{nameof(options.MembershipDocumentAlias)} is required.");
        }

        if (string.IsNullOrWhiteSpace(options.ClusterVersionDocumentAlias))
        {
            failures.Add($"{nameof(options.ClusterVersionDocumentAlias)} is required.");
        }

        if (string.Equals(
                options.MembershipDocumentAlias,
                options.ClusterVersionDocumentAlias,
                StringComparison.OrdinalIgnoreCase))
        {
            failures.Add("Membership and cluster-version document aliases must differ.");
        }

        if (options.MaxStaleness <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(options.MaxStaleness)} must be greater than zero.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
