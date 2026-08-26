using System.Security.Cryptography;
using System.Text;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

/// <summary>
/// MARTEN-01: reversible, collision-free encoding of grain ids for storage and
/// cache keys. The previous scheme collapsed '/' to '_', which merged distinct
/// grain ids ("a/b" vs "a_b") onto one document — cross-tenant state confusion.
/// New keys use Base64Url of the full grain id string; legacy collapsed keys
/// remain readable via <see cref="LegacyEncode"/> fallbacks during migration.
/// </summary>
public static class GrainKeyEncoding
{
    public static string Encode(GrainId grainId) => Encode(grainId.ToString());

    public static string Encode(string grainId)
    {
        var bytes = Encoding.UTF8.GetBytes(grainId);
        return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    public static bool TryDecodeToGrainId(string encoded, out GrainId grainId)
    {
        grainId = default!;
        if (!TryDecodeToString(encoded, out var value))
            return false;

        if (!GrainId.TryParse(value, out var parsed))
            return false;

        grainId = parsed;
        return true;
    }

    public static bool TryDecodeToString(string encoded, out string decoded)
    {
        decoded = string.Empty;
        try
        {
            var padded = encoded.Replace('-', '+').Replace('_', '/');
            padded += new string('=', (4 - padded.Length % 4) % 4);
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(padded));
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>The pre-MARTEN-01 lossy encoding, kept only for read/migration fallbacks.</summary>
    public static string LegacyEncode(string grainId) => grainId.Replace('/', '_');
}
