using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Orleans.Runtime;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests;

/// <summary>
/// MARTEN-01: grain-key encoding must be reversible and collision-free so
/// distinct grain ids can never share one Marten document or Redis entry.
/// </summary>
public class GrainKeyEncodingTests
{
    [Theory]
    [InlineData("tenant-a/prod/customer-1")]
    [InlineData("tenant_a/prod/customer_1")]
    [InlineData("acme/prod")]
    [InlineData("acme_prod")]
    [InlineData("plain")]
    public void Encode_Decode_RoundTrips(string grainId)
    {
        var encoded = GrainKeyEncoding.Encode(grainId);

        Assert.True(GrainKeyEncoding.TryDecodeToString(encoded, out var decoded));
        Assert.Equal(grainId, decoded);
    }

    [Fact]
    public void Slash_And_Underscore_Ids_Encode_Differently()
    {
        var a = GrainKeyEncoding.Encode("acme/prod");
        var b = GrainKeyEncoding.Encode("acme_prod");

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Encoded_Key_Is_Url_Safe()
    {
        var encoded = GrainKeyEncoding.Encode("ten/ant:weird value+more");

        Assert.DoesNotContain('/', encoded);
        Assert.DoesNotContain('+', encoded);
        Assert.DoesNotContain('=', encoded);
    }

    [Fact]
    public void TryDecode_Rejects_Legacy_Collapsed_Form()
    {
        // The legacy collapsed form is not valid Base64Url of any id in the
        // common case; decoding must fail cleanly rather than throw.
        Assert.False(GrainKeyEncoding.TryDecodeToGrainId("not valid base64!!", out _));
    }
}
