// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System

// Settings for the S3 stores, bound from the Legate:Storage:S3
// configuration section. A plain class with mutable properties and
// defaults, so hosts can set properties before registering and the
// configuration binder only overrides the keys the host sets. The signing
// key stays options-bound and is never logged: exception messages carry
// the endpoint and bucket, never the credentials.

/// <summary>
/// Where the S3 stores live and how they sign: the service endpoint, the
/// region, the bucket, the credentials, the path-style switch, and the
/// presigned-URL expiry cap. Bound from the
/// <c>Legate:Storage:S3</c> configuration section.
/// </summary>
/// <remarks>
/// <para>Conventions per endpoint:</para>
/// <list type="bullet">
/// <item><description>AWS: leave <see cref="P:Legate.Storage.S3.S3StorageOptions.ServiceUrl" />
/// empty so the SDK resolves the default endpoint chain, keep
/// <see cref="P:Legate.Storage.S3.S3StorageOptions.ForcePathStyle" /> false
/// for virtual-hosted style, and set <see cref="P:Legate.Storage.S3.S3StorageOptions.Region" />
/// to the bucket's region. Conditional writes are supported.</description></item>
/// <item><description>Hetzner Object Storage: set
/// <see cref="P:Legate.Storage.S3.S3StorageOptions.ServiceUrl" /> to the
/// regional endpoint (for example
/// <c>https://fsn1.your-objectstorage.com</c>), keep
/// <see cref="P:Legate.Storage.S3.S3StorageOptions.ForcePathStyle" /> true,
/// and set <see cref="P:Legate.Storage.S3.S3StorageOptions.Region" /> to
/// the location (for example <c>fsn1</c>).</description></item>
/// <item><description>MinIO (including the Testcontainers conformance
/// harness): set <see cref="P:Legate.Storage.S3.S3StorageOptions.ServiceUrl" />
/// to the server URL, keep
/// <see cref="P:Legate.Storage.S3.S3StorageOptions.ForcePathStyle" /> true.
/// Recent releases evaluate conditional writes; older ones predate them.
/// </description></item>
/// </list>
/// <para>Support matrix: compare-exchange and the package publish pointer
/// commit require endpoint support for the <c>If-Match</c> and
/// <c>If-None-Match</c> preconditions. AWS and recent MinIO evaluate them;
/// Hetzner conditional-write support is unproven against a live endpoint;
/// an endpoint that rejects them fails fast with
/// <see cref="T:Legate.Storage.S3.S3ConditionalWriteNotSupportedException" />
/// and writes nothing: there is deliberately no read-modify-write
/// fallback, so a non-supporting endpoint can never silently lose a write
/// race.</para>
/// </remarks>
type S3StorageOptions() =

    /// <summary>
    /// The configuration section the stores bind from:
    /// <c>Legate:Storage:S3</c>.
    /// </summary>
    static member ConfigurationSectionPath = "Legate:Storage:S3"

    /// <summary>
    /// The S3 service endpoint URL, for example
    /// <c>https://fsn1.your-objectstorage.com</c> or
    /// <c>http://127.0.0.1:9000</c>. Empty means the AWS default endpoint
    /// chain. Must be empty or an absolute HTTP(S) URL; never carries
    /// credentials.
    /// </summary>
    member val ServiceUrl: string = "" with get, set

    /// <summary>
    /// The region the client signs for, for example <c>us-east-1</c> on
    /// AWS or <c>fsn1</c> on Hetzner. Defaults to <c>us-east-1</c>; used
    /// as the signing region against custom endpoints and as the AWS
    /// region against the default chain. Must be non-empty.
    /// </summary>
    member val Region: string = "us-east-1" with get, set

    /// <summary>
    /// The bucket every store reads and writes. Created on first write
    /// when the credentials allow it; reads against a missing bucket
    /// report absent instead of creating it. Must be a non-empty bucket
    /// name without slashes.
    /// </summary>
    member val Bucket: string = "" with get, set

    /// <summary>
    /// The access key id the client signs with. Empty together with an
    /// empty <see cref="P:Legate.Storage.S3.S3StorageOptions.SecretAccessKey" />
    /// means credentialless: data operations fail server-side and
    /// presigning returns null. Never logged.
    /// </summary>
    member val AccessKeyId: string = "" with get, set

    /// <summary>
    /// The secret access key the client signs with. Empty together with an
    /// empty <see cref="P:Legate.Storage.S3.S3StorageOptions.AccessKeyId" />
    /// means credentialless: data operations fail server-side and
    /// presigning returns null. Never logged, never embedded in exception
    /// messages.
    /// </summary>
    member val SecretAccessKey: string = "" with get, set

    /// <summary>
    /// Whether the client addresses objects path-style
    /// (<c>endpoint/bucket/key</c>) instead of virtual-hosted style
    /// (<c>bucket.endpoint/key</c>). Defaults to false (AWS convention);
    /// set true for Hetzner and MinIO.
    /// </summary>
    member val ForcePathStyle: bool = false with get, set

    /// <summary>
    /// The cap on presigned-URL lifetimes: a presign call asking for more
    /// receives this instead. Defaults to fifteen minutes. Must be
    /// positive.
    /// </summary>
    member val PresignExpiry: TimeSpan = TimeSpan.FromMinutes 15. with get, set

    /// <summary>
    /// Checks the options: the bucket must be a non-empty slash-free
    /// name, the region non-empty, the endpoint empty or an absolute
    /// HTTP(S) URL, the credentials both set or both empty, and the
    /// presign expiry positive.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.Bucket then
            "S3StorageOptions.Bucket must be a non-empty bucket name."
        elif this.Bucket.Contains '/' then
            "S3StorageOptions.Bucket must not contain slashes."
        elif String.IsNullOrWhiteSpace this.Region then
            "S3StorageOptions.Region must be a non-empty region name."
        elif isNull (box this.ServiceUrl) then
            "S3StorageOptions.ServiceUrl must not be null: use the empty string for the AWS default endpoint chain."
        elif
            this.ServiceUrl <> ""
            && not (
                Uri.TryCreate(this.ServiceUrl, UriKind.Absolute) |> fst
                && (this.ServiceUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                    || this.ServiceUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            )
        then
            "S3StorageOptions.ServiceUrl must be empty or an absolute HTTP(S) URL."
        elif isNull (box this.AccessKeyId) || isNull (box this.SecretAccessKey) then
            "S3StorageOptions.AccessKeyId and S3StorageOptions.SecretAccessKey must not be null: use empty strings for credentialless configuration."
        elif
            (String.IsNullOrEmpty this.AccessKeyId
             <> String.IsNullOrEmpty this.SecretAccessKey)
        then
            "S3StorageOptions.AccessKeyId and S3StorageOptions.SecretAccessKey must both be set or both be empty."
        elif this.PresignExpiry <= TimeSpan.Zero then
            "S3StorageOptions.PresignExpiry must be positive."
        else
            null
