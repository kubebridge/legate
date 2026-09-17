// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.FileSystem

open System
open System.Security.Cryptography
open System.Text

// Options-bound HMAC presigning for the file-system blob store: signed URLs
// are built only when FileSystemStorageOptions carries both a base URL and a
// signing key; otherwise TryGetPresignedUrl returns null. The key travels
// from options to HMAC and never to a log.

/// <summary>
/// HMAC-SHA256 presigned-URL builder for the file-system blob store.
/// Internal: hosts reach presigning only through
/// <see cref="M:Legate.IBlobStore.TryGetPresignedUrl*" />.
/// </summary>
module internal FileSystemSigning =

    /// <summary>
    /// Builds a presigned URL for a blob key: the base URL plus the escaped
    /// key, carrying the expiry as Unix seconds and an HMAC-SHA256
    /// signature (base64url, no padding) over <c>{key}\n{expiry}</c> keyed
    /// by the options-bound signing key.
    /// </summary>
    /// <param name="baseUrl">The configured base URL.</param>
    /// <param name="signingKey">The configured signing key. Never logged.</param>
    /// <param name="key">The validated blob key.</param>
    /// <param name="expiresAt">When the URL stops being valid.</param>
    /// <returns>The signed URL.</returns>
    let signUrl (baseUrl: string) (signingKey: string) (key: string) (expiresAt: DateTimeOffset) : Uri =
        let expirySeconds = expiresAt.ToUnixTimeSeconds()
        let message = Encoding.UTF8.GetBytes(sprintf "%s\n%i" key expirySeconds)

        use hmac = new HMACSHA256(Encoding.UTF8.GetBytes signingKey)
        let signature = hmac.ComputeHash message

        let encoded =
            Convert.ToBase64String(signature).Replace('+', '-').Replace('/', '_').TrimEnd('=')

        let escaped = String.Join("/", key.Split('/') |> Array.map Uri.EscapeDataString)

        Uri(sprintf "%s/%s?exp=%i&sig=%s" (baseUrl.TrimEnd '/') escaped expirySeconds encoded)
