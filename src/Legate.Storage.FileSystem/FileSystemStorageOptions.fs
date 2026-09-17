// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.FileSystem

open System

// Options for the file-system stores: the rooted directory both stores share
// plus the optional presigned-URL pair. A reference type with mutable
// properties so absent configuration keeps the defaults and C# object
// initialisers work. The signing key is options-bound and never logged: the
// stores snapshot it at construction and only feed it to HMAC-SHA256.

/// <summary>
/// Where the file-system stores live and how presigned URLs are minted: the
/// rooted directory shared by
/// <see cref="T:Legate.Storage.FileSystem.FileSystemBlobStore" /> and
/// <see cref="T:Legate.Storage.FileSystem.FileSystemAgentPackageStore" />,
/// plus the optional base URL and signing key presigning is built from.
/// </summary>
/// <remarks>
/// Defaults describe a host that only reads and writes local files: no base
/// URL and no signing key, so
/// <see cref="M:Legate.IBlobStore.TryGetPresignedUrl*" /> returns null. A
/// base URL without a signing key (or the reverse) is a half-configured pair
/// and fails <see cref="M:Legate.Storage.FileSystem.FileSystemStorageOptions.Validate" />:
/// presigned URLs are minted only when both are set.
/// </remarks>
type FileSystemStorageOptions() =

    /// <summary>
    /// The rooted directory both stores share. Created with its parents on
    /// first use when absent. Must be a non-empty directory path.
    /// </summary>
    member val RootDirectory: string = "" with get, set

    /// <summary>
    /// The base URL presigned blob URLs are built under, for example
    /// <c>https://files.example/files</c>. Null (the default) means the
    /// backend cannot presign. Must be set together with
    /// <see cref="P:Legate.Storage.FileSystem.FileSystemStorageOptions.PresignedSigningKey" />.
    /// </summary>
    member val PresignedBaseUrl: string | null = null with get, set

    /// <summary>
    /// The key presigned blob URLs are signed with (HMAC-SHA256). Null (the
    /// default) means the backend cannot presign. Must be set together with
    /// <see cref="P:Legate.Storage.FileSystem.FileSystemStorageOptions.PresignedBaseUrl" />;
    /// never logged.
    /// </summary>
    member val PresignedSigningKey: string | null = null with get, set

    /// <summary>
    /// Checks the options: the root directory must be non-empty, the base
    /// URL and signing key must be set together or both absent, and a set
    /// base URL must be absolute.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.RootDirectory then
            "FileSystemStorageOptions.RootDirectory must be a non-empty directory path."
        else
            let baseSet = not (String.IsNullOrWhiteSpace this.PresignedBaseUrl)
            let keySet = not (String.IsNullOrWhiteSpace this.PresignedSigningKey)

            if baseSet <> keySet then
                "FileSystemStorageOptions.PresignedBaseUrl and FileSystemStorageOptions.PresignedSigningKey must be set together: presigned URLs are minted only when both are configured."
            elif baseSet then
                if Uri.IsWellFormedUriString(this.PresignedBaseUrl, UriKind.Absolute) then
                    null
                else
                    "FileSystemStorageOptions.PresignedBaseUrl must be an absolute URL."
            else
                null
