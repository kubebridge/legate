// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

// Turns a synchronous list into an IAsyncEnumerable that yields without
// asynchrony.
module internal SqliteAsync =

    /// Adapts a list into an IAsyncEnumerable, in order.
    let ofList (items: 'T list) : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_: CancellationToken) : IAsyncEnumerator<'T> =
                let items = items |> List.toArray
                let mutable index = -1

                { new IAsyncEnumerator<'T> with
                    member this.MoveNextAsync() : ValueTask<bool> =
                        let mutable next = false

                        if index + 1 < items.Length then
                            index <- index + 1
                            next <- true

                        ValueTask<bool>(next)

                    member _.Current: 'T = items[index]

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

/// A write stream that buffers locally and commits its bytes to the SQLite
/// database when disposed: an abandoned stream that is never disposed
/// commits nothing, and a disposed stream lands atomically.
type internal SqliteCommitOnDisposeStream(database: SqliteDatabase, key: string, contentType: string) =
    inherit MemoryStream()

    override this.Dispose(disposing: bool) =
        if disposing then
            let bytes = this.ToArray()

            try
                lock database.Gate (fun () ->
                    use connection = database.OpenConnection()

                    use command = connection.CreateCommand()

                    let blobs = database.Table "blobs"
                    let etag = Ulid.NewUlid().ToString()

                    command.CommandText <-
                        $"INSERT INTO \"%s{blobs}\" (key, content, content_type, etag, size_bytes) VALUES ($key, $content, $type, $etag, $size) ON CONFLICT (key) DO UPDATE SET content = $content, content_type = $type, etag = $etag, size_bytes = $size"

                    command.Parameters.AddWithValue("$key", key) |> ignore
                    command.Parameters.Add("$content", SqliteType.Blob).Value <- bytes
                    command.Parameters.AddWithValue("$type", contentType) |> ignore
                    command.Parameters.AddWithValue("$etag", etag) |> ignore
                    command.Parameters.AddWithValue("$size", int64 bytes.Length) |> ignore
                    command.ExecuteNonQuery() |> ignore)
                |> ignore
            with :? SqliteException as sql ->
                raise (SqliteErrors.ofSqliteException database.Path sql)

        base.Dispose disposing

    override this.DisposeAsync() : ValueTask =
        this.Dispose(true)
        ValueTask()

// The SQLite IBlobStore: the blob primitives over one shared database
// table. Etags are fresh ULIDs per write and never reused; OpenWrite
// buffers and commits atomically on dispose, so an abandoned stream never
// commits and readers never see a partial blob; presigning returns null
// (the backend cannot presign); keys and prefixes validate through
// BlobKeys. Every SqliteException funnels through the SqliteErrors
// boundary.

/// <summary>
/// The SQLite <see cref="T:Legate.IBlobStore" /> over one shared database
/// file.
/// </summary>
/// <param name="database">The shared database every store uses. Must not be null.</param>
type SqliteBlobStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let path = database.Path
    let mapSql (ex: SqliteException) : LegateException = SqliteErrors.ofSqliteException path ex
    let blobsTable () = database.Table "blobs"

    let write (connection: SqliteConnection) (key: string) (bytes: byte[]) (contentType: string) : BlobMetadata =
        use command = connection.CreateCommand()

        let etag = Ulid.NewUlid().ToString()
        let metadata = BlobMetadata(contentType, int64 bytes.Length, etag)

        command.CommandText <-
            $"INSERT INTO \"%s{blobsTable ()}\" (key, content, content_type, etag, size_bytes) VALUES ($key, $content, $type, $etag, $size) ON CONFLICT (key) DO UPDATE SET content = $content, content_type = $type, etag = $etag, size_bytes = $size"

        command.Parameters.AddWithValue("$key", key) |> ignore
        command.Parameters.Add("$content", SqliteType.Blob).Value <- bytes
        command.Parameters.AddWithValue("$type", contentType) |> ignore
        command.Parameters.AddWithValue("$etag", etag) |> ignore
        command.Parameters.AddWithValue("$size", int64 bytes.Length) |> ignore
        command.ExecuteNonQuery() |> ignore
        metadata

    interface IBlobStore with

        member _.Get(key, _) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <- $"SELECT content FROM \"%s{blobsTable ()}\" WHERE key = $key"

                            command.Parameters.AddWithValue("$key", key) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                reader.GetFieldValue<byte[]>(0)
                            else
                                Unchecked.defaultof<byte[]>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.Put(key, content, _) =
            task {
                if isNull (box content.Bytes) then
                    raise (ArgumentNullException(nameof content))

                if isNull (box content.ContentType) then
                    raise (ArgumentNullException(nameof content))

                BlobKeys.Validate key |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            write connection key content.Bytes content.ContentType)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CompareExchange(key, content, expectedEtag, _) =
            task {
                if isNull (box content.Bytes) then
                    raise (ArgumentNullException(nameof content))

                if isNull (box content.ContentType) then
                    raise (ArgumentNullException(nameof content))

                BlobKeys.Validate key |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use check = connection.CreateCommand()
                            check.Transaction <- transaction

                            check.CommandText <- $"SELECT etag FROM \"%s{blobsTable ()}\" WHERE key = $key"

                            check.Parameters.AddWithValue("$key", key) |> ignore

                            use reader = check.ExecuteReader()
                            let hasRow = reader.Read()

                            let storedEtag = if hasRow then Some(reader.GetString(0)) else None

                            reader.Close()

                            let matches =
                                match expectedEtag, storedEtag with
                                | null, None -> true
                                | null, Some _ -> false
                                | _, None -> false
                                | etag, Some stored -> String.Equals(etag, stored, StringComparison.Ordinal)

                            if matches then
                                let metadata = write connection key content.Bytes content.ContentType
                                transaction.Commit()
                                metadata
                            else
                                transaction.Rollback()
                                Unchecked.defaultof<BlobMetadata>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.OpenRead(key, _) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <- $"SELECT content FROM \"%s{blobsTable ()}\" WHERE key = $key"

                            command.Parameters.AddWithValue("$key", key) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                new MemoryStream(reader.GetFieldValue<byte[]>(0), false) :> Stream
                            else
                                raise (FileNotFoundException(sprintf "No blob exists under key %s." key, key)))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.OpenWrite(key, contentType, _) =
            task {
                if isNull (box contentType) then
                    raise (ArgumentNullException(nameof contentType))

                BlobKeys.Validate key |> ignore

                try
                    return new SqliteCommitOnDisposeStream(database, key, contentType) :> Stream
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.List(prefix, _) =
            BlobKeys.ValidatePrefix prefix |> ignore

            lock database.Gate (fun () ->
                try
                    use connection = database.OpenConnection()

                    use command = connection.CreateCommand()

                    command.CommandText <-
                        $"SELECT key FROM \"%s{blobsTable ()}\" WHERE key LIKE $like ESCAPE '\\' ORDER BY key"

                    let escaped = prefix.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_")

                    command.Parameters.AddWithValue("$like", escaped + "%") |> ignore

                    use reader = command.ExecuteReader()
                    let keys = List<string>()

                    while reader.Read() do
                        keys.Add(reader.GetString(0))

                    keys |> Seq.toList
                with :? SqliteException as sql ->
                    raise (mapSql sql))
            |> SqliteAsync.ofList

        member _.DeletePrefix(prefix, _) =
            task {
                BlobKeys.ValidatePrefix prefix |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"DELETE FROM \"%s{blobsTable ()}\" WHERE key LIKE $like ESCAPE '\\'"

                            let escaped = prefix.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_")

                            command.Parameters.AddWithValue("$like", escaped + "%") |> ignore
                            command.ExecuteNonQuery())
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.GetMetadata(key, _) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT content_type, size_bytes, etag FROM \"%s{blobsTable ()}\" WHERE key = $key"

                            command.Parameters.AddWithValue("$key", key) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                BlobMetadata(reader.GetString(0), reader.GetInt64(1), reader.GetString(2))
                            else
                                Unchecked.defaultof<BlobMetadata>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.TryGetPresignedUrl(_, _, _) =
            // The SQLite backend cannot presign.
            Task.FromResult Unchecked.defaultof<Uri>
