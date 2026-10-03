// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage

open System
open System.Collections.Generic
open System.Text
open System.Text.Json
open Legate

/// Linked into all providers so scope and finite-fence rules are identical.
module internal RecoveryCandidateCursor =
    type Position = { Upper: string; Last: string }

    let validate state size =
        if size < 1 || size > 1000 then
            raise (ArgumentOutOfRangeException(nameof size))

        if not (Enum.IsDefined(typeof<SessionState>, state)) then
            raise (ArgumentOutOfRangeException(nameof state))

    let decode (tenant: TenantId) (state: SessionState) (token: string | null) =
        match token with
        | null -> None
        | token ->
            try
                if token.Length > 4096 then
                    raise (FormatException())

                let json = Convert.FromBase64String token |> Encoding.UTF8.GetString
                let fields = JsonSerializer.Deserialize<string[]>(json)

                match fields with
                | null -> raise (FormatException())
                | fields when
                    fields.Length = 5
                    && fields[0] = "1"
                    && fields[1] = tenant.ToString()
                    && fields[2] = state.ToString()
                    ->
                    let upper = SessionId.Parse(fields[3]).ToString()
                    let last = SessionId.Parse(fields[4]).ToString()

                    if
                        upper <> fields[3]
                        || last <> fields[4]
                        || StringComparer.Ordinal.Compare(last, upper) > 0
                    then
                        raise (FormatException())

                    Some { Upper = upper; Last = last }
                | _ -> raise (FormatException())
            with _ ->
                raise (ArgumentException("Malformed or mismatched recovery cursor.", nameof token))

    let page (tenant: TenantId) (state: SessionState) size (upper: string) (rows: string list) : SessionCandidatePage =
        let emitted = rows |> List.truncate size

        let token: string | null =
            if rows.Length <= size then
                null
            else
                let last = emitted |> List.last

                JsonSerializer.Serialize(
                    [|
                        "1"
                        tenant.ToString()
                        state.ToString()
                        upper
                        last
                    |]
                )
                |> Encoding.UTF8.GetBytes
                |> Convert.ToBase64String

        {
            Items = (emitted |> List.map SessionId.Parse |> List.toArray) :> IReadOnlyList<SessionId>
            Continuation = token
        }
