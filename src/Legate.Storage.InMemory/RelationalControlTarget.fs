// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage

open System
open System.Data.Common
open System.Globalization
open System.Text.Json
open System.Threading
open Legate

// The owning provider begins the write transaction and supplies qualified names.
// PostgreSQL always locks the tenant/session before any target/claim/evidence rows.
module internal RelationalControlTarget =
    let command (connection: DbConnection) (transaction: DbTransaction) sql tenant sessionId =
        let cmd = connection.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- sql

        for name, value in
            [
                "t", tenant.ToString()
                "sid", sessionId.ToString()
            ] do
            let parameter = cmd.CreateParameter()
            parameter.ParameterName <- name
            parameter.Value <- value
            cmd.Parameters.Add parameter |> ignore

        cmd

    let initialize connection transaction controlTable tenant sessionId =
        use cmd =
            command
                connection
                transaction
                $"INSERT INTO {controlTable} (tenant,session_id,format_version,control_json) VALUES (@t,@sid,1,@json)"
                tenant
                sessionId

        let parameter = cmd.CreateParameter()
        parameter.ParameterName <- "json"
        parameter.Value <- ControlTargetProtocol.encode (ControlTargetProtocol.fresh ())
        cmd.Parameters.Add parameter |> ignore
        cmd.ExecuteNonQuery() |> ignore

    let load connection transaction controlTable tenant sessionId =
        use cmd =
            command
                connection
                transaction
                $"SELECT format_version,control_json FROM {controlTable} WHERE tenant=@t AND session_id=@sid"
                tenant
                sessionId

        use reader = cmd.ExecuteReader()

        if not (reader.Read()) || reader.GetInt32(0) <> 1 then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Legacy control state is unsupported. Start a new session."
                )
            )

        ControlTargetProtocol.decode sessionId (reader.GetString(1))

    let requireNoBinding connection transaction controlTable tenant sessionId =
        let state = load connection transaction controlTable tenant sessionId

        if not (isNull (box state.Binding)) then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "controlPending",
                    "Retire current control work before changing prime ownership or lifecycle."
                )
            )

    let invoke
        connection
        transaction
        sessionTable
        inboxTable
        turnsTable
        controlTable
        postgres
        tenant
        sessionId
        (utcNow: unit -> DateTimeOffset)
        (ct: CancellationToken)
        operation
        =
        ct.ThrowIfCancellationRequested()

        let lifecycle, current =
            let locking = if postgres then " FOR UPDATE" else ""

            use cmd =
                command
                    connection
                    transaction
                    $"SELECT state,current_turn_id FROM {sessionTable} WHERE tenant=@t AND id=@sid{locking}"
                    tenant
                    sessionId

            use reader = cmd.ExecuteReader()

            if not (reader.Read()) then
                raise (SessionNotFoundException(sessionId, "The session does not exist in this tenant."))

            Enum.Parse<SessionState>(reader.GetString(0)),
            (if reader.IsDBNull(1) then
                 None
             else
                 Some(reader.GetString(1)))

        let state = load connection transaction controlTable tenant sessionId

        let prime =
            match current with
            | None -> None
            | Some turn ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT turn_id,claim_token,claim_owner,claim_expires_at,attempt FROM {turnsTable} WHERE tenant=@t AND session_id=@sid AND turn_id=@turn AND status IN ('Pending','Running','Suspended')"
                        tenant
                        sessionId

                let parameter = cmd.CreateParameter()
                parameter.ParameterName <- "turn"
                parameter.Value <- turn
                cmd.Parameters.Add parameter |> ignore
                use reader = cmd.ExecuteReader()

                if
                    reader.Read()
                    && not (reader.IsDBNull(1))
                    && not (reader.IsDBNull(2))
                    && not (reader.IsDBNull(3))
                then
                    Some
                        {
                            TurnId = TurnId.Parse(reader.GetString(0))
                            Token = reader.GetString(1)
                            Owner = reader.GetString(2)
                            ExpiresAt = DateTimeOffset.Parse(reader.GetString(3), CultureInfo.InvariantCulture)
                            Attempt = reader.GetInt32(4)
                        }
                else
                    None

        let entries =
            use cmd =
                command
                    connection
                    transaction
                    $"SELECT position,consumed,payload_json FROM {inboxTable} WHERE tenant=@t AND session_id=@sid"
                    tenant
                    sessionId

            use reader = cmd.ExecuteReader()
            let collected = ResizeArray<int64 * bool>()

            while reader.Read() do
                use payload = JsonDocument.Parse(reader.GetString(2))
                let mutable kind = Unchecked.defaultof<JsonElement>

                if not (payload.RootElement.TryGetProperty("$type", &kind)) then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            "unsupportedInboxFormat",
                            "Unsupported inbox payload. Start a new session."
                        )
                    )

                if kind.GetString() = "userMessage" then
                    collected.Add(
                        reader.GetInt64(0),
                        Convert.ToBoolean(reader.GetValue(1), CultureInfo.InvariantCulture)
                    )
                elif kind.GetString() <> "reply" then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            "unsupportedInboxFormat",
                            "Unsupported inbox payload. Start a new session."
                        )
                    )

            collected |> Map.ofSeq

        let context =
            {
                SessionId = sessionId
                Lifecycle = lifecycle
                Now = utcNow ()
                Prime = prime
                Entries = entries
            }

        let result, updated = operation context state
        ct.ThrowIfCancellationRequested()

        if not (obj.ReferenceEquals(state, updated)) then
            use cmd =
                command
                    connection
                    transaction
                    $"UPDATE {controlTable} SET control_json=@json WHERE tenant=@t AND session_id=@sid AND format_version=1"
                    tenant
                    sessionId

            let parameter = cmd.CreateParameter()
            parameter.ParameterName <- "json"
            parameter.Value <- ControlTargetProtocol.encode updated
            cmd.Parameters.Add parameter |> ignore

            if cmd.ExecuteNonQuery() <> 1 then
                raise (InvalidOperationException("Control record changed unexpectedly."))

        ct.ThrowIfCancellationRequested()
        result
