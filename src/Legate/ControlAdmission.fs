// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading

/// Execution-context-scoped control admission inherited by nested runner continuations.
module internal ControlAdmission =
    let private current = AsyncLocal<(unit -> bool) option>()

    let check () =
        match current.Value with
        | Some admission -> admission ()
        | None -> true

    let enter admission =
        let previous = current.Value
        current.Value <- Some admission

        { new IDisposable with
            member _.Dispose() = current.Value <- previous
        }
