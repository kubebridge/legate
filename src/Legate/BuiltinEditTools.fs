// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Shared implementation behind BuiltinEditTools.CreateEditFileTool: the
// ordinal exact-match, occurrence count, and change report. Internal so the
// public surface stays the factory plus its tool-name constant.
module internal EditFileInternals =

    /// The tool name the model calls, validated once at factory time.
    let toolName = "edit_file"

    /// The model-usable contract: exact matching, the uniqueness rule,
    /// replace_all, and the change report.
    let description =
        "Replace exact text in a workspace file (edit_file). "
        + "Reads the file, replaces old_string with new_string using ordinal exact matching, and writes the result back. "
        + "old_string must occur in the file: the call fails when it is missing, "
        + "and fails when it matches more than once unless replace_all is true. "
        + "Pass replace_all as true to replace every occurrence, false to replace exactly one. "
        + "Returns a change report naming the file and the replacement count. "
        + "path is a workspace-relative slash-separated path."

    /// Counts the ordinal occurrences of oldText in content.
    let countOccurrences (content: string) (oldText: string) : int =
        let mutable count = 0
        let mutable index = content.IndexOf(oldText, StringComparison.Ordinal)

        while index >= 0 do
            count <- count + 1
            index <- content.IndexOf(oldText, index + oldText.Length, StringComparison.Ordinal)

        count

    /// Runs one edit_file invocation against the bound workspace: a
    /// ReadFile/WriteFile round-trip with exact-string and uniqueness checks.
    let invokeAsync
        (workspace: IWorkspace)
        (path: string)
        (old_string: string)
        (new_string: string)
        (replace_all: bool)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            if isNull (box path) then
                raise (ToolException(toolName, "The file path must not be null."))

            if isNull (box old_string) then
                raise (ToolException(toolName, "The old string must not be null."))

            if isNull (box new_string) then
                raise (ToolException(toolName, "The new string must not be null."))

            if old_string = "" then
                raise (ToolException(toolName, "The old string must not be empty."))

            // The round-trip goes through IWorkspace, never the file system
            // directly: path rules and FileNotFound surface as-is. The read
            // stream is fully consumed and disposed before the write below:
            // the workspace replaces the file atomically, which fails while
            // a reader still holds it open.
            let! original =
                task {
                    use! stream = workspace.ReadFile(path, cancellationToken)
                    use reader = new StreamReader(stream, Encoding.UTF8)
                    return! reader.ReadToEndAsync()
                }

            let occurrences = countOccurrences original old_string

            if occurrences = 0 then
                raise (ToolException(toolName, "The old string was not found in the file."))

            if occurrences > 1 && not replace_all then
                raise (
                    ToolException(
                        toolName,
                        sprintf
                            "The old string matched %d times; set replace_all to true to replace every occurrence."
                            occurrences
                    )
                )

            let updated =
                if replace_all then
                    original.Replace(old_string, new_string, StringComparison.Ordinal)
                else
                    let first = original.IndexOf(old_string, StringComparison.Ordinal)

                    original.Substring(0, first)
                    + new_string
                    + original.Substring(first + old_string.Length)

            let content = Encoding.UTF8.GetBytes updated
            do! workspace.WriteFile(path, content, cancellationToken)

            let before = Encoding.UTF8.GetByteCount original

            return
                sprintf "Replaced %d occurrence(s) in %s (%d bytes -> %d bytes)." occurrences path before content.Length
        }

/// The per-workspace invoker behind the edit_file AIFunction. An instance
/// method (not a lambda) so the replace_all default survives on the
/// MethodInfo the factory registers: the model may omit replace_all and the
/// runtime supplies false.
type internal EditInvoker(workspace: IWorkspace) =

    member _.InvokeAsync
        (
            path: string,
            old_string: string,
            new_string: string,
            [<Optional; DefaultParameterValue(false)>] replace_all: bool,
            cancellationToken: CancellationToken
        ) : Task<string> =
        EditFileInternals.invokeAsync workspace path old_string new_string replace_all cancellationToken

/// Factories for the edit_file built-in tool, bound per turn to the session's workspace.
[<Sealed; AbstractClass>]
type BuiltinEditTools =

    /// The name the model calls the edit tool by.
    static member EditFileToolName: string = EditFileInternals.toolName

    /// Builds the edit_file tool over the given workspace: exact-string
    /// replacement with a uniqueness check, carried out as a
    /// ReadFile/WriteFile round-trip on that workspace.
    /// <param name="workspace">The bound workspace the tool reads and writes.</param>
    /// <returns>The edit_file tool offered to the model.</returns>
    static member CreateEditFileTool(workspace: IWorkspace) : AITool =
        ArgumentNullException.ThrowIfNull(workspace)

        let name = ToolNameRules.Validate(EditFileInternals.toolName)

        let method =
            match
                typeof<EditInvoker>
                    .GetMethod("InvokeAsync", BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
            with
            | null -> raise (InvalidOperationException("The edit_file invoker is missing its InvokeAsync method."))
            | found -> found

        AIFunctionFactory.Create(
            method,
            EditInvoker(workspace) :> obj,
            name,
            EditFileInternals.description,
            Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
        )
