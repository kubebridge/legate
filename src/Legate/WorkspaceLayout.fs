// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

// Workspace layout roots: the three directories every workspace runtime
// creates per session. The process runtime's CreateDirectoryLayout is the
// conformance target (input/, output/, scratch/ directly under the
// session's directory); these helpers only build the workspace-relative
// paths that address those roots through IWorkspace, so they stay
// consistent across runtimes. Nothing here touches storage: validation
// happens when a path is used, at the IWorkspace implementation.

/// The three layout roots of a bound workspace: the directories the
/// runtime creates per session and the file tools address through
/// <see cref="T:Legate.IWorkspace" /> relative paths. A flat enum so C#
/// hosts can name a root without F# types.
type WorkspaceLayoutRoot =
    /// The session's read-only inputs.
    | Input = 0
    /// The session's durable outputs.
    | Output = 1
    /// The session's throwaway working space.
    | Scratch = 2

/// Builds the workspace-relative paths that address the session's
/// <c>input/</c>, <c>output/</c>, and <c>scratch/</c> roots through
/// <see cref="T:Legate.IWorkspace" />. Pure string combinators: the
/// runtime that bound the workspace owns directory creation (the process
/// runtime's <c>CreateDirectoryLayout</c> is the conformance target), and
/// the workspace validates the combined path on use, so these helpers do
/// not duplicate the path rules.
type WorkspaceLayout private () =

    /// Names the directory a layout root lives under: "input", "output",
    /// or "scratch", matching what the runtimes create per session.
    /// <param name="root">The layout root to name.</param>
    /// <returns>The root's directory name.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The value is not a defined root (only reachable by casting from C#).</exception>
    static member RootName(root: WorkspaceLayoutRoot) : string =
        match root with
        | WorkspaceLayoutRoot.Input -> "input"
        | WorkspaceLayoutRoot.Output -> "output"
        | WorkspaceLayoutRoot.Scratch -> "scratch"
        // Defensive: C# can cast any int to the enum, so an undefined
        // value fails loudly instead of producing a path no runtime owns.
        | _ -> raise (ArgumentOutOfRangeException(nameof root, "The workspace layout root is not defined."))

    /// Guards the caller-supplied relative path: null and empty are
    /// programming errors, and the workspace rejects everything else it
    /// cannot serve when the combined path is used.
    static member private RequireRelativePath(relativePath: string) : string =
        if isNull (box relativePath) then
            raise (ArgumentNullException(nameof relativePath))

        if relativePath = "" then
            raise (ArgumentException("A workspace-relative path must not be empty.", nameof relativePath))

        relativePath

    /// Builds the workspace-relative path of a file under a layout root:
    /// <c>{root}/{relativePath}</c>, for example
    /// <c>Resolve(Input, "data/orders.csv")</c> is
    /// <c>input/data/orders.csv</c>. The combined path is not validated
    /// here; the workspace validates it on use.
    /// <param name="root">The layout root the path lives under.</param>
    /// <param name="relativePath">The path under the root, for example "data/orders.csv". Must not be null or empty.</param>
    /// <returns>The workspace-relative path to pass to <see cref="T:Legate.IWorkspace" />.</returns>
    static member Resolve(root: WorkspaceLayoutRoot, relativePath: string) : string =
        sprintf "%s/%s" (WorkspaceLayout.RootName root) (WorkspaceLayout.RequireRelativePath relativePath)

    /// Builds the workspace-relative path of a file under the
    /// <c>input/</c> root.
    /// <param name="relativePath">The path under <c>input/</c>. Must not be null or empty.</param>
    /// <returns>The workspace-relative path to pass to <see cref="T:Legate.IWorkspace" />.</returns>
    static member InputPath(relativePath: string) : string =
        WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Input, relativePath)

    /// Builds the workspace-relative path of a file under the
    /// <c>output/</c> root.
    /// <param name="relativePath">The path under <c>output/</c>. Must not be null or empty.</param>
    /// <returns>The workspace-relative path to pass to <see cref="T:Legate.IWorkspace" />.</returns>
    static member OutputPath(relativePath: string) : string =
        WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Output, relativePath)

    /// Builds the workspace-relative path of a file under the
    /// <c>scratch/</c> root.
    /// <param name="relativePath">The path under <c>scratch/</c>. Must not be null or empty.</param>
    /// <returns>The workspace-relative path to pass to <see cref="T:Legate.IWorkspace" />.</returns>
    static member ScratchPath(relativePath: string) : string =
        WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Scratch, relativePath)
