// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Tool-result cell artifact enrichment (issue 117). The fold in Cells.fs
// never sets Artifacts, so the runtime enriches ToolResult cells here: the
// [artifact: name="..."] references the sinks substituted into tool output
// parse back into the cell's Artifacts list through the shared
// ArtifactReference helper. Pure: reads the cells, returns fresh cells for
// enriched kinds, no I/O. Wired once in Transcripts.readTranscript, so
// every served transcript carries the enrichment.
module internal SessionArtifactEnrichment =

    /// Enriches one cell: a ToolResult cell whose content references
    /// artifacts gains their names, in order; every other cell passes
    /// through by reference.
    /// <param name="cell">The cell to enrich. Must not be null.</param>
    /// <returns>The enriched cell, or the cell itself when it carries no references.</returns>
    let private enrichOne (cell: SessionCell) : SessionCell =
        if isNull (box cell) then
            raise (ArgumentNullException(nameof cell))

        if cell.Kind <> SessionCellKind.ToolResult || isNull (box cell.Content) then
            cell
        else
            let names = ArtifactReference.TryParseNames cell.Content

            if names.Count = 0 then
                cell
            else
                { cell with Artifacts = names }

    /// Enriches transcript cells with their artifact references.
    /// <param name="cells">The derived cells, in transcript order. Must not be null.</param>
    /// <returns>The cells with ToolResult references enriched, in the same order.</returns>
    let enrichCells (cells: IReadOnlyList<SessionCell>) : IReadOnlyList<SessionCell> =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        let enriched = ResizeArray<SessionCell>(cells.Count)

        for cell in cells do
            enriched.Add(enrichOne cell)

        enriched :> IReadOnlyList<SessionCell>
