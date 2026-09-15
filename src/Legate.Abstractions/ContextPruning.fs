// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Shared pure context-size estimation and marker-based pruning over
// transcript cells (issue 44). The Cells.fs precedent applies: one shared
// helper so the journal, store, and export cannot drift. The estimator is a
// documented heuristic (ceiling chars/4 per content class plus a small fixed
// per-cell cost), never an exact tokenizer count: exact BPE counting would
// add a native dependency and break the no-external-services build rule.
// Pruning replaces eligible ToolResult cell contents with the configured
// marker, never touches the last N assistant turns, and reports an audit
// result the caller journals as a ContextPrunedEvent. No TurnLoop wiring
// lives here: the loop hook is owned by issues 38/39.

/// The audit result of one pruning pass: the rewritten transcript plus the
/// counts the caller journals as a
/// <see cref="T:Legate.ContextPrunedEvent" />.
/// <param name="prunedCells">The transcript with eligible tool results replaced by the marker, in transcript order.</param>
/// <param name="prunedCount">How many tool-result cells were replaced with the marker.</param>
/// <param name="beforeEstimate">The estimated token count of the transcript before pruning.</param>
/// <param name="afterEstimate">The estimated token count after pruning, markers included.</param>
[<Sealed>]
type ContextPruneResult
    (prunedCells: IReadOnlyList<SessionCell>, prunedCount: int, beforeEstimate: int64, afterEstimate: int64) =

    do
        if isNull (box prunedCells) then
            raise (ArgumentNullException(nameof prunedCells))

    /// The transcript with eligible tool results replaced by the marker,
    /// in transcript order.
    member _.PrunedCells = prunedCells

    /// How many tool-result cells were replaced with the marker.
    member _.PrunedCount = prunedCount

    /// The estimated token count of the transcript before pruning.
    member _.BeforeEstimate = beforeEstimate

    /// The estimated token count after pruning, markers included.
    member _.AfterEstimate = afterEstimate

/// Shared pure estimation and pruning over transcript cells. Every method is
/// pure: it reads the inputs, returns fresh values, and performs no I/O.
/// <see cref="T:Legate.SessionCellDeriver" /> remains the only transcript
/// fold; this helper operates on already-derived cells.
type ContextPruning() =

    /// The fixed per-cell framing cost the estimator adds to every cell,
    /// covering role and framing overhead no content class carries.
    static member OverheadTokensPerCell = 4

    /// Estimates one cell's token size: the ceiling of its content length
    /// over 4, plus the ceiling of its tool-name length over 4 when set,
    /// plus <see cref="P:Legate.ContextPruning.OverheadTokensPerCell" />.
    /// Text-bearing kinds (User, Assistant, System) estimate from Content;
    /// a ToolCall cell estimates from its tool name plus Content (the
    /// arguments JSON when a host enriches the cell; the fold leaves it
    /// empty); a ToolResult cell estimates from its result Content. Null
    /// Content or ToolName contributes nothing. The estimate grows
    /// monotonically with content length: it is a heuristic bound, not an
    /// exact tokenizer count.
    /// <param name="cell">The cell to estimate. Must not be null.</param>
    /// <returns>The cell's estimated token count.</returns>
    /// <exception cref="T:System.ArgumentNullException">The cell is null.</exception>
    static member EstimateCell(cell: SessionCell) : int64 =
        if isNull (box cell) then
            raise (ArgumentNullException(nameof cell))

        let contentChars =
            match cell.Content with
            | null -> 0L
            | text -> int64 text.Length

        let nameChars =
            match cell.ToolName with
            | null -> 0L
            | name -> int64 name.Length

        let ceilingDiv4 (chars: int64) = (chars + 3L) / 4L

        int64 ContextPruning.OverheadTokensPerCell
        + ceilingDiv4 contentChars
        + ceilingDiv4 nameChars

    /// Estimates a transcript's token size: the sum of
    /// <c>EstimateCell</c> over the cells, in order. Pure; a null element
    /// fails rather than estimating silently.
    /// <param name="cells">The cells, in transcript order. Must not be null and must not contain null.</param>
    /// <returns>The transcript's estimated token count.</returns>
    /// <exception cref="T:System.ArgumentNullException">The cell list is null, or an element is null.</exception>
    static member Estimate(cells: IReadOnlyList<SessionCell>) : int64 =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        let mutable total = 0L

        for cell in cells do
            total <- total + ContextPruning.EstimateCell cell

        total

    /// Derives the prune threshold from the model's catalog entry: the
    /// entry's <c>ContextWindowTokens</c> minus its
    /// <c>ReservedOutputTokens</c> minus the configured
    /// <c>ReservedBufferTokens</c>, clamped at zero. A null entry means the
    /// model is unknown to the catalog, so both limits fall back to
    /// <see cref="P:Legate.DefaultModelCatalog.FallbackContextWindowTokens" />
    /// and
    /// <see cref="P:Legate.DefaultModelCatalog.FallbackReservedOutputTokens" />
    /// (128,000 total with a 20,000 reserve).
    /// <param name="entry">The model's catalog entry, or null when the model is unknown.</param>
    /// <param name="reservedBufferTokens">The extra buffer held back beyond the reserved output. Must be at least 0.</param>
    /// <returns>The estimated-token budget the transcript must fit; never negative.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The buffer is negative.</exception>
    static member PruneThreshold(entry: ModelCatalogEntry | null, reservedBufferTokens: int) : int =
        if reservedBufferTokens < 0 then
            raise (ArgumentOutOfRangeException(nameof reservedBufferTokens, "ReservedBufferTokens must be at least 0."))

        let contextWindow =
            match entry with
            | null -> DefaultModelCatalog.FallbackContextWindowTokens
            | known -> known.ContextWindowTokens

        let reservedOutput =
            match entry with
            | null -> DefaultModelCatalog.FallbackReservedOutputTokens
            | known -> known.ReservedOutputTokens

        max 0 (contextWindow - reservedOutput - reservedBufferTokens)

    /// Replaces every eligible ToolResult cell's content with the
    /// configured marker and reports the audit counts. A ToolResult is
    /// eligible when it sits before the protected tail: the tail starts at
    /// the Nth-from-last Assistant cell for
    /// <c>KeepLastAssistantTurns</c> N, so the last N assistant turns keep
    /// their results. Zero keeps protect nothing; a keep at or above the
    /// assistant-cell count protects the whole transcript; with no
    /// assistant cells there is no recent turn to protect, so every tool
    /// result is eligible. Cells already carrying the marker are left
    /// alone and uncounted, so repeated passes are idempotent.
    /// Non-ToolResult cells are never touched.
    /// <param name="cells">The cells, in transcript order. Must not be null and must not contain null.</param>
    /// <param name="options">The pruning knobs. Must not be null and must validate.</param>
    /// <returns>The rewritten transcript with before/after estimates and the replaced count.</returns>
    /// <exception cref="T:System.ArgumentNullException">The cell list or the options are null, or a cell is null.</exception>
    /// <exception cref="T:System.ArgumentException">The options do not validate.</exception>
    static member Prune(cells: IReadOnlyList<SessionCell>, options: ContextPruningOptions) : ContextPruneResult =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        match options.Validate() with
        | null -> ()
        | violation -> raise (ArgumentException(violation, nameof options))

        let beforeEstimate = ContextPruning.Estimate cells
        let marker = options.PrunedMarker
        let keep = options.KeepLastAssistantTurns

        let assistantIndices = ResizeArray<int>()

        for index in 0 .. cells.Count - 1 do
            let cell = cells[index]

            if isNull (box cell) then
                raise (ArgumentNullException(nameof cells))

            if cell.Kind = SessionCellKind.Assistant then
                assistantIndices.Add index

        // The first protected index: everything at or after it keeps its
        // results. Zero keeps protect nothing, and with no assistant
        // cells there is no recent turn to protect; a keep covering
        // every assistant cell protects index 0 onward, which prunes
        // nothing because no index sits before it.
        let protectedStart =
            if keep <= 0 || assistantIndices.Count = 0 then cells.Count
            elif assistantIndices.Count <= keep then 0
            else assistantIndices[assistantIndices.Count - keep]

        let pruned = ResizeArray<SessionCell>(cells.Count)
        let mutable prunedCount = 0

        for index in 0 .. cells.Count - 1 do
            let cell = cells[index]

            if
                cell.Kind = SessionCellKind.ToolResult
                && index < protectedStart
                && cell.Content <> marker
            then
                pruned.Add { cell with Content = marker }
                prunedCount <- prunedCount + 1
            else
                pruned.Add cell

        let afterEstimate = ContextPruning.Estimate(pruned :> IReadOnlyList<SessionCell>)

        ContextPruneResult(pruned :> IReadOnlyList<SessionCell>, prunedCount, beforeEstimate, afterEstimate)
