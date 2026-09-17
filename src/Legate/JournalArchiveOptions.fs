// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open Microsoft.Extensions.Options

// Host-configured journal archive and cleanup (issue 111). The leased
// archive worker polls closed sessions, writes each due journal to one
// events.jsonl file under ArchiveDirectory after the retention delay,
// verifies it byte-for-byte, then deletes the journal rows through the
// fenced CompleteCleanup. These knobs bind standalone under
// Legate:Archive with standard configuration binding (durations in the
// standard TimeSpan forms, for example "7.00:00:00"); the
// LegateOptionsBinding duration pre-pass targets only the LegateOptions
// root and does not apply here.

// ──────────────────────────────────────────────────────────────────────────
// Options

/// Host-configured journal archive and cleanup. A plain class with mutable
/// properties and defaults, so C# object initialisers work and absent
/// configuration keeps the defaults. An unset (null or empty)
/// <see cref="P:Legate.JournalArchiveOptions.ArchiveDirectory" /> disables
/// the worker's passes: hosts that never archive keep the defaults and the
/// worker sweeps nothing.
/// <remarks>
/// Binds standalone under <c>Legate:Archive</c> through standard
/// configuration binding: durations parse in the standard TimeSpan forms
/// and no custom binder is involved.
/// </remarks>
[<Sealed>]
type JournalArchiveOptions() =

    /// The configuration section these options bind from.
    static member ConfigurationSectionPath = "Legate:Archive"

    /// The local directory archived journals are written to, one
    /// <c>&lt;tenant&gt;_&lt;session&gt;/events.jsonl</c> file per archived
    /// journal. Null or empty disables the worker's passes (the default:
    /// hosts that never archive change nothing). Otherwise the worker
    /// creates it at pass start when absent.
    member val ArchiveDirectory: string | null = null with get, set

    /// How long after a session closes its journal waits before archival.
    /// Must not be negative; zero archives closed sessions on the next
    /// pass. Defaults to seven days.
    member val RetentionDelay: TimeSpan = TimeSpan.FromDays 7.0 with get, set

    /// How long the worker waits between archive passes. Must be positive.
    /// Defaults to one hour.
    member val PollInterval: TimeSpan = TimeSpan.FromHours 1.0 with get, set

    /// How long each journal-cleanup lease lasts before it expires and the
    /// journal re-opens to other claimants. Must be positive. Defaults to
    /// ten minutes, covering the archive of a large journal.
    member val LeaseDuration: TimeSpan = TimeSpan.FromMinutes 10.0 with get, set

    /// The base backoff the worker waits (on the delay seam) after an
    /// archive verification failure defers a journal: the actual wait adds
    /// up to one more base of jitter, so competing workers do not retry in
    /// lockstep. The journal is never deleted on this path. Must be
    /// positive. Defaults to one minute.
    member val VerifyBackoff: TimeSpan = TimeSpan.FromMinutes 1.0 with get, set

    /// Validates the options, returning the first violation or null when
    /// valid. An unset archive directory is valid (the worker stays
    /// disabled); a set one must carry no invalid path characters.
    /// <returns>The first violation, or null when the options are valid.</returns>
    member this.Validate() : string | null =
        if this.PollInterval <= TimeSpan.Zero then
            "JournalArchiveOptions.PollInterval must be positive."
        elif this.LeaseDuration <= TimeSpan.Zero then
            "JournalArchiveOptions.LeaseDuration must be positive."
        elif this.VerifyBackoff <= TimeSpan.Zero then
            "JournalArchiveOptions.VerifyBackoff must be positive."
        elif this.RetentionDelay < TimeSpan.Zero then
            "JournalArchiveOptions.RetentionDelay must not be negative."
        else
            // The guard above proved the directory set; the conversion
            // narrows the nullability the compiler cannot see through it.
            let directory: string = string this.ArchiveDirectory

            if String.IsNullOrWhiteSpace directory then
                null
            elif directory.IndexOfAny(Path.GetInvalidPathChars()) >= 0 then
                "JournalArchiveOptions.ArchiveDirectory carries invalid path characters."
            else
                null

// ──────────────────────────────────────────────────────────────────────────
// Options validation

/// Validates <see cref="T:Legate.JournalArchiveOptions" /> through the
/// options pipeline, surfacing the first violation as the failure message.
type internal JournalArchiveOptionsValidation() =

    interface IValidateOptions<JournalArchiveOptions> with
        member _.Validate(_name: string, options: JournalArchiveOptions) =
            if isNull (box options) then
                ValidateOptionsResult.Fail "JournalArchiveOptions must not be null."
            else
                let violation = options.Validate()

                if isNull (box violation) then
                    ValidateOptionsResult.Success
                else
                    ValidateOptionsResult.Fail $"Invalid JournalArchiveOptions: %s{violation}"
