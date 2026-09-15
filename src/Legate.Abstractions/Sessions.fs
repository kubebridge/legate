// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Session contracts. A Session is one conversation with one agent that
// lives until it is closed or expires: identity, tenant, title, lifecycle
// state, the turn running now, timestamps, the workspace binding it points
// at, and the options it was opened with. Headless runs are ordinary
// sessions opened with options (AutoClose, an outcome mode, a permission
// policy selection, and an optional completion sink), never a separate
// concept. All types serialise with System.Text.Json and are constructible
// from C# through property setters.

/// The lifecycle of a session. A session opens Idle, enters Running while a
/// turn executes, may suspend to WaitingForInput while the host is expected
/// to reply, and ends in Closed, which is terminal.
type SessionState =

    /// The session exists but no turn is executing: the state a session is
    /// opened in and the state it returns to when a turn finishes.
    /// Transitions: to <see cref="F:Legate.SessionState.Running" /> when a
    /// turn starts.
    | Idle = 0

    /// A turn is executing in the session. Transitions: from
    /// <see cref="F:Legate.SessionState.Idle" /> when a prompt starts a
    /// turn; back to Idle when the turn finishes; to
    /// <see cref="F:Legate.SessionState.WaitingForInput" /> when the turn
    /// suspends on a permission request or question; to
    /// <see cref="F:Legate.SessionState.Closed" /> when the session is
    /// closed or expires.
    | Running = 1

    /// A turn is suspended awaiting a host reply (a permission decision or
    /// a question answer). Transitions: from Running when a turn suspends;
    /// back to <see cref="F:Legate.SessionState.Running" /> when the host
    /// replies. Messages prompted while waiting join the inbox instead of
    /// starting a turn.
    | WaitingForInput = 2

    /// Terminal: the session was closed by the host or expired. No
    /// transitions leave Closed; prompting, replying, or otherwise acting
    /// on a closed session is rejected.
    | Closed = 3

/// How a session reports completion when it runs headless: the mode set
/// through <see cref="P:Legate.SessionOptions.Outcome" />. The structured
/// outcome values themselves belong to the turn result contract, not here.
type SessionOutcomeMode =

    /// The interactive default: the session reports no structured
    /// completion, and hosts observe state and transcript instead.
    | None = 0

    /// The session's completion sink receives a structured outcome when the
    /// session completes.
    | Structured = 1

/// How a session is opened: display title, headless behaviour (AutoClose
/// and the outcome mode), the permission policy selection, the completion
/// sink, per-turn limits, and host metadata. A reference type with mutable
/// properties so absent JSON properties keep the defaults and C# object
/// initialisers work. Defaults produce an interactive session:
/// <see cref="P:Legate.SessionOptions.AutoClose" /> is false and
/// <see cref="P:Legate.SessionOptions.Outcome" /> is
/// <see cref="F:Legate.SessionOutcomeMode.None" />.
type SessionOptions() =

    /// The session's display title, or null when the host supplies none and
    /// the runtime titles the session itself.
    member val Title: string | null = null with get, set

    /// Whether the session closes itself when its headless run finishes.
    /// Defaults to false: an interactive session stays open until the host
    /// closes it.
    member val AutoClose: bool = false with get, set

    /// How the session reports completion when it runs headless. Defaults
    /// to <see cref="F:Legate.SessionOutcomeMode.None" />.
    member val Outcome: SessionOutcomeMode = SessionOutcomeMode.None with get, set

    /// The permission policy the session's tool calls are evaluated
    /// against, or null to use the runtime default. The policy contract is
    /// <see cref="T:Legate.IPermissionPolicy" />.
    member val Permissions: IPermissionPolicy | null = null with get, set

    /// The ask_user headless policy the session's questions are answered
    /// against, or null to use the runtime default from configuration.
    /// The policy contract is <see cref="T:Legate.AskUserOptions" />.
    member val AskUser: AskUserOptions | null = null with get, set

    /// The sink notified when a headless session completes, or null when
    /// the host observes completion another way. The sink contract is
    /// <see cref="T:Legate.ISessionCompletionSink" />.
    member val CompletionSink: ISessionCompletionSink | null = null with get, set

    /// The most model iterations a single turn may spend. 0 means the
    /// runtime default from configuration, never an unbounded turn.
    member val MaxIterations: int = 0 with get, set

    /// The most wall-clock time a single turn may spend, or empty (HasValue
    /// is false) meaning the runtime default from configuration.
    member val Timeout: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

    /// Host metadata carried with the session (for example a correlation
    /// id), or null when the session has none. Never logged.
    member val Metadata: IReadOnlyDictionary<string, string> | null = null with get, set

    /// Host instruction file paths appended to every turn's composed
    /// system prompt (issue 66), or null when the session adds none. The
    /// paths are a per-session snapshot taken when the session opens (see
    /// <see cref="P:Legate.Session.Options" />): hosts mutate this list
    /// before opening. Files read in the listed order and land after the
    /// package instructions, the agent prompt, and the skills block; each
    /// missing, unreadable, blank, or over-bound file is skipped, so one
    /// bad path never fails a turn.
    member val HostInstructionFiles: IReadOnlyList<string> | null = null with get, set

/// The durable state of one conversation with one agent: what the runtime
/// journals and what the store epic persists. Options is the snapshot taken
/// when the session opened, never a live view of host configuration; hosts
/// mutate <see cref="T:Legate.SessionOptions" /> before opening. Title is
/// never null: it is the host's title, a runtime auto-title, or the empty
/// string (auto-titles are the runtime's concern). WorkspaceBinding is an
/// opaque reference the host uses to locate the session's workspace; the
/// runtime never interprets it, and the workspace contract issue defines
/// the shape it points at. PermissionGrants names the tools the host already
/// allowed for the whole session (an AllowForSession decision per tool
/// name): the runtime consults it before evaluating the policy again, the
/// store persists it with the session so grants survive a restart, and
/// closing the session evicts it. Constructible from C# through property
/// setters and serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type Session =
    {
        /// The session's durable identifier.
        Id: SessionId
        /// The tenant the session belongs to; hosts supply it through
        /// <see cref="M:Legate.TenantId.Create(System.String)" /> or
        /// <see cref="F:Legate.TenantId.Default" />.
        Tenant: TenantId
        /// The agent the session converses with.
        AgentId: AgentId
        /// The session's display title as opened; never null.
        Title: string
        /// The session's current lifecycle state.
        State: SessionState
        /// The turn currently running or suspended in the session, or empty
        /// (HasValue is false) when no turn is in flight.
        CurrentTurnId: Nullable<TurnId>
        /// When the session was created.
        CreatedAt: DateTimeOffset
        /// When the session's state last changed.
        UpdatedAt: DateTimeOffset
        /// When the session closed, or empty while it is open.
        ClosedAt: Nullable<DateTimeOffset>
        /// An opaque reference to the session's workspace binding, or null
        /// when the session has none. The runtime never interprets it.
        WorkspaceBinding: string | null
        /// The options the session was opened with: the snapshot taken at
        /// open, not a live view.
        Options: SessionOptions
        /// The tool names the host already allowed for the whole session,
        /// one entry per AllowForSession decision. Empty when nothing is
        /// granted yet; never null on a stored row (a null deserialised
        /// value reads as empty). Evicted when the session closes.
        PermissionGrants: IReadOnlyList<string>
    }
