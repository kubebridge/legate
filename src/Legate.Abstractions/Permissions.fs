// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System.Text.Json.Serialization

// Permission contracts. Every tool call is gated by an IPermissionPolicy:
// the runtime builds a PermissionRequest for the call and passes it to the
// policy's Evaluate, whose verdict decides whether the call executes,
// never runs, or suspends the turn awaiting a host reply. All types
// serialise with System.Text.Json; the verdict hierarchy carries stable
// $type discriminators like Reply and TurnOutcome. Public signatures stay
// BCL-only (no option, list, or DU).

/// What the runtime passes to
/// <see cref="M:Legate.IPermissionPolicy.Evaluate(Legate.PermissionRequest)" />
/// for every tool call it is about to make: which session and turn want the
/// call, which tool, where the tool came from, a bounded redacted preview of
/// the arguments, and a stable request id. The argument preview is bounded
/// and redacted: the runtime truncates it to a fixed maximum and strips
/// secret-looking values, so it is safe to journal, log, and surface to a
/// host UI; policies must never rely on it being the full arguments.
/// Serialises with System.Text.Json; identifiers serialise as plain strings.
[<CLIMutable; NoComparison>]
type PermissionRequest =
    {
        /// The session the tool call belongs to.
        SessionId: SessionId
        /// The turn the tool call belongs to.
        TurnId: TurnId
        /// The name the model called the tool by (the permission key, per
        /// the tool-name rules).
        ToolName: string
        /// The tool source the tool came from, or null when the runtime
        /// does not attribute the tool to a source.
        ToolSourceId: string | null
        /// A bounded, redacted preview of the call arguments: truncated to
        /// a fixed maximum and stripped of secret-looking values, safe for
        /// journaling and host display. Never the full arguments.
        ArgumentPreview: string | null
        /// The stable identifier of this request: the id a
        /// <see cref="T:Legate.Reply" /> carrying a
        /// <see cref="T:Legate.PermissionDecision" /> answers with.
        RequestId: string
    }

/// What a permission policy decided about one tool call. Serialises
/// polymorphically: every concrete verdict carries a stable <c>$type</c>
/// discriminator on the wire, mirroring <see cref="T:Legate.Reply" /> and
/// <see cref="T:Legate.TurnOutcome" />. The static conveniences
/// <see cref="M:Legate.PermissionVerdict.Allow" />,
/// <see cref="M:Legate.PermissionVerdict.Deny(System.String)" />, and
/// <see cref="M:Legate.PermissionVerdict.Ask" /> build verdicts without
/// naming the sealed subtypes; Deny requires a reason, so there is no
/// reasonless deny.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<AllowVerdict>, "allow")>]
[<JsonDerivedType(typeof<DenyVerdict>, "deny")>]
[<JsonDerivedType(typeof<AskVerdict>, "ask")>]
type PermissionVerdict() =

    /// The verdict that allows the call.
    /// <returns>A fresh <see cref="T:Legate.AllowVerdict" />.</returns>
    static member Allow: PermissionVerdict = AllowVerdict() :> PermissionVerdict

    /// The verdict that denies the call.
    /// <param name="reason">Why the call was denied. Never contains secrets or tool arguments (the exception-message rule).</param>
    /// <returns>A fresh <see cref="T:Legate.DenyVerdict" /> carrying the reason.</returns>
    static member Deny(reason: string) : PermissionVerdict =
        DenyVerdict(reason) :> PermissionVerdict

    /// The verdict that asks the host: the turn suspends until the host
    /// replies.
    /// <returns>A fresh <see cref="T:Legate.AskVerdict" />.</returns>
    static member Ask: PermissionVerdict = AskVerdict() :> PermissionVerdict

/// The verdict that allows the call to execute.
and [<Sealed>] AllowVerdict() =
    inherit PermissionVerdict()

/// The verdict that the call must not execute.
/// <param name="reason">Why the call was denied. Never contains secrets or tool arguments (the exception-message rule).</param>
and [<Sealed>] DenyVerdict(reason: string) =
    inherit PermissionVerdict()

    /// Why the call was denied. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The verdict that the policy cannot decide alone: the turn suspends
/// awaiting a host reply.
and [<Sealed>] AskVerdict() =
    inherit PermissionVerdict()

/// How the runtime gates every tool call: hosts implement Evaluate and the
/// runtime calls it inline, per tool call, before the call executes. The
/// contract pins three rules:
/// <list type="bullet">
/// <item><description><b>Synchronous.</b> Evaluate returns the verdict for
/// a request as a plain value, never a task: policies are in-memory host
/// rules the runtime evaluates inline per tool call, and an asynchronous
/// policy would only invite blocking inside Evaluate.</description></item>
/// <item><description><b>Verdicts.</b> Evaluate returns
/// <see cref="T:Legate.AllowVerdict" /> to let the call execute,
/// <see cref="T:Legate.DenyVerdict" /> to block it (with a reason; never
/// secrets or tool arguments), or <see cref="T:Legate.AskVerdict" /> to
/// suspend the turn until the host answers with a
/// <see cref="T:Legate.PermissionDecision" /> carrying the request's
/// <see cref="P:Legate.PermissionRequest.RequestId" />.</description></item>
/// <item><description><b>AllowForSession memory.</b> A host decision of
/// <see cref="F:Legate.PermissionDecisionKind.AllowForSession" /> is
/// remembered by the runtime, per session and tool name, never by the
/// policy: the runtime consults its remembered decisions before calling
/// Evaluate again for a session-tool pair it has already allowed, so a
/// policy stays free to keep returning
/// <see cref="T:Legate.AskVerdict" />.</description></item>
/// </list>
/// Built-in policies (AllowAll, AskForWritesAndExec, rule-based) are
/// runtime-epic deliverables, not part of this contract.
type IPermissionPolicy =

    /// Decides what happens to one tool call.
    /// <param name="request">What the runtime is about to do, with a bounded redacted argument preview.</param>
    /// <returns>The verdict for the request: the call executes, must not execute, or the turn suspends awaiting a host reply.</returns>
    abstract Evaluate: request: PermissionRequest -> PermissionVerdict
