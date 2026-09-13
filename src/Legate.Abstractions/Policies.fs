// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System.Text.Json.Serialization

// Host policy contracts: the three hooks that let a host veto work and
// observe usage. An ISessionAdmissionPolicy decides whether the runtime may
// open a session or accept a prompt; a rejection surfaces as
// AdmissionRejectedException. An IUsageObserver receives usage checkpoints
// during a turn and the settled usage when the turn attempt ends. An
// IModelPolicy authorises the tenant-provider-model call before every
// provider call. Usage never carries cost: the runtime reports token counts
// only and pricing is a host concern, computed outside the runtime. All
// types serialise with System.Text.Json; the decision hierarchies carry
// stable $type discriminators like PermissionVerdict and TurnOutcome.
// Public signatures stay BCL-only (no option, list, or DU).

/// When the runtime consults admission: once when a session opens and again
/// before every prompt the session accepts.
type SessionAdmissionPhase =

    /// Admission for opening a session. Transitions: the phase the runtime
    /// passes before <see cref="F:Legate.SessionAdmissionPhase.Prompt" />.
    | Open = 0

    /// Admission for one prompt entering an open session. Transitions: from
    /// <see cref="F:Legate.SessionAdmissionPhase.Open" /> once the session
    /// exists; consulted for every prompt, including prompts that join the
    /// inbox while a turn is suspended.
    | Prompt = 1

/// What the runtime passes to
/// <see cref="M:Legate.ISessionAdmissionPolicy.Authorize(Legate.SessionAdmissionContext)" />
/// when it is about to open a session or accept a prompt: which tenant asks,
/// which session and agent are involved, and in which phase. Serialises with
/// System.Text.Json; identifiers serialise as plain strings.
[<CLIMutable; NoComparison>]
type SessionAdmissionContext =
    {
        /// The tenant requesting admission.
        Tenant: TenantId
        /// The session admission is requested for.
        SessionId: SessionId
        /// The agent the session converses with.
        AgentId: AgentId
        /// The phase the runtime is admitting: session open or prompt.
        Phase: SessionAdmissionPhase
    }

/// What an admission policy decided about one session open or one prompt.
/// Serialises polymorphically: every concrete decision carries a stable
/// <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.PermissionVerdict" /> and
/// <see cref="T:Legate.TurnOutcome" />. The static conveniences
/// <see cref="P:Legate.SessionAdmissionDecision.Allow" /> and
/// <see cref="M:Legate.SessionAdmissionDecision.Reject(System.String)" />
/// build decisions without naming the sealed subtypes. The rejection reason
/// is host-defined: hosts own their business rules, so the contract pins no
/// fixed reason taxonomy.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<AdmissionAllowed>, "allowed")>]
[<JsonDerivedType(typeof<AdmissionRejected>, "rejected")>]
type SessionAdmissionDecision() =

    /// The decision that admits the request.
    /// <returns>A fresh <see cref="T:Legate.AdmissionAllowed" />.</returns>
    static member Allow: SessionAdmissionDecision = AdmissionAllowed() :> SessionAdmissionDecision

    /// The decision that rejects the request.
    /// <param name="reason">Why admission was rejected. Host-defined: hosts own their business rules, so there is no fixed reason taxonomy. Never contains secrets (the exception-message rule).</param>
    /// <returns>A fresh <see cref="T:Legate.AdmissionRejected" /> carrying the reason.</returns>
    static member Reject(reason: string) : SessionAdmissionDecision =
        AdmissionRejected(reason) :> SessionAdmissionDecision

/// The decision that admits the session open or prompt.
and [<Sealed>] AdmissionAllowed() =
    inherit SessionAdmissionDecision()

/// The decision that rejects the session open or prompt: the runtime
/// surfaces <see cref="P:Legate.AdmissionRejected.Reason" /> as
/// <see cref="T:Legate.AdmissionRejectedException" />, failing the open or
/// prompt that asked for admission.
/// <param name="reason">Why admission was rejected. Host-defined: hosts own their business rules, so there is no fixed reason taxonomy. Never contains secrets.</param>
and [<Sealed>] AdmissionRejected(reason: string) =
    inherit SessionAdmissionDecision()

    /// Why admission was rejected. Host-defined: hosts own their business
    /// rules, so there is no fixed reason taxonomy. Never contains secrets.
    member _.Reason = reason

/// How the runtime gates session opens and prompts: hosts implement
/// Authorize and the runtime calls it inline, before the session opens or
/// the prompt is accepted. The contract pins two rules:
/// <list type="bullet">
/// <item><description><b>Synchronous.</b> Authorize returns the decision as
/// a plain value, never a task: policies are in-memory host rules the
/// runtime evaluates inline, and an asynchronous policy would only invite
/// blocking on the admission path.</description></item>
/// <item><description><b>Decisions.</b> Authorize returns
/// <see cref="T:Legate.AdmissionAllowed" /> to admit the open or prompt, or
/// <see cref="T:Legate.AdmissionRejected" /> to reject it; the rejection
/// reason surfaces as <see cref="T:Legate.AdmissionRejectedException" />.</description></item>
/// </list>
/// The runtime default when a host registers none is
/// <see cref="T:Legate.AllowAllAdmissionPolicy" />.
type ISessionAdmissionPolicy =

    /// Decides whether the runtime may open the session or accept the prompt.
    /// <param name="context">Who wants admission (tenant, session, agent) and in which phase.</param>
    /// <returns>The decision: admit, or reject with a host-defined reason.</returns>
    abstract Authorize: context: SessionAdmissionContext -> SessionAdmissionDecision

/// Token usage checkpointed during a turn attempt: who consumed it, where,
/// and how much. The token counts are cumulative for the turn under that
/// attempt, <see cref="T:Legate.UsageSummary" />-shaped. Usage never carries
/// cost: the runtime reports token counts only and pricing is a host
/// concern, computed from these counts outside the runtime. Serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type UsageCheckpoint =
    {
        /// The tenant the turn belongs to.
        Tenant: TenantId
        /// The session the turn runs in.
        SessionId: SessionId
        /// The turn the usage belongs to.
        TurnId: TurnId
        /// The 1-based attempt the usage was consumed under; incremented on
        /// resume.
        Attempt: int
        /// The provider id that served the calls, the provider segment of a
        /// <see cref="T:Legate.ModelReference" />, for example "anthropic".
        Provider: string
        /// The model id the calls named, the model segment of a
        /// <see cref="T:Legate.ModelReference" />, for example
        /// "claude-sonnet".
        Model: string
        /// Cumulative input tokens the turn consumed under this attempt.
        InputTokens: int64
        /// Cumulative output tokens the turn produced under this attempt.
        OutputTokens: int64
        /// The stable key the runtime assigns this delivery; observers
        /// deduplicate on it because delivery is at-least-once.
        IdempotencyKey: string
    }

/// The settled usage for one turn attempt, delivered to
/// <see cref="M:Legate.IUsageObserver.OnSettled(Legate.UsageSettlement)" />
/// after that attempt's checkpoints. The token counts are cumulative for the
/// whole attempt, <see cref="T:Legate.UsageSummary" />-shaped. Usage never
/// carries cost: the runtime reports token counts only and pricing is a host
/// concern. Serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type UsageSettlement =
    {
        /// The tenant the turn belongs to.
        Tenant: TenantId
        /// The session the turn runs in.
        SessionId: SessionId
        /// The turn the usage belongs to.
        TurnId: TurnId
        /// The 1-based attempt the usage was consumed under; incremented on
        /// resume.
        Attempt: int
        /// The provider id that served the calls, the provider segment of a
        /// <see cref="T:Legate.ModelReference" />, for example "anthropic".
        Provider: string
        /// The model id the calls named, the model segment of a
        /// <see cref="T:Legate.ModelReference" />, for example
        /// "claude-sonnet".
        Model: string
        /// Cumulative input tokens the attempt consumed.
        InputTokens: int64
        /// Cumulative output tokens the attempt produced.
        OutputTokens: int64
        /// The stable key the runtime assigns this delivery; observers
        /// deduplicate on it because delivery is at-least-once.
        IdempotencyKey: string
    }

/// How a host receives usage from the runtime: the runtime calls OnCheckpoint
/// while a turn runs and OnSettled when the turn attempt settles. The
/// contract pins four rules:
/// <list type="bullet">
/// <item><description><b>Synchronous and non-blocking.</b> Both methods
/// return unit and are called inline on the turn path, like
/// <see cref="T:Legate.IPermissionPolicy" />: observers hand the payload to
/// their own queue or sink and return immediately, never blocking or
/// waiting.</description></item>
/// <item><description><b>At-least-once delivery.</b> A checkpoint or
/// settlement may be delivered more than once, for example after a crash and
/// resume: observers deduplicate on the payload's
/// <see cref="P:Legate.UsageCheckpoint.IdempotencyKey" />.</description></item>
/// <item><description><b>Per-attempt ordering.</b> Within one turn and
/// attempt, checkpoints arrive in emission order and the settlement arrives
/// after that attempt's checkpoints.</description></item>
/// <item><description><b>No cross-session ordering.</b> Deliveries from
/// different sessions carry no ordering guarantee.</description></item>
/// </list>
/// There is no default observer: when a host registers none, the runtime
/// observes nothing.
type IUsageObserver =

    /// Receives one checkpointed usage delivery while the turn runs.
    /// <param name="usage">The cumulative usage so far for the turn under its current attempt.</param>
    abstract OnCheckpoint: usage: UsageCheckpoint -> unit

    /// Receives the settled usage for a turn attempt, after that attempt's
    /// checkpoints.
    /// <param name="usage">The cumulative usage the attempt consumed.</param>
    abstract OnSettled: usage: UsageSettlement -> unit

/// What a model policy decided about one tenant, provider, and model call.
/// Serialises polymorphically: every concrete decision carries a stable
/// <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.SessionAdmissionDecision" />. The static conveniences
/// <see cref="P:Legate.ModelPolicyDecision.Allow" /> and
/// <see cref="M:Legate.ModelPolicyDecision.Deny(System.String)" /> build
/// decisions without naming the sealed subtypes.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<ModelAllowed>, "allowed")>]
[<JsonDerivedType(typeof<ModelDenied>, "denied")>]
type ModelPolicyDecision() =

    /// The decision that allows the call.
    /// <returns>A fresh <see cref="T:Legate.ModelAllowed" />.</returns>
    static member Allow: ModelPolicyDecision = ModelAllowed() :> ModelPolicyDecision

    /// The decision that denies the call.
    /// <param name="message">Why the call was denied. Client-safe: the runtime surfaces this text to the caller, so it never contains secrets, keys, or internal topology (the exception-message rule).</param>
    /// <returns>A fresh <see cref="T:Legate.ModelDenied" /> carrying the message.</returns>
    static member Deny(message: string) : ModelPolicyDecision =
        ModelDenied(message) :> ModelPolicyDecision

/// The decision that allows the tenant-provider-model call.
and [<Sealed>] ModelAllowed() =
    inherit ModelPolicyDecision()

/// The decision that denies the tenant-provider-model call: the runtime
/// surfaces <see cref="P:Legate.ModelDenied.Message" /> to the caller in
/// place of the provider response.
/// <param name="message">Why the call was denied. Client-safe: the runtime surfaces this text to the caller, so it never contains secrets, keys, or internal topology.</param>
and [<Sealed>] ModelDenied(message: string) =
    inherit ModelPolicyDecision()

    /// Why the call was denied. Client-safe: the runtime surfaces this text
    /// to the caller, so it never contains secrets, keys, or internal
    /// topology.
    member _.Message = message

/// How the runtime authorises a model before every provider call: hosts
/// implement Authorize and the runtime calls it inline, before the call is
/// built. Provider and model are the same values a
/// <see cref="T:Legate.ModelReference" /> carries. The contract pins two
/// rules:
/// <list type="bullet">
/// <item><description><b>Synchronous.</b> Authorize returns the decision as
/// a plain value, never a task: policies are in-memory host rules the
/// runtime evaluates inline, and an asynchronous policy would only invite
/// blocking on the turn path.</description></item>
/// <item><description><b>Decisions.</b> Authorize returns
/// <see cref="T:Legate.ModelAllowed" /> to let the call proceed, or
/// <see cref="T:Legate.ModelDenied" /> to block it; the deny message is the
/// client-safe text that surfaces to the caller.</description></item>
/// </list>
/// The runtime default when a host registers none is
/// <see cref="T:Legate.AllowAllModelPolicy" />.
type IModelPolicy =

    /// Decides whether the tenant may call the model on the provider.
    /// <param name="tenant">The tenant making the call.</param>
    /// <param name="provider">The provider id the call resolves to, the provider segment of a <see cref="T:Legate.ModelReference" />, for example "anthropic".</param>
    /// <param name="model">The model id the call names, the model segment of a <see cref="T:Legate.ModelReference" />, for example "claude-sonnet".</param>
    /// <returns>The decision: allow, or deny with a client-safe message.</returns>
    abstract Authorize: tenant: TenantId * provider: string * model: string -> ModelPolicyDecision

/// The admission default when a host registers no
/// <see cref="T:Legate.ISessionAdmissionPolicy" />: every session open and
/// every prompt is admitted. Sealed so the default cannot drift; register a
/// real policy to veto.
[<Sealed>]
type AllowAllAdmissionPolicy() =

    /// Admits every request.
    interface ISessionAdmissionPolicy with
        member _.Authorize(_context: SessionAdmissionContext) = SessionAdmissionDecision.Allow

/// The model-policy default when a host registers no
/// <see cref="T:Legate.IModelPolicy" />: every tenant-provider-model call is
/// allowed. Sealed so the default cannot drift; register a real policy to
/// veto.
[<Sealed>]
type AllowAllModelPolicy() =

    /// Allows every call.
    interface IModelPolicy with
        member _.Authorize(_tenant: TenantId, _provider: string, _model: string) = ModelPolicyDecision.Allow
