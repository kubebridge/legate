// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open Legate
open Microsoft.Extensions.AI

// Settings for the native Anthropic provider, bound from the
// Legate:Llm:Providers:anthropic configuration section. Extends the shared
// LlmProviderOptions (which carries the API key) with the model the provider
// serves by default, the per-call timeout the SDK enforces, the thinking
// mode the SDK adapter sends, and the default cap on output tokens. A plain
// class with mutable properties and defaults, so hosts can set properties
// before registering and the configuration binder only overrides the keys
// the host sets. Prompt-cache breakpoints are caller-applied through the
// SDK's own cache-control extensions and pass through the adapter untouched;
// there is no provider-level auto-stamping.
type AnthropicLlmOptions() =
    inherit LlmProviderOptions()

    /// The default model the provider serves, used when a host registers the
    /// provider without naming a model. Current Sonnet at the time of
    /// release; hosts override it with <c>Model</c> for pinned versions.
    static member DefaultModel = "claude-sonnet-5"

    /// The configuration section the provider binds from:
    /// <c>Legate:Llm:Providers:anthropic</c>, converging with the sibling
    /// provider packages (<c>Legate:Llm:Providers:&lt;Name&gt;</c>).
    static member ConfigurationSectionPath = "Legate:Llm:Providers:anthropic"

    /// The model the provider serves by default. Defaults to
    /// <see cref="P:Legate.Llm.AnthropicLlmOptions.DefaultModel" />; must
    /// stay a non-empty string (the provider rejects blanks when it builds
    /// clients).
    member val Model: string = AnthropicLlmOptions.DefaultModel with get, set

    /// How long one provider call may run before the SDK cancels it.
    /// Defaults to 100 seconds; must stay positive (the provider rejects
    /// non-positive values when it builds a client). The coordinator's own
    /// deadline still bounds the call end to end.
    member val Timeout: TimeSpan = TimeSpan.FromSeconds 100.0 with get, set

    /// Which thinking mode the SDK adapter sends: adaptive for current
    /// models, extended for models that predate adaptive thinking. Defaults
    /// to adaptive. Per-call effort still travels on the chat options; an
    /// unset effort thinks at the model default, and no effort disables
    /// thinking.
    member val ThinkingMode: AnthropicThinkingMode = AnthropicThinkingMode.Adaptive with get, set

    /// The default cap on output tokens the adapter sends, or null for the
    /// adapter default. Must stay positive when set.
    member val MaxOutputTokens: Nullable<int> = Nullable() with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.Model then
            "Model must be a non-empty string."
        elif this.Timeout <= TimeSpan.Zero then
            "Timeout must be positive."
        elif not (Enum.IsDefined(typeof<AnthropicThinkingMode>, this.ThinkingMode)) then
            "ThinkingMode must be a defined AnthropicThinkingMode value."
        elif this.MaxOutputTokens.HasValue && this.MaxOutputTokens.Value <= 0 then
            "MaxOutputTokens must be positive when set."
        else
            null
