// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open Legate

// Settings for the Google Gemini provider, bound from the
// Legate:Llm:Providers:Google configuration section. Extends the shared
// LlmProviderOptions (which carries the API key) with the model the provider
// serves by default and the per-call timeout the SDK's HTTP client enforces.
// A plain class with mutable properties and defaults, so hosts can set
// properties before registering and the configuration binder only overrides
// the keys the host sets.
type GoogleLlmOptions() =
    inherit LlmProviderOptions()

    /// The default model the provider serves, used when a host registers the
    /// provider without naming a model. Current stable Gemini Flash at the
    /// time of release; <c>gemini-2.5-flash</c> is excluded (legacy-gated).
    static member DefaultModel = "gemini-3.8-flash"

    /// The configuration section the provider binds from:
    /// <c>Legate:Llm:Providers:Google</c>, converging with the sibling
    /// provider packages (<c>Legate:Llm:Providers:&lt;Name&gt;</c>).
    static member ConfigurationSectionPath = "Legate:Llm:Providers:Google"

    /// The model the provider serves by default. Defaults to
    /// <see cref="P:Legate.Llm.GoogleLlmOptions.DefaultModel" />; a blank
    /// value falls back to that constant when the provider builds clients.
    member val Model: string = GoogleLlmOptions.DefaultModel with get, set

    /// How long one provider call may run before the SDK's HTTP client
    /// cancels it. Defaults to 100 seconds; must stay positive (the provider
    /// rejects non-positive values when it builds a client). The
    /// coordinator's own deadline still bounds the call end to end.
    member val Timeout: TimeSpan = TimeSpan.FromSeconds 100.0 with get, set
