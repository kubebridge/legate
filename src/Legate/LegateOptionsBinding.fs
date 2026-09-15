// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open Microsoft.Extensions.Configuration

// Internal binding helper for LegateOptions. Standard ConfigurationBinder
// handles scalars, nested sections, lists, and dictionaries; this module
// adds a whitelisted duration pre-pass (30s/15m/1h/500ms/30d forms with a
// TimeSpan.Parse fallback) and explicit raw-string enum parsing so unknown
// values become precise violations instead of silent zeros, ending in a
// composite Validate whose non-null result throws with the section path.
// No AddLegate wiring: UseConfiguration and DI registration belong to 28.
module internal LegateOptionsBinding =

    // Relative paths (from the Legate section) of every TimeSpan property in
    // the options graph. Whitelisted explicitly so the pass never wanders
    // into unrelated types; the options graph has no cycles.
    let private durationPaths =
        [
            "Sessions:LeaseDuration"
            "Sessions:LeaseRenewalInterval"
            "Turns:DefaultTimeout"
            "Permissions:AskTimeout"
            "Llm:Coordination:MinRetryBackoff"
            "Llm:Coordination:MaxRetryBackoff"
            "Llm:Coordination:RateLimitCooldown"
            "Workspace:IdleTeardownAfter"
            "Completion:RetryDelay"
            "Cluster:ShutdownGraceSeconds"
        ]

    // Relative paths of every flat-choice enum with its type and the phrase
    // the precise violation uses. DeliveryMode and PermissionDecisionKind are
    // reused from the contracts; the rest are defined in LegateOptions.fs.
    let private enumPaths: (string * Type * string * string list) list =
        [
            "Turns:DefaultDelivery", typeof<DeliveryMode>, "delivery mode", [ "Queue"; "Inject"; "Interrupt" ]
            "Turns:CrashResume", typeof<TurnCrashResume>, "crash resume", [ "Fail"; "RetryTurn" ]
            "Permissions:DefaultDecision",
            typeof<PermissionDecisionKind>,
            "permission decision",
            [
                "AllowOnce"
                "AllowForSession"
                "Deny"
            ]
            "Cluster:Mode", typeof<ClusterMode>, "cluster mode", [ "Local"; "Clustered" ]
            "Workspace:Mode", typeof<WorkspaceMode>, "workspace mode", [ "Process"; "HostDirectory"; "Docker" ]
            "AskUser:Mode", typeof<AskUserMode>, "ask-user mode", [ "Fail"; "AnswerWith" ]
        ]

    // Parses a duration from the whitelisted forms (500ms, 30s, 15m, 1h,
    // 30d) with a TimeSpan.Parse fallback for 00:00:30-style values.
    let private tryParseDuration (raw: string) : TimeSpan option =
        let text = raw.Trim()

        if String.IsNullOrEmpty text then
            None
        else
            let lower = text.ToLowerInvariant()

            let trySuffix (suffix: string) (factory: float -> TimeSpan) : TimeSpan option =
                if lower.EndsWith(suffix, StringComparison.Ordinal) then
                    let number = text.Substring(0, text.Length - suffix.Length).Trim()
                    let mutable value = 0.0

                    if
                        Double.TryParse(
                            number,
                            Globalization.NumberStyles.Float,
                            Globalization.CultureInfo.InvariantCulture,
                            &value
                        )
                    then
                        try
                            Some(factory value)
                        with
                        | :? OverflowException -> None
                        | :? ArgumentException -> None
                    else
                        None
                else
                    None

            // Longest suffix first so ms never reads as minutes.
            match trySuffix "ms" TimeSpan.FromMilliseconds with
            | Some span -> Some span
            | None ->
                match trySuffix "s" TimeSpan.FromSeconds with
                | Some span -> Some span
                | None ->
                    match trySuffix "m" TimeSpan.FromMinutes with
                    | Some span -> Some span
                    | None ->
                        match trySuffix "h" TimeSpan.FromHours with
                        | Some span -> Some span
                        | None ->
                            match trySuffix "d" TimeSpan.FromDays with
                            | Some span -> Some span
                            | None ->
                                let mutable fallback = TimeSpan.Zero

                                if TimeSpan.TryParse(text, Globalization.CultureInfo.InvariantCulture, &fallback) then
                                    Some fallback
                                else
                                    None

    // Explicit raw-string enum parsing: case-insensitive names plus defined
    // numeric values; anything else is a precise violation, never a silent
    // zero.
    let private tryParseEnum (enumType: Type) (raw: string) : obj option =
        let text = raw.Trim()
        let mutable parsed = Unchecked.defaultof<obj>

        if
            Enum.TryParse(enumType, text, true, &parsed)
            && not (isNull (box parsed))
            && Enum.IsDefined(enumType, parsed)
        then
            Some parsed
        else
            None

    let private fullPath (sectionPath: string) (relative: string) =
        if String.IsNullOrEmpty sectionPath then
            relative
        else
            sectionPath + ConfigurationPath.KeyDelimiter + relative

    // Throws a precise violation for one raw enum value.
    let private rejectEnum (path: string) (kind: string) (expected: string list) (raw: string) : 'T =
        let names = String.Join(", ", expected)

        raise (
            InvalidOperationException(
                $"Invalid Legate configuration at '%s{path}': unknown %s{kind} '%s{raw.Trim()}'. Expected one of: %s{names}."
            )
        )

    // Pre-validates every flat-choice enum from its raw string so unknown
    // values fail with a precise violation before the binder runs.
    let private validateEnums (section: IConfigurationSection) =
        for relative, enumType, kind, expected in enumPaths do
            match Option.ofObj (section.GetSection(relative).Value) with
            | Some raw when not (String.IsNullOrWhiteSpace raw) ->
                match tryParseEnum enumType raw with
                | Some _ -> ()
                | None -> rejectEnum (fullPath section.Path relative) kind expected raw |> ignore
            | _ -> ()

        // Rules-list decisions carry indices, so they are validated
        // separately from the fixed enum paths above.
        let rules = section.GetSection("Permissions:Rules").GetChildren()

        for rule in rules do
            match Option.ofObj (rule.GetSection("Decision").Value) with
            | Some raw when not (String.IsNullOrWhiteSpace raw) ->
                match tryParseEnum typeof<PermissionDecisionKind> raw with
                | Some _ -> ()
                | None ->
                    rejectEnum
                        (fullPath section.Path ($"Permissions:Rules:{rule.Key}:Decision"))
                        "permission decision"
                        [
                            "AllowOnce"
                            "AllowForSession"
                            "Deny"
                        ]
                        raw
                    |> ignore
            | _ -> ()

    // Pre-parses every duration from its raw string; unknown forms fail with
    // the section path before the binder runs.
    let private parseDurations (section: IConfigurationSection) : Map<string, string> =
        (Map.empty, durationPaths)
        ||> List.fold (fun acc relative ->
            match Option.ofObj (section.GetSection(relative).Value) with
            | Some raw when not (String.IsNullOrWhiteSpace raw) ->
                match tryParseDuration raw with
                | Some span -> Map.add (fullPath section.Path relative) (span.ToString("c")) acc
                | None ->
                    raise (
                        InvalidOperationException(
                            $"Invalid Legate configuration at '%s{fullPath section.Path relative}': unknown duration '%s{raw.Trim()}'. Expected a 30s, 15m, 1h, 500ms, or 30d form, or a TimeSpan such as 00:00:30."
                        )
                    )
            | _ -> acc)

    // Binds the section onto a fresh root. Durations are patched to their
    // canonical TimeSpan form in a copied in-memory section first, because
    // the binder itself only understands TimeSpan.Parse; everything else
    // binds through ConfigurationBinder for env-var, nested, list, and
    // dictionary support.
    let bind (section: IConfigurationSection) : LegateOptions =
        ArgumentNullException.ThrowIfNull(section)
        validateEnums section
        let durations = parseDurations section

        let pairs =
            section.AsEnumerable()
            |> Seq.choose (fun entry ->
                match Option.ofObj entry.Value with
                | None -> None
                | Some v ->
                    let replacement =
                        durations
                        |> Map.tryPick (fun full canonical ->
                            if String.Equals(full, entry.Key, StringComparison.OrdinalIgnoreCase) then
                                Some canonical
                            else
                                None)

                    Some(KeyValuePair<string, string>(entry.Key, defaultArg replacement v)))
            |> Seq.toArray

        let patched = ConfigurationBuilder().AddInMemoryCollection(pairs).Build()

        let options = LegateOptions()

        if String.IsNullOrEmpty section.Path then
            patched.Bind(options)
        else
            patched.GetSection(section.Path).Bind(options)

        match options.Validate() with
        | null -> options
        | violation ->
            raise (InvalidOperationException($"Invalid Legate configuration at '%s{section.Path}': %s{violation}"))
