// SPDX-License-Identifier: Apache-2.0
module Dot.DotMarkdown

open System
open System.Text.RegularExpressions

let blue = "38;2;8;102;255"
let softBlue = "38;2;112;172;255"
let textColor = "38;2;218;225;235"

let paint enabled code text =
    if enabled then $"\u001b[{code}m{text}\u001b[0m" else text

let private inlinePlain text =
    Regex.Replace(text, @"\[([^\]]+)\]\(([^)]+)\)", "$1 ($2)")
    |> fun text -> Regex.Replace(text, @"(\*\*|__)(.+?)\1", "$2")
    |> fun text -> Regex.Replace(text, @"`([^`]+)`", "$1")
    |> fun text -> Regex.Replace(text, @"(?<!\*)\*([^*]+)\*(?!\*)", "$1")
    |> fun text -> Regex.Replace(text, @"~~(.+?)~~", "$1")

let private inlineStyled enabled (text: string) =
    if not enabled then
        inlinePlain text
    else
        Regex.Replace(
            text,
            @"\[([^\]]+)\]\(([^)]+)\)",
            fun m -> paint true ("4;" + softBlue) ($"{m.Groups[1].Value} ({m.Groups[2].Value})")
        )
        |> fun text -> Regex.Replace(text, @"(\*\*|__)(.+?)\1", fun m -> paint true "1" m.Groups[2].Value)
        |> fun text ->
            Regex.Replace(text, @"`([^`]+)`", fun m -> paint true "48;2;20;36;64;38;2;151;195;255" m.Groups[1].Value)
        |> fun text -> Regex.Replace(text, @"(?<!\*)\*([^*]+)\*(?!\*)", fun m -> paint true "3" m.Groups[1].Value)
        |> fun text -> Regex.Replace(text, @"~~(.+?)~~", fun m -> paint true "9" m.Groups[1].Value)

let private cells (line: string) =
    Regex.Split(line.Trim().Trim('|'), @"(?<!\\)\|")
    |> Array.map (fun value -> value.Trim().Replace("\\|", "|"))

let private separator (line: string) =
    let values = cells line

    values.Length > 0
    && values |> Array.forall (fun value -> Regex.IsMatch(value, @"^:?-{3,}:?$"))

/// Width-aware Markdown rendering, including tables that reflow into labeled
/// rows on narrow terminals. Input is sanitized before markup is interpreted.
let render
    columns
    enabled
    (sanitize: string -> string)
    (wrap: int -> string -> string list)
    (text: string)
    : string list =
    let columns = max 1 columns

    let source =
        text.Replace("\r\n", "\n").Replace('\r', '\n').Replace("\t", "    ").Split('\n')
        |> Array.map sanitize

    let rows = ResizeArray<string>()
    let mutable index = 0
    let mutable codeFence = false

    let addWrapped code text =
        for line in wrap columns text do
            rows.Add(paint enabled code line)

    while index < source.Length do
        let line = source[index]
        let trimmed = line.TrimStart()

        if trimmed.StartsWith("```") || trimmed.StartsWith("~~~") then
            codeFence <- not codeFence

            if codeFence then
                let language = trimmed.Substring(3).Trim()
                addWrapped softBlue (if language = "" then "┌─ code" else "┌─ " + language)
            else
                addWrapped softBlue "└─"

            index <- index + 1
        elif codeFence then
            for chunk in wrap (max 1 (columns - 2)) (line.Replace("\t", "    ")) do
                rows.Add(
                    paint
                        enabled
                        "48;2;14;26;46;38;2;192;216;250"
                        (("  " + chunk).Substring(0, min columns (chunk.Length + 2)))
                )

            index <- index + 1
        elif index + 1 < source.Length && line.Contains('|') && separator source[index + 1] then
            let headers = cells line |> Array.map inlinePlain
            let alignments = cells source[index + 1]
            let data = ResizeArray<string array>()
            index <- index + 2

            while index < source.Length
                  && source[index].Contains('|')
                  && source[index].Trim() <> "" do
                data.Add(cells source[index] |> Array.map inlinePlain)
                index <- index + 1

            let count = headers.Length

            let widths =
                Array.init count (fun col ->
                    seq {
                        yield headers[col].Length

                        for row in data do
                            if col < row.Length then
                                yield row[col].Length
                    }
                    |> Seq.max
                    |> max 3
                    |> min 40)

            let overhead = 3 * count + 1

            if columns < overhead + 3 * count then
                for row in data do
                    for col in 0 .. count - 1 do
                        let value = if col < row.Length then row[col] else ""
                        addWrapped textColor ($"{headers[col]}: {value}")

                    rows.Add ""
            else
                while Array.sum widths + overhead > columns do
                    let widest = widths |> Array.mapi (fun i size -> i, size) |> Array.maxBy snd |> fst
                    widths[widest] <- widths[widest] - 1

                let rule (left: string) (middle: string) (right: string) =
                    left
                    + String.Join(middle, widths |> Array.map (fun width -> String('─', width + 2)))
                    + right

                rows.Add(paint enabled blue (rule "┌" "┬" "┐"))

                let emit header (values: string array) =
                    let parts =
                        Array.init count (fun col ->
                            wrap widths[col] (if col < values.Length then values[col] else "")
                            |> List.toArray)

                    let height = parts |> Array.map Array.length |> Array.max

                    for n in 0 .. height - 1 do
                        let content =
                            Array.init count (fun col ->
                                let value = if n < parts[col].Length then parts[col][n] else ""
                                let alignment = if col < alignments.Length then alignments[col] else ""

                                let value =
                                    if alignment.StartsWith(":") && alignment.EndsWith(":") then
                                        let left = (widths[col] - value.Length) / 2
                                        (String(' ', left) + value).PadRight widths[col]
                                    elif alignment.EndsWith(":") then
                                        value.PadLeft widths[col]
                                    else
                                        value.PadRight widths[col]

                                " " + value + " ")

                        rows.Add(
                            paint
                                enabled
                                (if header then "1;" + softBlue else textColor)
                                ("│" + String.Join("│", content) + "│")
                        )

                emit true headers
                rows.Add(paint enabled blue (rule "├" "┼" "┤"))

                for row in data do
                    emit false row

                rows.Add(paint enabled blue (rule "└" "┴" "┘"))
        elif Regex.IsMatch(trimmed, @"^#{1,6}\s+") then
            addWrapped ("1;" + softBlue) (Regex.Replace(trimmed, @"^#{1,6}\s+", "") |> inlinePlain)
            index <- index + 1
        elif Regex.IsMatch(trimmed, @"^([-*_])(?:\s*\1){2,}\s*$") then
            rows.Add(paint enabled blue (String('─', columns)))
            index <- index + 1
        else
            let display =
                if trimmed.StartsWith("> ") then
                    "│ " + trimmed.Substring(2)
                elif Regex.IsMatch(trimmed, @"^[-*+]\s") then
                    "• " + trimmed.Substring(2)
                else
                    line
            // Style unwrapped inline spans; wrapped paragraphs still remove
            // markup and use the normal body color, never split ANSI escapes.
            if (inlinePlain display).Length <= columns then
                rows.Add(inlineStyled enabled display)
            else
                addWrapped textColor (inlinePlain display)

            index <- index + 1

    List.ofSeq rows
