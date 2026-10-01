// SPDX-License-Identifier: Apache-2.0
module Dot.DotInput

open System

// Multiline input box for the dot TUI (issue 332): the pure editor model
// (multiline buffer plus cursor), the ConsoleKeyInfo decoder for the
// documented key map, session-only in-memory history, slash-command hints,
// the width-clamped input-region renderer, and the intent classification
// that emits exactly the strings ReplEngine.HandleLineAsync already routes.
// Stdlib-only and TTY-free: every function is pure over its inputs, so
// multiline editing, paste handling, history browsing, hints, and the key
// map are unit-tested without a terminal. DotTui owns the key pump and
// routes each intent through the engine; the plain REPL never loads this
// module and keeps its behavior byte-identically.

// ──────────────────────────────────────────────────────────────────────────
// Editor model

/// One input buffer: the lines (never empty) plus the cursor position.
/// Column clamps to the current line length on every operation.
type EditorState =
    {
        /// The buffer lines, oldest first. Never empty.
        Lines: string list
        /// The zero-based cursor row.
        Row: int
        /// The zero-based cursor column within the current line.
        Col: int
    }

/// The empty buffer: one empty line with the cursor at its start.
let empty: EditorState = { Lines = [ "" ]; Row = 0; Col = 0 }

/// Builds a buffer from pasted or restored text: carriage returns are
/// stripped, newlines split lines, and the cursor lands at the end.
/// <param name="text">The text to load. Null or empty loads the empty buffer.</param>
/// <returns>The buffer holding the text.</returns>
let fromText (text: string | null) : EditorState =
    let safe =
        match text with
        | null -> ""
        | content -> content.Replace("\r\n", "\n").Replace('\r', '\n')

    let lines = safe.Split('\n') |> List.ofArray
    let last = lines.Length - 1

    {
        Lines = lines
        Row = last
        Col = lines[last].Length
    }

/// Reads the buffer back as text with embedded newlines.
/// <param name="editor">The buffer to read.</param>
/// <returns>The buffer text.</returns>
let toText (editor: EditorState) : string = String.Join("\n", editor.Lines)

/// True when the buffer holds nothing submittable: empty or whitespace.
/// Empty input sends nothing.
/// <param name="editor">The buffer to check.</param>
/// <returns>True when the buffer is blank.</returns>
let isBlank (editor: EditorState) : bool =
    String.IsNullOrWhiteSpace(toText editor)

/// The cursor line the hints query reads: the line the cursor sits on.
/// <param name="editor">The buffer to read.</param>
/// <returns>The cursor line, or empty when the buffer is invalid.</returns>
let cursorLine (editor: EditorState) : string =
    if editor.Row >= 0 && editor.Row < editor.Lines.Length then
        editor.Lines[editor.Row]
    else
        ""

/// Inserts one character at the cursor.
/// <param name="editor">The buffer to edit.</param>
/// <param name="value">The character to insert.</param>
/// <returns>The edited buffer.</returns>
let insertChar (editor: EditorState) (value: char) : EditorState =
    let line = editor.Lines[editor.Row]
    let col = min editor.Col line.Length

    { editor with
        Lines =
            editor.Lines
            |> List.mapi (fun index content ->
                if index = editor.Row then
                    content.Insert(col, string value)
                else
                    content)
        Col = col + 1
    }

/// Inserts pasted text at the cursor: carriage returns are stripped and
/// embedded newlines split lines, so a 10KB single line stays one clamped
/// row and embedded newlines become buffer rows.
/// <param name="editor">The buffer to edit.</param>
/// <param name="text">The pasted text.</param>
/// <returns>The edited buffer.</returns>
let insertText (editor: EditorState) (text: string | null) : EditorState =
    match text with
    | null -> editor
    | pasted ->
        let mutable current = editor

        for value in pasted.Replace("\r\n", "\n").Replace('\r', '\n') do
            if value = '\n' then
                let line = current.Lines[current.Row]
                let col = min current.Col line.Length

                current <-
                    { current with
                        Lines =
                            current.Lines
                            |> List.mapi (fun index content ->
                                if index = current.Row then
                                    line.Substring(0, col)
                                else
                                    content)
                            |> List.insertAt (current.Row + 1) (line.Substring(col))
                        Row = current.Row + 1
                        Col = 0
                    }
            else
                current <- insertChar current value

        current

/// Splits the current line at the cursor, moving the tail to a new row.
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let insertNewline (editor: EditorState) : EditorState = insertText editor "\n"

/// Moves the cursor one column left, stopping at the buffer start.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let moveLeft (editor: EditorState) : EditorState =
    if editor.Col > 0 then
        { editor with Col = editor.Col - 1 }
    elif editor.Row > 0 then
        let previous = editor.Lines[editor.Row - 1]

        { editor with
            Row = editor.Row - 1
            Col = previous.Length
        }
    else
        editor

/// Moves the cursor one column right, stopping at the buffer end.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let moveRight (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]

    if editor.Col < line.Length then
        { editor with Col = editor.Col + 1 }
    elif editor.Row < editor.Lines.Length - 1 then
        { editor with
            Row = editor.Row + 1
            Col = 0
        }
    else
        editor

/// Moves the cursor one row up, clamping to the shorter line.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let moveUp (editor: EditorState) : EditorState =
    if editor.Row > 0 then
        let above = editor.Lines[editor.Row - 1]

        { editor with
            Row = editor.Row - 1
            Col = min editor.Col above.Length
        }
    else
        editor

/// Moves the cursor one row down, clamping to the shorter line.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let moveDown (editor: EditorState) : EditorState =
    if editor.Row < editor.Lines.Length - 1 then
        let below = editor.Lines[editor.Row + 1]

        { editor with
            Row = editor.Row + 1
            Col = min editor.Col below.Length
        }
    else
        editor

/// Moves the cursor to the start of the current line.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let lineStart (editor: EditorState) : EditorState = { editor with Col = 0 }

/// Moves the cursor to the end of the current line.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let lineEnd (editor: EditorState) : EditorState =
    { editor with
        Col = editor.Lines[editor.Row].Length
    }

/// True for word characters: letters, digits, and the underscore.
let private isWordChar (value: char) : bool =
    Char.IsLetterOrDigit(value) || value = '_'

/// Moves the cursor to the start of the previous word (Alt+B): skips
/// whitespace left, then the word itself.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let wordBack (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let mutable col = min editor.Col line.Length

    while col > 0 && Char.IsWhiteSpace(line[col - 1]) do
        col <- col - 1

    while col > 0 && isWordChar (line[col - 1]) do
        col <- col - 1

    { editor with Col = col }

/// Moves the cursor past the next word (Alt+F): skips the word itself,
/// then trailing whitespace.
/// <param name="editor">The buffer to move in.</param>
/// <returns>The moved buffer.</returns>
let wordForward (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let mutable col = min editor.Col line.Length

    while col < line.Length && isWordChar line[col] do
        col <- col + 1

    while col < line.Length && Char.IsWhiteSpace(line[col]) do
        col <- col + 1

    { editor with Col = col }

/// Deletes the character before the cursor, joining with the previous
/// line at its start.
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let backspace (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let col = min editor.Col line.Length

    if col > 0 then
        { editor with
            Lines =
                editor.Lines
                |> List.mapi (fun index content ->
                    if index = editor.Row then
                        content.Remove(col - 1, 1)
                    else
                        content)
            Col = col - 1
        }
    elif editor.Row > 0 then
        let previous = editor.Lines[editor.Row - 1]

        {
            Lines =
                editor.Lines
                |> List.removeAt editor.Row
                |> List.mapi (fun index content -> if index = editor.Row - 1 then previous + line else content)
            Row = editor.Row - 1
            Col = previous.Length
        }
    else
        editor

/// Deletes the character under the cursor, joining with the next line at
/// the line end.
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let deleteChar (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let col = min editor.Col line.Length

    if col < line.Length then
        { editor with
            Lines =
                editor.Lines
                |> List.mapi (fun index content ->
                    if index = editor.Row then
                        content.Remove(col, 1)
                    else
                        content)
        }
    elif editor.Row < editor.Lines.Length - 1 then
        {
            Lines =
                editor.Lines
                |> List.removeAt (editor.Row + 1)
                |> List.mapi (fun index content ->
                    if index = editor.Row then
                        line + editor.Lines[editor.Row + 1]
                    else
                        content)
            Row = editor.Row
            Col = col
        }
    else
        editor

/// Kills the word behind the cursor (Ctrl+W): deletes whitespace then the
/// word itself.
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let killWordBack (editor: EditorState) : EditorState =
    let target = wordBack editor

    if target.Col = min editor.Col editor.Lines[editor.Row].Length then
        editor
    else
        let line = editor.Lines[editor.Row]
        let col = min editor.Col line.Length

        { editor with
            Lines =
                editor.Lines
                |> List.mapi (fun index content ->
                    if index = editor.Row then
                        content.Remove(target.Col, col - target.Col)
                    else
                        content)
            Col = target.Col
        }

/// Kills to the start of the current line (Ctrl+U).
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let killToStart (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let col = min editor.Col line.Length

    { editor with
        Lines =
            editor.Lines
            |> List.mapi (fun index content ->
                if index = editor.Row then
                    content.Substring(col)
                else
                    content)
        Col = 0
    }

/// Kills to the end of the current line (Ctrl+K): at the line end it joins
/// the next line instead.
/// <param name="editor">The buffer to edit.</param>
/// <returns>The edited buffer.</returns>
let killToEnd (editor: EditorState) : EditorState =
    let line = editor.Lines[editor.Row]
    let col = min editor.Col line.Length

    if col < line.Length then
        { editor with
            Lines =
                editor.Lines
                |> List.mapi (fun index content ->
                    if index = editor.Row then
                        content.Substring(0, col)
                    else
                        content)
        }
    else
        deleteChar editor

// ──────────────────────────────────────────────────────────────────────────
// Session history (in-memory only, never persisted to disk)

// Browse state over the submitted entries (oldest first): None edits the
// fresh buffer, Some index shows that entry. Draft preserves the
// unsubmitted buffer while browsing so the newest step restores it.
type History =
    {
        /// The submitted entries, oldest first.
        Entries: string list
        /// The browsed entry index, or None for the fresh buffer.
        Cursor: int option
        /// The unsubmitted buffer preserved while browsing.
        Draft: string
    }

/// The empty history: no entries, no browse, no draft.
let emptyHistory: History =
    {
        Entries = []
        Cursor = None
        Draft = ""
    }

/// Appends one submitted entry and leaves browse mode.
/// <param name="history">The history to append to.</param>
/// <param name="text">The submitted text.</param>
/// <returns>The history with the entry appended.</returns>
let appendHistory (history: History) (text: string) : History =
    { history with
        Entries = history.Entries @ [ text ]
        Cursor = None
    }

/// Leaves browse mode after an edit: the edited buffer keeps its text.
/// <param name="history">The history to reset.</param>
/// <returns>The history with no browse cursor.</returns>
let resetBrowse (history: History) : History = { history with Cursor = None }

/// Browses one entry older: from the fresh buffer the draft is the
/// current text; at the oldest entry the buffer holds.
/// <param name="history">The history to browse.</param>
/// <param name="current">The current buffer text.</param>
/// <returns>The history and the buffer text to show.</returns>
let browseOlder (history: History) (current: string | null) : History * string =
    let currentText =
        match current with
        | null -> ""
        | text -> text

    match history.Entries with
    | [] -> history, currentText
    | entries ->
        match history.Cursor with
        | None ->
            let draft = currentText

            let index = entries.Length - 1

            { history with
                Cursor = Some index
                Draft = draft
            },
            entries[index]
        | Some index ->
            let older = max 0 (index - 1)

            { history with Cursor = Some older }, entries[older]

/// Browses one entry newer: past the newest entry the preserved draft
/// returns and browse mode ends.
/// <param name="history">The history to browse.</param>
/// <returns>The history and the buffer text to show.</returns>
let browseNewer (history: History) : History * string =
    match history.Cursor with
    | None -> history, ""
    | Some index ->
        if index >= history.Entries.Length - 1 then
            { history with Cursor = None }, history.Draft
        else
            let newer = index + 1

            { history with Cursor = Some newer }, history.Entries[newer]

// ──────────────────────────────────────────────────────────────────────────
// Slash-command hints

/// One hint row: the slash name plus its one-line description.
type CommandHint =
    {
        /// The slash name, with its leading slash.
        Name: string
        /// The one-line description shown beside the name.
        Description: string
    }

/// The full REPL command set in README order, each with its one-liner.
/// Mirrors ReplEngine.commandsUsage so hints and handlers never diverge in
/// naming; descriptions stay one line for the width clamp.
let builtinCommands: CommandHint list =
    [
        {
            Name = "/new"
            Description = "open a new session"
        }
        {
            Name = "/sessions"
            Description = "list stored sessions"
        }
        {
            Name = "/resume"
            Description = "attach a session by id or index"
        }
        {
            Name = "/model"
            Description = "list providers or switch model"
        }
        {
            Name = "/steer"
            Description = "interrupt the turn with new text"
        }
        {
            Name = "/follow"
            Description = "fold text into the running turn"
        }
        {
            Name = "/abort"
            Description = "abort the running turn"
        }
        {
            Name = "/compact"
            Description = "compact the session transcript"
        }
        {
            Name = "/tree"
            Description = "list journal positions to branch from"
        }
        {
            Name = "/fork"
            Description = "branch the session at a sequence"
        }
        {
            Name = "/clone"
            Description = "duplicate the active branch"
        }
        {
            Name = "/session"
            Description = "show message, turn, and token totals"
        }
        {
            Name = "/export"
            Description = "write the journal to a file"
        }
        {
            Name = "/quit"
            Description = "quit dot"
        }
        {
            Name = "/exit"
            Description = "quit dot (alias)"
        }
    ]

/// The maximum hint rows shown: the list truncates past this count.
let maxHints = 5

/// Queries the hint table for one cursor line: lines not starting with a
/// slash show nothing; otherwise builtins in table order then matching
/// template names ("/" plus name) ordinally sorted, truncated to maxHints.
/// <param name="line">The cursor line.</param>
/// <param name="templateNames">The available template names.</param>
/// <returns>The matching hints, at most maxHints.</returns>
let queryHints (line: string | null) (templateNames: string list) : CommandHint list =
    match line with
    | null -> []
    | text when not (text.StartsWith("/", StringComparison.Ordinal)) -> []
    | prefix ->
        let builtins =
            builtinCommands
            |> List.filter (fun hint -> hint.Name.StartsWith(prefix, StringComparison.Ordinal))

        let templates =
            if isNull (box templateNames) then
                []
            else
                templateNames
                |> List.choose (fun name ->
                    match box name with
                    | null -> None
                    | _ when String.IsNullOrWhiteSpace name -> None
                    | _ ->
                        let slash = "/" + name.Trim()

                        if slash.StartsWith(prefix, StringComparison.Ordinal) then
                            Some(
                                {
                                    Name = slash
                                    Description = "prompt template"
                                }
                            )
                        else
                            None)
                |> List.sortWith (fun left right -> String.CompareOrdinal(left.Name, right.Name))

        (builtins @ templates) |> List.truncate maxHints

// ──────────────────────────────────────────────────────────────────────────
// Input intents: the routing contract with DotTui

/// What one key means for the session: submit and steer carry the buffer
/// text, abort and quit drive the turn lifecycle, noop edits or ignores.
/// DotTui feeds submit and steer through ReplEngine.HandleLineAsync
/// verbatim (plain text queues, slash commands route as in the REPL),
/// abort through "/abort" (never quits), and quit through "/quit" for its
/// drain before exiting the loop.
type InputIntent =
    /// Submit the buffer: Enter on a non-blank buffer.
    | SubmitText of string
    /// Steer the running turn with the buffer: Ctrl+S on non-blank.
    | SteerText of string
    /// Abort the running turn without quitting: Ctrl+C.
    | AbortTurn
    /// Quit dot: Esc on an empty buffer, Ctrl+Q, or /quit.
    | QuitTui
    /// Edit the buffer or ignore the key: no routing.
    | Noop

/// Maps one submitted buffer to the exact line DotTui feeds the engine:
/// None when blank (empty sends nothing), else the raw text.
/// <param name="text">The buffer text.</param>
/// <returns>The line to route, or None.</returns>
let submitLine (text: string | null) : string option =
    match text with
    | null -> None
    | content when String.IsNullOrWhiteSpace content -> None
    | content -> Some content

/// Maps one steer buffer to the exact line DotTui feeds the engine: None
/// when blank, the verbatim slash line when the buffer names a command,
/// else "/steer " plus the buffer for the Interrupt path.
/// <param name="text">The buffer text.</param>
/// <returns>The line to route, or None.</returns>
let steerLine (text: string | null) : string option =
    match text with
    | null -> None
    | content when String.IsNullOrWhiteSpace content -> None
    | content when content.Trim().StartsWith("/", StringComparison.Ordinal) -> Some content
    | content -> Some("/steer " + content)

/// Classifies one submitted buffer: blank sends nothing, /quit and /exit
/// quit, everything else submits verbatim for the engine to route.
/// <param name="text">The buffer text.</param>
/// <returns>The submit intent.</returns>
let classifySubmit (text: string) : InputIntent =
    match submitLine text with
    | None -> Noop
    | Some content ->
        let trimmed = content.Trim()

        if
            String.Equals(trimmed, "/quit", StringComparison.Ordinal)
            || String.Equals(trimmed, "/exit", StringComparison.Ordinal)
        then
            QuitTui
        else
            SubmitText content

/// Classifies one steer buffer: blank sends nothing, slash buffers submit
/// verbatim, plain buffers steer.
/// <param name="text">The buffer text.</param>
/// <returns>The steer intent.</returns>
let classifySteer (text: string) : InputIntent =
    match steerLine text with
    | None -> Noop
    | Some routed when routed.StartsWith("/steer ", StringComparison.Ordinal) -> SteerText text
    | Some routed -> SubmitText routed

// ──────────────────────────────────────────────────────────────────────────
// Key decoder and pump step

/// True when the modifiers carry exactly Control (plus optionally Shift,
/// which terminals report for Ctrl+Shift+letter).
let private isCtrlOnly (modifiers: ConsoleModifiers) : bool =
    modifiers.HasFlag(ConsoleModifiers.Control)
    && not (modifiers.HasFlag(ConsoleModifiers.Alt))

/// True when no modifier is held (Shift alone still types).
let private isPlain (modifiers: ConsoleModifiers) : bool =
    not (modifiers.HasFlag(ConsoleModifiers.Control))
    && not (modifiers.HasFlag(ConsoleModifiers.Alt))

/// Decodes one console key to its pump outcome: the edited buffer, the
/// history, and the routing intent. Pure over the key, so the key map is
/// proven without a terminal:
/// Enter submits (blank sends nothing); Ctrl+O and Alt+Enter (where the
/// terminal reports it) insert a newline; Ctrl+S steers; Ctrl+C aborts
/// without quitting; Ctrl+Q quits; Esc quits on an empty buffer and leaves
/// browse mode otherwise; Up/Down browse history on the edge rows and move
/// inside the buffer; Ctrl+P/N always browse; Alt+B/F move by word;
/// Ctrl+A/E/Home/End move by line; Backspace/Delete remove; Ctrl+W/U/K
/// kill; printable characters insert; bare q (and every other printable)
/// types; anything else is ignored.
/// <param name="editor">The current buffer.</param>
/// <param name="history">The current history.</param>
/// <param name="key">The console key.</param>
/// <returns>The buffer, history, and intent after the key.</returns>
let applyKey (editor: EditorState) (history: History) (key: ConsoleKeyInfo) : EditorState * History * InputIntent =
    let edit (next: EditorState) : EditorState * History * InputIntent = next, resetBrowse history, Noop

    let show (text: string) : EditorState * History * InputIntent = fromText text, history, Noop

    if
        key.Key = ConsoleKey.Enter
        && key.Modifiers.HasFlag(ConsoleModifiers.Alt)
        && not (key.Modifiers.HasFlag(ConsoleModifiers.Control))
    then
        // Alt+Enter where the terminal reports it: progressive enhancement
        // for the Ctrl+O newline primary.
        edit (insertNewline editor)
    elif key.Key = ConsoleKey.Enter && isPlain key.Modifiers then
        match classifySubmit (toText editor) with
        | SubmitText _ as intent ->
            match submitLine (toText editor) with
            | Some content -> empty, appendHistory history content, intent
            | None -> editor, history, Noop
        | QuitTui -> editor, history, QuitTui
        | _ -> editor, history, Noop
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.O then
        edit (insertNewline editor)
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.S then
        match classifySteer (toText editor) with
        | SteerText _ as intent ->
            match steerLine (toText editor) with
            | Some _ -> empty, appendHistory history (toText editor), intent
            | None -> editor, history, Noop
        | SubmitText routed -> empty, appendHistory history (toText editor), SubmitText routed
        | _ -> editor, history, Noop
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.C then
        editor, history, AbortTurn
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.Q then
        editor, history, QuitTui
    elif key.Key = ConsoleKey.Escape then
        if isBlank editor then
            editor, history, QuitTui
        else
            resetBrowse history |> fun cleared -> editor, cleared, Noop
    elif key.Key = ConsoleKey.UpArrow && isPlain key.Modifiers then
        if editor.Row > 0 then
            edit (moveUp editor)
        elif history.Entries.IsEmpty then
            editor, history, Noop
        else
            let next, text = browseOlder history (toText editor)
            show text |> fun (shown, _, _) -> shown, next, Noop
    elif key.Key = ConsoleKey.DownArrow && isPlain key.Modifiers then
        if editor.Row < editor.Lines.Length - 1 then
            edit (moveDown editor)
        else
            match history.Cursor with
            | None -> editor, history, Noop
            | Some _ ->
                let next, text = browseNewer history
                show text |> fun (shown, _, _) -> shown, next, Noop
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.P then
        if history.Entries.IsEmpty then
            editor, history, Noop
        else
            let next, text = browseOlder history (toText editor)
            show text |> fun (shown, _, _) -> shown, next, Noop
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.N then
        match history.Cursor with
        | None -> editor, history, Noop
        | Some _ ->
            let next, text = browseNewer history
            show text |> fun (shown, _, _) -> shown, next, Noop
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.A then
        edit (lineStart editor)
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.E then
        edit (lineEnd editor)
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.W then
        edit (killWordBack editor)
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.U then
        edit (killToStart editor)
    elif isCtrlOnly key.Modifiers && key.Key = ConsoleKey.K then
        edit (killToEnd editor)
    elif
        key.Modifiers.HasFlag(ConsoleModifiers.Alt)
        && not (key.Modifiers.HasFlag(ConsoleModifiers.Control))
        && key.Key = ConsoleKey.B
    then
        edit (wordBack editor)
    elif
        key.Modifiers.HasFlag(ConsoleModifiers.Alt)
        && not (key.Modifiers.HasFlag(ConsoleModifiers.Control))
        && key.Key = ConsoleKey.F
    then
        edit (wordForward editor)
    elif key.Key = ConsoleKey.LeftArrow && isPlain key.Modifiers then
        edit (moveLeft editor)
    elif key.Key = ConsoleKey.RightArrow && isPlain key.Modifiers then
        edit (moveRight editor)
    elif key.Key = ConsoleKey.Home && isPlain key.Modifiers then
        edit (lineStart editor)
    elif key.Key = ConsoleKey.End && isPlain key.Modifiers then
        edit (lineEnd editor)
    elif key.Key = ConsoleKey.Backspace then
        edit (backspace editor)
    elif key.Key = ConsoleKey.Delete then
        edit (deleteChar editor)
    elif
        isPlain key.Modifiers
        && key.KeyChar <> '\u0000'
        && not (Char.IsControl(key.KeyChar))
    then
        edit (insertChar editor key.KeyChar)
    else
        editor, history, Noop

/// True when the key is a plain Enter: the send key with no modifier
/// held (Shift alone still sends; Alt+Enter and Ctrl combinations keep
/// their newline or command meanings). The TUI uses it to resume a blank
/// pending question answer verbatim: a blank buffer otherwise decodes to
/// Noop (empty sends nothing), but the REPL console reader resumes ""
/// through ReplyAsync, so plain Enter on a blank buffer with a pending
/// question must answer, not idle.
/// <param name="key">The console key.</param>
/// <returns>True for a plain Enter.</returns>
let isPlainEnter (key: ConsoleKeyInfo) : bool =
    key.Key = ConsoleKey.Enter && isPlain key.Modifiers

// ──────────────────────────────────────────────────────────────────────────
// Input-region renderer

/// The prompt prefix opening the first input row.
let promptPrefix = "> "

/// The continuation prefix opening wrapped buffer rows.
let continuationPrefix = "  "

/// The hint bar naming the key map, painted under the buffer and hints so
/// the bindings stay discoverable fullscreen (mirrored in the README).
let hintBarText =
    "Enter send | Ctrl+O newline | Ctrl+S steer | Ctrl+C abort | Ctrl+T expand | Up/Down history | Esc-empty/Ctrl+Q quit"

/// Clamps one row to the width with the DotShell trim marker.
/// <param name="width">The console width in columns.</param>
/// <param name="row">The row to clamp.</param>
/// <returns>The clamped row.</returns>
let clampRow (width: int) (row: string | null) : string =
    let safeWidth = max 20 (min 240 width)

    match row with
    | null -> ""
    | text when text.Length <= safeWidth -> text
    | text -> text.Substring(0, safeWidth - 1) + ">"

/// Renders the input region as width-clamped rows plus the cursor screen
/// position: the buffer tail window (scrolling to keep the cursor visible),
/// the slash hints, and the key-map hint bar. No row exceeds the width and
/// the row count never exceeds maxRows.
/// <param name="width">The console width in columns.</param>
/// <param name="maxRows">The maximum region rows, hint bar included.</param>
/// <param name="editor">The buffer to render.</param>
/// <param name="hints">The hint rows to show under the buffer.</param>
/// <returns>The rows and the zero-based (row, column) cursor position.</returns>
let renderInputRegion
    (width: int)
    (maxRows: int)
    (editor: EditorState)
    (hints: CommandHint list)
    : string list * (int * int) =
    let safeRows = max 3 maxRows

    let shownHints =
        if isNull (box hints) then
            []
        else
            hints |> List.truncate maxHints

    let hintRows =
        shownHints |> List.map (fun hint -> $"  {hint.Name} - {hint.Description}")

    let bufferWindow = max 1 (safeRows - 1 - hintRows.Length)

    let total = editor.Lines.Length
    let cursorRow = min (max 0 editor.Row) (max 0 (total - 1))

    let first = max 0 (min cursorRow (total - bufferWindow))

    let visible = editor.Lines |> List.skip first |> List.truncate bufferWindow

    let rows =
        visible
        |> List.mapi (fun index line ->
            let prefix =
                if first + index = 0 then
                    promptPrefix
                else
                    continuationPrefix

            clampRow width (prefix + line))
        |> fun buffer -> buffer @ (hintRows |> List.map (clampRow width))
        |> fun withHints -> withHints @ [ clampRow width hintBarText ]

    let cursorScreenRow = cursorRow - first

    let cursorLineLength =
        if cursorRow >= 0 && cursorRow < total then
            editor.Lines[cursorRow].Length
        else
            0

    let cursorScreenCol =
        let prefixLength =
            if cursorRow = 0 then
                promptPrefix.Length
            else
                continuationPrefix.Length

        min (prefixLength + min (max 0 editor.Col) cursorLineLength) (max 0 ((min 240 (max 20 width)) - 1))

    rows, (cursorScreenRow, cursorScreenCol)
