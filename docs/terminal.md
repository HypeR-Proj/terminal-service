<!-- SPDX-FileCopyrightText: 2026 roolrz -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native terminals

The formal environment provides terminal behavior through a userspace service.
It uses Native capabilities and messages, not Linux termios structures, ioctl
numbers, signals or process-group semantics. Guest UART emulation is separate.

## Service boundaries

`bootstrap/stage1/console-raw` owns the physical Console capability. It serves
byte streams and independent background-log channels. It does not edit input,
interpret EOF or apply terminal settings. Bootstrap init uses these byte streams
for its fallback shell only after a pre-handoff failure. All bootstrap programs
remain statically linked and independent of the formal terminal library.

The startup coordinator starts `svc/session-manager`. The manager owns one `terminal-service`
for its assigned Console transport and a session task group containing the shell
and its ordinary children. Ordinary data goes directly between the client,
terminal-service and console-raw. The manager is not a byte relay. Capabilities
and terminal state belong to that instance; the current deployment exposes one
physical Console endpoint.

`svc/terminal-service` privately owns the line discipline, bounded input/output
queues, echo, character conversion, software flow control and read timing.
`lib/terminal` provides the settings/events wire contract and scoped client API
for applications that configure a terminal or manage foreground leases. It does
not execute terminal behavior or own terminal queues. Applications that only
read stdin or write stdout/stderr use `std::io` without a terminal-library
dependency, whether their streams connect to a terminal, pipe or journal.
Generic request/response streams and standard-I/O integration live in `stdlib`;
they do not depend on the terminal library. The kernel retains raw Console I/O,
object waits, Process observation and TaskGroup retirement.

Background services receive independent log channels without ordinary raw
Console write authority. Bootstrap mirrors them through console-raw. Formal
services write stdout/stderr into the independent journal, which retains bounded
records and mirrors them to console-raw. Overflow counters remain observable
through journal queries; see [service management](https://hyper-proj.github.io/component-manager/docs/components.html). Logs bypass terminal software flow control and can disturb a prompt
or full-screen display. Kernel log mirroring is unchanged. This transport is not
a persistent journal.

## Session configuration

`session-manager` reads the optional `/etc/hyper/session.json` file once
at startup. An absent file, or omitted fields, uses these defaults:

```json
{
  "terminal": "svc/terminal-service",
  "shell": "bin/sh",
  "restart_limit": 3,
  "failure_grace_ms": 2000
}
```

Image paths are relative to the retained formal root environment. Absolute
paths, empty components, `.` and `..` components, and NUL bytes are rejected.
The file is limited to 4096 bytes; unknown fields and malformed JSON fail manager
startup instead of silently selecting defaults.

`restart_limit` accepts 0 through 10 and limits consecutive session restarts
after exits within 10 seconds of launch, including clean shell exits. Zero
disables retries after such an exit. A session lasting at least 10 seconds resets
the counter. Each restart waits 250 ms. This is the manager's local session
policy; component-manager continues to own whole-service supervision.

When terminal-service dies, its existing streams return `PEER_CLOSED`; this is
not the canonical-mode EOF character. After reporting the failure, session-manager
allows up to `failure_grace_ms` (0 through 10000) for disconnected applications to
finish or save through their remaining capabilities. It then stops the old
session group and resets the raw console before creating a new terminal and
shell. No old stream is reconnected and no buffered input is replayed. The grace
period provides an opportunity for application cleanup, not an automatic-save
guarantee. Startup failure before a shell exists does not need this grace period.

Component-manager reports startup state directly on the Console after receiving
bootstrap ownership, before formal providers are adopted. It writes complete
lines without erasing previous output. Raw consoles continue mirroring service
logs throughout startup; kernel and emergency output remain independent.

The terminal is prepared after raw console and journal readiness, before I/O and
filesystem adoption. Session READY means the terminal exists; an independent
one-shot startup channel holds back the first shell until the startup sequence
has attempted retirement. Failed adoption or retirement also ends the progress
report and releases an already prepared session. This gate controls only the
first interactive client, not service logging. Later session epochs use the
normal local restart policy without this startup gate.

Each raw log source retains at most 64 KiB of rendered bytes under UART
backpressure and discards excess records. Journal independently retains bounded
queryable records. These bounds do not provide a logging or durability barrier;
background logs continue after the interactive session starts.

## Input and output behavior

The default profile is canonical, with CR-to-LF input conversion, echo, UTF-8
scalar erase, and LF-to-CRLF output conversion. Configurable controls include
EOF, erase, line kill, word erase, literal next, reprint and an extra line
terminator. A disabled control is distinct from a control assigned to NUL.

EOF after a partial line makes those bytes readable without adding another EOF
record. EOF on an empty line produces one zero-length read; later reads still
work. Overlong canonical lines are discarded through their delimiter and report
input loss, so a truncated command is not executed. Erase handles Unicode
scalars and malformed bytes deterministically; it is not a grapheme or terminal
screen-width implementation.

Input conversion includes ignoring CR, CR/LF mapping, optional high-bit stripping
and case conversion. Literal-next bypasses CR/LF conversion, delimiters and
flow recognition, but still applies high-bit stripping and lowercase conversion.
The supported formatting choices follow Linux N_TTY behavior:

| Native setting/control | Formatting behavior |
| --- | --- |
| `visual_erase` | Erase a displayed character with backspace/space/backspace; a tab needs only backspaces. Without it, echo the erase control itself. |
| `echo_kill_line`, `visual_kill` | With echo, visual erase and both kill options enabled, erase the line visually. Otherwise echo the kill control and append a newline only when `echo_kill_line` is enabled. Empty-line kill is silent. |
| `echo_erased` | Hardcopy echo: print erased characters between backslash and slash, instead of moving the cursor back. |
| `echo_newline` | Echo an unquoted canonical newline when ordinary echo is off; it has no effect in noncanonical mode. |
| Word erase | Remove trailing separators and the last word, retaining its preceding separator. Like N_TTY, classify a UTF-8 scalar by its leading byte using the Latin-1 alphanumeric/underscore class. |
| Literal-next, reprint | With control notation enabled, literal-next shows caret/backspace. Reprint echoes its control, a newline, then the unfinished line. Reprint is ordinary input when echo is disabled. |
| `input.lowercase`, `output.uppercase` | Traditional Latin-1 byte case conversion, as in N_TTY. These options are disabled by default and should remain disabled for UTF-8 text. |
| CR/LF and tabs | Suppress a CR at column zero before CR-to-LF conversion. CR/LF transformations are nonrecursive; tabs use eight-column stops. Invisible controls such as BEL do not advance the tracked column. |

With `echo_controls`, quoted LF echoes as `^J`. Outside canonical mode, a literal
LF also echoes as `^J`, while a CR converted to LF echoes a line break. UTF-8
column tracking counts leading bytes, as N_TTY does; display-width and grapheme
editing belong to the application. Echo and application output share one ordered
bounded queue with space reserved for echo. Echo overflow reports loss and does
not advance the column for bytes that were not queued.

`Settings::raw()` makes both directions binary-transparent, including NUL, ESC,
CR, Ctrl-D, XON and XOFF. It disables canonical processing, echo, transformations
and both software-flow directions. Editors and display tools should explicitly
select their required profile rather than assume the shell's settings.

Noncanonical reads use an explicit policy:

| Policy | Completion |
| --- | --- |
| `Available` | Available bytes, or `WouldBlock` when empty |
| `AtLeast` | At least the requested minimum, capped by the read buffer |
| `FirstByteTimeout` | Available bytes, or a timeout measured from admission |
| `InterByteTimeout` | Minimum bytes, or a gap timeout after the first byte; new input restarts the gap |

An absolute request deadline is independent of that policy. EOF, timeout,
cancellation and configuration change have distinct Native read results; peer
disconnection is an error. Input-queue loss is reported through terminal events,
not a claim to detect every physical UART overrun. A settings change does not
reinterpret previously accepted bytes. Canonical partial input becomes readable
when changing to noncanonical. Each settings change seals any buffered
noncanonical tail: that record remains readable as a short read even if the new
profile requires a larger minimum. Fresh input follows the new read policy.

## Software flow control and ordering

Three states are independent: the peer's STOP pauses local output, explicit
output pause is administrative, and a locally sent STOP throttles the peer.
START and optional resume-on-any clear only the first state. STOP itself does
not resume output. Input and control messages continue to be serviced while
output is paused or input buffers are full.

Automatic input throttling uses high/low watermarks with headroom for bytes
already in flight. Canonical input is throttled only while a readable record is
queued; an unfinished line must still be able to receive its delimiter. A
separate bounded flow queue bypasses paused/transformed
output. Before replacing active flow characters, the service sends the old
START and fences its handoff, then commits the new recovery descriptor to the
manager before it can send a new STOP. The descriptor has an idempotent
transaction identity. If the terminal dies, the manager can restore the last
committed START without guessing which profile was active.

A stream `FENCE` follows data on the same private writer connection. Completion
means preceding bytes reached the kernel Console transmit queue, not that the
UART has physically finished transmitting. Flush application-side buffers first.
Input discard cancels the raw pending read and performs a bounded cutoff in both
the raw service and kernel receive queue. It is not an unbounded read-until-empty
loop.

## Client ownership and standard I/O

A launcher prepares inactive child endpoints, starts the process, derives a
WAIT-only Process observer and transfers that observer to activate the lease.
The launcher retains its independent Process supervisor. Prepared children may
emit bounded startup diagnostics but cannot consume foreground input. Ordinary
foreground changes preserve unconsumed typeahead; owner death restores the
parent lease and its settings. Revoking an ancestor removes its entire subtree
before restoring the surviving parent once, including asynchronous flow-profile
recovery. Size and coalesced terminal events have a separate control/event
interface.

The lease follows its observed Process or an explicit launcher release. Closing
the optional control handle does not invalidate standard streams during runtime
cleanup. A launcher must release a prepared child after a failed launch.

Binary input with no echo, conversion or software flow is pulled only for an
outstanding read. A bounded ingress buffer retains any untouched bytes from an
in-flight transport request across foreground changes; the next owner's profile
processes them once. Already processed logical records are never reinterpreted.
This preserves pasted input when a raw shell starts a canonical command.

`hyper_terminal::Terminal::take` obtains optional scoped control from startup.
Ordinary data uses `std::io`. `hyper_os::stream::ProcessInput` exposes the same
process-owned stdin connection for event-driven clients such as `top` and
`vmm console`; a second concurrent owner is rejected. An abandoned pending read
closes its private connection, preventing its reply from becoming a later
reader's result.

An interactive shell prepares one canonical foreground lease for a pipeline.
Every unredirected terminal stream uses that lease; the last stage alone receives
control/events and owns the lease through its Process observer. An upstream
producer exiting does not revoke the consumer's keyboard access. This is pipeline
ownership, not a Unix process group or job-control implementation.

The last stage also receives the optional `client::STREAM` connector.
`Terminal::open_input()` opens a private keyboard connection on the same lease,
independently of stdin. Fullscreen applications can redirect stdin while still
reading navigation keys from the terminal. Other stages retain ordinary byte streams
without independent terminal-control authority. Applications must coordinate
multiple readers of the same foreground input; the shell does not multiplex
keystrokes among pipeline stages.
Generic `std::process::Command` inheritance instead opens a private connection
under the caller's existing lease; it does not manufacture terminal control or
foreground policy inside stdlib. The caller should relinquish stdin while that
child reads. Editors that need an independent foreground child use the terminal
library's prepare/bind interface. Caller-owner death revokes inherited access.

The runtime lazily connects standard streams after launch readiness. Each process
has private request/response connections; child inheritance passes connectors,
not a parent's pending read or private session. Ordinary terminal `Read`,
`read_exact`, vectored reads and byte iteration request the caller's capacity
instead of prefetching into Rust's global stdin buffer. Explicit `BufRead` retains
its buffering contract. Pipes retain their ordinary buffering and preserve every
byte. Per-descriptor `IsTerminal` follows connector metadata; replacing a stream
with a pipe removes that terminal identity.

## Failure and recovery

Terminal/session failure invalidates the old raw binding before stopping its
task groups. The manager waits for their retirement, discards stale input,
restores the committed flow character and display baseline, and starts a fresh
session with bounded retries and backoff. This is fresh-session recovery, not
transparent editor-state preservation. The flat session group covers ordinary
shell children, not arbitrary descendants explicitly moved into other groups.
The recovery guarantee assumes the manager survives terminal failure.

Stage-1 fallback handles only failures before supervision handoff. Formal-init
or terminal failures after that commit do not restart bootstrap recovery.

Hardware baud rate, parity, modem controls, break generation and legacy printer
delay flags are not provided by this terminal API. There is no implicit Linux
signal generation or Unix job-control ABI.

## Validation

`make app-test APP_TEST_PACKAGE=hyper-terminal` checks the settings wire contract.
`make app-test APP_TEST_PACKAGE=hyper-terminal-service` exercises the private discipline,
including editing, EOF, binary raw data, timing, conversions and flow control.
The shared byte vectors in `svc/terminal-service/tests/formatting.tsv` also run against
real Linux PTYs through `python3 -B svc/terminal-service/tests/linux-formatting.py` in the
scripts suite on Linux. This checks observable formatting without adopting the
Linux termios ABI. Native timeout/error distinctions, bounded overflow handling
and malformed-UTF-8 recovery remain explicit Native policies.
`make sdk-test` covers generic stream ownership, cancellation and ordered writes.
`make test-terminal ARCH=aarch64` runs the development-only `terminal-probe`
through the actual shell and standard streams, including editing/echo formatting,
full reply queues,
cancellation ordering and subtree revocation during flow-profile recovery.
`make test-console` covers normal shell interaction, std subprocess inheritance
and interactive command exit.

The fullscreen [editor](https://hyper-proj.github.io/editor/docs/usage.html) uses these application interfaces.
`make test-editor` checks editing, successful saves, failed-save recovery and
restoration of shell interaction. The `native-apps` CI shard includes this
scenario on both supported architectures.

The [pager](https://hyper-proj.github.io/less/docs/usage.html) reads file or pipe content independently of the terminal
keyboard. `make test-less` checks file/pipeline paging, navigation while a
producer is idle, redirected byte copying and restoration of shell interaction.
It also runs in the `native-apps` CI shard.
