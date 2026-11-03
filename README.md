<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# terminal-service

Formal Native terminal line discipline and reactor.

Owns one terminal’s settings, input queues, stream processing and control admission over
a raw-console binding. `lib-terminal` defines client records; session-manager owns
process creation and session retirement.

See the [implementation entry](src/main.rs) and [component declaration](component.json)
for the public interfaces, build dependencies and delivered artifacts.

## Boundaries

The raw binding can be revoked during retirement. This service supplies the implemented
terminal protocol, not a complete POSIX TTY/signal subsystem.

## Build and test

Use this checkout inside the [repo-managed workspace](https://github.com/HypeR-Proj/HypeR).
From this repository directory:

```sh
make
make test
```

An absolute `OUT=/path/to/out` selects a shared output directory. The module
entry delegates to build/system, which prepares the installed SDK and declared
dependencies. Native binaries require HypeR; host module checks cover the tests
selected by component.json. A standalone clone does not contain the workspace’s
other source repositories or Native SDK.


## Stream admission

A lease's stream listener rejects malformed peer requests and rolled-back transfers
without closing later admission. Only `InvalidStreamRequest` identifies malformed
stream wire data; trusted SDK failures are reported and terminate the listener.
Lease expiry and endpoint closure retain their ordinary ownership cleanup.
