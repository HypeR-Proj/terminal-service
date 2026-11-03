<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# terminal-service

A HypeR component. See [HypeR](https://github.com/HypeR-Proj/HypeR) for
workspace setup and the installed SDK build boundary.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test terminal-service
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Stream admission

A lease's stream listener rejects malformed peer requests and rolled-back transfers
without closing later admission. Only `InvalidStreamRequest` identifies malformed
stream wire data; trusted SDK failures are reported and terminate the listener.
Lease expiry and endpoint closure retain their ordinary ownership cleanup.
