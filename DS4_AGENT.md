# ds4-agent

`ds4-agent` is the native DS4 coding-agent interface. It runs the model locally,
keeps the active DS4 session in process, exposes DSML tools directly, and wraps
tool execution in workspace and Docker controls that are meant for real codebase
work rather than toy prompts.

Build it with:

```sh
make ds4-agent
```

For a CPU build path, include the repository's `cpu` target:

```sh
make cpu ds4-agent
```

Run `./ds4-agent --help` for the complete option list.

## Starting The Agent

A typical launch is:

```sh
./ds4-agent --workspace /path/to/project
```

Useful startup options:

- `-p, --prompt TEXT`: submit an initial prompt after startup.
- `--non-interactive`: run without the terminal UI. With `-p`, this performs one
  turn; without `-p`, it reads repeated prompts from stdin.
- `-sys, --system TEXT`: add extra system prompt text. An empty value disables
  extra text.
- `--trace FILE`: write prompt, token, and DSML debug trace output.
- `--recover SHA`: load a saved agent session by SHA prefix at startup.
- `--chdir DIR`: change directory before loading runtime assets.
- `--workspace DIR`: add a jailed workspace root. Repeatable; defaults to the
  launch directory.
- `--temp-directory DIR`: set the directory for temporary files.
- `--web-cdp-host HOST` and `--web-cdp-port N`: connect web tooling to an
  existing Chrome DevTools Protocol endpoint.

## Workspace Jail

`ds4-agent` treats file access as workspace-scoped.

If no `--workspace` is passed, the launch directory becomes the initial
workspace root. Relative tool paths resolve from the first workspace root, and
bash commands start there. The agent canonicalizes tool paths and rejects
escapes outside the configured roots.

Runtime workspace commands:

- `/workspace`: list workspace roots and auto-allowed paths.
- `/workspace +PATH`: add a workspace root.
- `/workspace -PATH`: remove a workspace root or auto-allowed path.

When an interactive tool call targets a path outside the current roots, the
agent asks before expanding access. The prompt offers the exact directory, its
parent, the next parent up, and deny. If no answer arrives in 30 seconds, it
chooses the exact target directory.

Tool-created files can be auto-allowed so follow-up reads can access them
without expanding the whole workspace. Use `/purge_auto_files` to list and
delete those generated files after a 5 second abort window. Use
`/preserve_agent_files` and `/no_preserve_agent_files` to control whether agent
output files are retained after reading.

## Sandboxing And Docker

**Strict sandboxing**: when enabled (the default), model tools require an active
Docker sandbox. File tools remain constrained by workspace path checks either
way. Pass `--no-strict-sandbox` at startup, or use `/no_strict_sandbox` at
runtime, to allow tools without an active Docker sandbox.

**Docker startup options**:

- `--docker-command PATH` — use a specific Docker executable (default: `docker`).
- `--docker-container NAME` — use an existing container by name at startup.
- `--no-docker-auto` — disable automatic selection of the first tagged DS4
  sandbox at startup. By default, if no `--docker-container` is given, the agent
  lists containers with the `ds4:sandbox` label and prompts to pick one (or
  auto-selects the first in non-interactive mode).

### `/docker help` — show sandbox command help

Prints the sandbox management command reference. Lists all available `/docker`
commands and their usage.

### `/docker debug` — toggle command debug logging

Toggles debug logging of every Docker command before execution. When enabled,
the full command line is printed in cyan. The flag is stored in
`cfg->docker_debug` and checked by every Docker exec call.

### `/docker list` — list tagged sandboxes

Lists all containers tagged `ds4:sandbox`. Shows:

- Container name
- State (running, exited, created, dead) — green highlighting when running
- IP address
- Image name
- JSON labels
- Active indicator (`*`) for the currently selected sandbox

Uses `agent_docker_list_sandbox_names` to collect names, then
`agent_docker_inspect_sandbox` with a Go template for each container to extract
name, labels, image, state, and IP.

### `/docker create IMAGE NAME [COMMAND]` — create a sandbox

Creates a new labeled Docker sandbox. Steps:

1. Validates that the Docker command is in PATH.
2. Parses positional arguments: `<image>` (required), `<name>` (required),
   `[command]` (optional, defaults to `sleep infinity`).
3. Builds a `docker run -d --name <name> --label ds4:sandbox` command with
   bind mounts for every workspace root and the temp directory.
4. Executes the command and selects the new sandbox immediately.
5. Stops any stale persistent shell and starts a fresh one.

The container is tagged `ds4:sandbox` so it appears in `/docker list`.

### `/docker use [NAME]` — show or switch sandbox

With no argument, prints the currently active sandbox name. With a name,
activates the named container: inspects it, verifies the `ds4:sandbox` label,
starts it if stopped, waits for running state, then starts a persistent shell.
The active container is stored in `cfg->docker_container`.

### `/docker describe NAME` — show sandbox metadata

Prints detailed metadata for one tagged sandbox:
- name, state, image, IP, tags (labels JSON), active flag

Uses `agent_docker_inspect_sandbox` and `agent_docker_parse_sandbox_row` to
extract fields from the tab-separated inspect output. Rejects containers that
do not carry the `ds4:sandbox` label.

### `/docker stop [NAME]` — stop sandbox(es)

With no argument, stops **all** labeled sandboxes. With a name, stops only that
container. If the active container is being stopped, the persistent shell is
torn down first and `cfg->docker_container` is cleared.

### `/docker destroy NAME` — remove a sandbox

Removes a stopped, inactive tagged sandbox after a confirmation prompt
(`"Destroy docker sandbox <name>? (Y/n)"`). The container must be in `exited`,
`created`, or `dead` state and must not be the currently active sandbox. Uses
`docker rm` for removal.

### Sandbox lifecycle

**Labeling**: every sandbox managed by `ds4-agent` carries the label `ds4:sandbox`.
All sandbox commands filter to this label so unrelated user containers are
ignored.

**Activation** (agent_docker_activate_named_sandbox): inspects the container,
verifies the `ds4:sandbox` label, starts it if needed, waits for a running
state, and then starts a persistent shell. The active container name is stored
in `cfg->docker_container`.

**Mount fingerprint** (agent_docker_mount_fingerprint): a hash of the Docker
command path, container name, temp directory, and workspace roots. The mount
refresh logic (`agent_docker_refresh_mounts`) compares the current fingerprint
against the stored value; if they differ, every labeled sandbox is stopped,
removed, and recreated with updated bind mounts. This is triggered automatically
after `/workspace +PATH` or `/workspace -PATH`.

**Persistent shell** (agent_docker_shell_start): a long-lived `docker exec -i
/bin/sh` child process attached to two pipes. Read-like file operations
(`read`, `search`, `list`) send commands through this shell and read back
output terminated by a sentinel line. Write, bash, and lifecycle operations use
separate `docker exec` calls that bypass the shell for reliability. The shell
is stopped (`agent_docker_shell_stop`) when the active sandbox changes or the
worker is freed.

**File operations when a sandbox is active**: every file tool call
(agent_tool_read_file, agent_tool_write_file, agent_tool_search, agent_tool_list)
checks `agent_tool_use_docker_filesystem`. When true, paths are resolved inside
the container using `agent_resolve_docker_candidate`, and reads/writes go
through `agent_docker_read_file_bytes`/`agent_docker_write_file_bytes` using
the persistent shell or direct `docker exec`. Bash jobs always run through
`docker exec` inside the sandbox. Workspace and temp directories are bind-mounted
identically to the host so paths remain consistent.

**Startup auto-selection** (agent_config_prepare_startup_docker_sandbox_with_streams):
at startup, if `--docker-container` was given it is used directly. Otherwise,
if `--no-docker-auto` is not set, the agent lists all `ds4:sandbox` containers
and prompts the user to pick one (interactive) or auto-selects the first
(non-interactive).

## Sessions

Agent sessions are stored in `~/.ds4/kvcache`.

Session commands:

- `/save`: save the current session.
- `/list`: list saved sessions.
- `/switch SHA`: load a saved session and show recent history.
- `/del SHA`: delete a saved session.
- `/strip SHA`: remove the saved KV payload while preserving rendered text and
  title; switching to the stripped session rebuilds KV by prefilling.
- `/history [N]`: show recent user turns from the current session.
- `/new`: start a fresh session from the system prompt.
- `/compact`: compact the current session context now.

The runtime asks before discarding a dirty session when switching, starting a new
session, or exiting.

## Interactive Controls

Core commands and keys:

- `/help`: show interactive commands.
- `/thinking off|default|max`: set thinking effort.
- `/strict_sandbox`: require an active Docker sandbox before tools run.
- `/no_strict_sandbox`: allow tools without an active Docker sandbox.
- `/command_output on|off`: show or hide command output mirrored in the
  terminal.
- `/power N`: set GPU duty cycle percentage from 1 to 100.
- `/quit` or `/exit`: exit.
- `Ctrl+C`: interrupt generation or clear edited text.
- `Enter`: queue text while the agent is busy.
- `Ctrl+X`: edit the first queued prompt.
- `Esc`: interrupt and send the queued prompt immediately.
- `Ctrl+D`: exit from an empty prompt.
- `Alt+Tab`: cycle thinking mode.

The footer shows the active workspace, sandbox state, thinking mode, queued
prompt preview, and subagent/background activity when relevant.

## Resident Subagents

`ds4-agent` can run resident subagent sessions inside the same process. The main
session and subagents share one loaded DS4 engine, while each session keeps its
own prompt queue, transcript/session state, output tracking, and thinking mode.
Model-facing work is serialized through a shared gate.

Subagent commands:

- `/subagent new [--tab|--background|--auto] [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]`
- `/subagent list`
- `/subagent switch <id|name>`
- `/subagent send <id|name> <prompt>`
- `/subagent stop <id|name>`
- `/subagent close <id|name>`
- `/subagent report <id|name>`
- `/subagent import <id|name>`

### `/subagent new` — create a subagent

Creates a resident subagent and optionally starts it with a prompt. Subagents
cannot create nested subagents (the active session must be the main session).

**Autonomy modes** (choose exactly one of `--tab`, `--background`, or `--auto`):

| Flag            | Behavior |
|-----------------|----------|
| `--tab`         | A manual session you can switch to with `/subagent switch`. No background execution — work only happens while it is the active session. |
| `--background`  | A background worker that processes its prompt queue autonomously. Output accumulates in a background buffer. If no prompt is given, `--tab` is forced so the session is not useless. |
| `--auto`        | An autonomous delegated worker. A mission envelope is prepended to the prompt with the tool policy, round budget, stop conditions, and report format. The worker runs until the budget is exhausted, an error occurs, it is interrupted, or it reports completion. The worker's tool round budget defaults to `-1` (disabled). |

With `--auto`, `--budget N` overrides the default model/tool round budget. `N`
must be `-1` or a positive integer. `-1` disables the round budget. The option
is rejected for non-autonomous modes.

**Thinking mode** (optional):

- `--thinking off` / `--thinking default` / `--thinking max` overrides the
  inherited thinking mode. Without this flag the subagent inherits the active
  session's thinking mode.

**Tool-access policy** (optional):

- `--tools POLICY` or `--allowed-tools POLICY` restricts which tools the
  subagent may call. The policy string is parsed into an allow/deny list. If
  omitted, the subagent inherits the active session's tool policy.

**Name and prompt** (positional):

- `<name>` — optional display name. If omitted, a default name like
  `subagent-1`, `subagent-2`, etc. is generated. Names must be unique; creation
  fails if a subagent with the same name already exists.
- `<prompt>` — optional initial prompt. If given, the subagent begins processing
  immediately (for `--background`/`--auto`) or is queued (for `--tab`). For
  `--auto` and `--background`, the prompt is wrapped in a mission envelope
  containing the autonomy mode, tool policy, budget, stop conditions, and
  report format.

**Limitation**: the underlying `ds4_agent_subagent_create_request` struct also
supports `stop_conditions` and `report_format`, but those fields are not
currently available as command-line flags. They are left at sensible defaults
(stop on done/blocked/interrupted/budget-exhausted, concise result with
evidence).

### `/subagent list` — list subagents

Shows every resident subagent with:

- Active indicator (`*` for the current session)
- Numeric ID and name
- State (idle, running, waiting-model, approval-blocked, error, stopped)
- Context usage (used/total, formatted by `agent_format_ctx_size`)
- Flags: `dirty` (unsaved session), `unread` (queued output pending),
  `approval` (web/path approval blocked), `report` (report available)
- Thinking mode (off/default/max)
- Budget usage (if a budget limit is set)

### `/subagent switch <id|name>` — switch active session

Makes the specified subagent the active session. The target is resolved by
numeric ID (decimal digits) first, then by name. All further interactive I/O
and command dispatch run against this session until another switch. Sets
`unread` to false on the target so queued output is no longer flagged.

### `/subagent send <id|name> <prompt>` — queue a prompt

Pushes a prompt into the subagent's queue. The subagent must be idle; if it is
busy the command fails. Background and autonomous subagents process queued
prompts automatically; tab-mode subagents only process while they are the active
session.

### `/subagent stop <id|name>` — interrupt a subagent

Calls `worker_interrupt` on the subagent's worker, which stops model inference
and tool execution. The stop reason is set to `"interrupted"`. The subagent
remains usable — you can send new prompts after stopping.

### `/subagent close <id|name>` — remove a subagent

Destroys the subagent slot and frees its resources. If the subagent has a dirty
worker session, the runtime prompts `"Save subagent before closing? (y/n)"`
before proceeding. If the closed session was the active one, the active session
falls back to the first remaining slot. The last session cannot be closed.

### `/subagent report <id|name>` — print the subagent's final report

Prints the stored report text — the last 4096 bytes of the subagent's output
captured when the worker stopped (idle, error, or interrupted). If no report is
available yet, prints `"No subagent report is available yet."`.

### `/subagent import <id|name>` — import report into the active session

Wraps the subagent's report in a preamble (`"Delegated subagent report from
<name>:\n\n"`) and pushes it as a queued prompt into the active session. If the
active session is not busy (the runtime flag `busy` is false), the message
`"report imported into active session"` is printed; otherwise
`"report queued for active session"` is shown.

## Web Tooling

The web subsystem can use a remote Chrome DevTools Protocol endpoint through
`--web-cdp-host` and `--web-cdp-port`. When those are set, `ds4-agent` targets
that endpoint rather than launching a local browser.
