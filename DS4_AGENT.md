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

Strict sandboxing is enabled by default. In strict mode, model tools require an
active Docker sandbox, except for web browsing and web fetch operations. Pass
`--no-strict-sandbox` at startup, or use `/no_strict_sandbox` at runtime, to
allow tools without an active Docker sandbox. File tools remain constrained by
workspace path checks either way.

Docker startup options:

- `--docker-command PATH`: use a specific Docker executable.
- `--docker-container NAME`: use an existing container.
- `--docker-image NAME`: create/use a container from an image.
- `--docker-build FILE`: build an image from a Dockerfile for bash tools.
- `--no-docker-auto`: disable automatic selection of the first tagged DS4
  sandbox when no container is specified.

Runtime Docker commands:

- `/docker help`: show Docker sandbox commands.
- `/docker debug`: toggle Docker command debug logging.
- `/docker list`: list containers tagged `ds4:sandbox`.
- `/docker create IMAGE NAME [COMMAND]`: create a tagged sandbox and switch to
  it. The command defaults to `sleep infinity`.
- `/docker use [NAME]`: show the current sandbox or switch to `NAME`.
- `/docker describe NAME`: show detailed metadata for a tagged sandbox.
- `/docker stop [NAME]`: stop one tagged sandbox, or all DS4 sandboxes when no
  name is given.
- `/docker destroy NAME`: remove a stopped, inactive tagged sandbox after
  confirmation.

When a sandbox is active, file reads, writes, search/list operations, and bash
jobs run through the container. Workspace and temporary directories are mounted
so paths remain consistent between the host and sandbox. The implementation uses
a persistent Docker shell for read-like filesystem commands and direct
`docker exec` calls for streamed writes, bash jobs, and lifecycle operations.
See [ds4_agent_docker.md](/Volumes/Repositories/ds4/ds4_agent_docker.md) for
the helper-level Docker call graph.

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

- `/subagent new [--tab|--background|--auto] [--thinking off|default|max] [name] [prompt]`
- `/subagent list`
- `/subagent switch <id|name>`
- `/subagent send <id|name> <prompt>`
- `/subagent stop <id|name>`
- `/subagent close <id|name>`
- `/subagent report <id|name>`
- `/subagent import <id|name>`

`--tab` creates another resident session for manual switching. `--background`
creates a resident background session. `--auto` creates an autonomous delegated
worker with a mission envelope, tool/write policy, round budget, stop
conditions, and report format. New subagents inherit the active session's
thinking mode unless `--thinking` is provided.

## Web Tooling

The web subsystem can use a remote Chrome DevTools Protocol endpoint through
`--web-cdp-host` and `--web-cdp-port`. When those are set, `ds4-agent` targets
that endpoint rather than launching a local browser.

## Related Notes

- [README.md](/Volumes/Repositories/ds4/README.md) covers the broader DS4
  project and agent positioning.
- [ds4_agent_docker.md](/Volumes/Repositories/ds4/ds4_agent_docker.md)
  documents Docker helper internals.
- [DRAFT_DS4_AGENT.md](/Volumes/Repositories/ds4/DRAFT_DS4_AGENT.md) remains a
  branch-review draft for cleanup work, not the user-facing README.
