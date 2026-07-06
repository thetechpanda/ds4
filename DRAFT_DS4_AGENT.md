# DRAFT DS4 Agent Branch Review

This is a working capture of agent-related changes on `feat/agent-updates` versus `main`.
It is intentionally verbose and provisional. The final condensed document will be `DS4_AGENT.md`.

## Branch scope

- Base: `main`
- Branch: `feat/agent-updates`
- Range reviewed: `main...HEAD`
- Head commit: `d4bdc87`
- Local worktree note: unrelated local modification exists in `.gitignore`; do not treat it as branch evidence

## Changed files

- `ds4_agent.c`
- `ds4_agent_internal.h`
- `ds4_agent_subagent.c`
- `ds4_agent_subagent.h`
- `Makefile`
- `ds4_help.c`
- `README.md`
- `QA_BEFORE_RELEASES.md`
- `ds4_agent_docker.md`
- `ds4_web.c`
- `ds4_web.h`
- `ds4_web_remote_cdp.c`
- `ds4_web_remote_cdp.h`
- `.gitignore`

## Commit subjects in range

Commit subjects are useful hints only. The code is the source of truth.

- `d4bdc87` feat: subagent new now accepts a thinking override flag with the same user-facing values as /thinking (off|default|max), new subagents still inherit the active session’s thinking mode when no override is provided, and subagent status/list surfaces now carry and display each session’s thinking mode
- `2bd26c1` fix: docker sandbox is not initialised on model start
- `6bcc809` ds4_agent: surface Docker tool stderr and resolve sandbox paths consistently
- `a83d64e` feat: show-subagent-output-notices-in-footer
- `4372ce9` feat(agent): improve prompt loading and docker shell handling
- `b79ad75` feat(ds4_agent): harden sandbox and simplify docker infrastructure
- `275f776` fix: docker read command truncate output
- `6e59177` feat(ds4-agent): add /thinking and /preserve_agent_files runtime commands
- `1dfd685` fix(ds4_agent): add path validation and fix docker/local exec logic
- `519ea32` ds4_agent: add /purge_auto_files command; harden docker sandbox env
- `a7b2c63` fix(ds4_agent): remove redundant condition in agent_docker_exec_add_env
- `8af66dc` chore: simplify docker command
- `e2747d5` strict sandbox enforcement now exempts web_search and web_fetch
- `792ffdc` feat: forbid commands without a sandbox
- `1f43705` feat: docker sandbox
- `f1c82aa` chore: tidy prompt window
- `c51dbde` add docker sandbox to footer
- `77ec6ca` fix: guards around the availability of docker
- `041aa96` docker: update mounts and sync with workspaces
- `e9d48c0` add: `/docker help`
- `3807806` fix: `/docker stop` should unset the sandbox
- `f60ed0a` add "/docker destroy"
- `7db5e58` add `/docker stop` to stop the container and updates `/docker use` to start the container if stopped.
- `ab52230` `/docker use` without arguments shows the currently used sandbox
- `9993bff` add `/docker use {name}` marks a container as the current container, running containers are left running unless explicitly shut.
- `667e33c` add /commands to history
- `0b8ee43` docker create command is optional, chat messages stay on screen
- `c7c6a4f` add `/docker create {image} {name} {command}` creates a new sandbox, sandboxes workspaces are mounted via docker to `/{path/to/directory}` inside the image, to keep path identical between agent and sandbox. `temp` directories are mounted to temp directories. automatically switches the docker container to the new one. old container is left running. containers are tagged "ds4:sandbox".
- `6b5a3c9` add `/docker list` to list all containers available.
- `a616a24` add docker command path via argument, tightens docker version detection.
- `4d0f905` add a safeguard at start that verify that the docker command is available in $PATH when using docker options
- `67c0e21` initial docker options
- `0677056` chore: --working-directory to --workspace
- `c88ba57` uses web remote cdp instead of launching its own
- `4f681d9` fix: sandbox execution within working directory
- `d3024dd` fix: --recover terminal
- `d23c3b9` auto_allowed_paths: list, delete, and clear on workspace removal
- `cd4c8b8` macOS bash sandbox: strict mode by default, opt-out --no-strict-sandbox
- `7ad74cd` prevent removal of the last workspace root
- `78a4c82` chore: remove unnecessary set ctx_used, ctx_size in agent_worker_recover_session
- `99f2073` feat: session recovery
- `e892bba` feat(agent): add --temp-directory, auto-allow for tool-created files, and macOS sandbox profile support
- `976927b` feat(agent): add filesystem jail with workspace roots, path approval, and default launch directory

## High-confidence feature areas from code

### 1. Workspace jail and path approval

- The agent now tracks multiple workspace roots through `agent_config.working_directory_args` and runtime `worker.working_directories`.
- If no `--workspace` flag is passed, the launch directory becomes the initial workspace root.
- Relative tool paths resolve from the first workspace root.
- Tool path resolution canonicalizes with `realpath`, checks every configured root, and rejects escapes.
- A tool path outside the current roots triggers an interactive approval flow.
- The approval flow offers up to three choices: exact directory, parent, grandparent, plus deny.
- The approval prompt times out after 30 seconds and auto-selects the first option.
- The last workspace root cannot be removed.
- `/workspace` lists roots and auto-allowed paths.
- `/workspace +PATH` and `/workspace -PATH` add and remove roots at runtime.
- Removing a workspace also removes auto-allowed paths underneath that root.

### 2. Auto-allowed paths and cleanup

- Tool-created files can be added to `auto_allowed_paths`, which bypass workspace-root checks.
- These auto-allowed entries are visible in `/workspace`.
- Stale auto-allowed entries are pruned when the file no longer exists.
- `/purge_auto_files` lists auto-created files, gives a 5 second abort window, then deletes them.
- `preserve_agent_files` controls whether agent-created output files are kept after reading.
- New runtime toggles:
  - `/preserve_agent_files`
  - `/no_preserve_agent_files`

### 3. Strict sandboxing and Docker-gated tool execution

- `strict_sandbox` defaults to enabled.
- `--no-strict-sandbox` disables the requirement for an active Docker sandbox.
- Runtime toggles exist:
  - `/strict_sandbox`
  - `/no_strict_sandbox`
- The code explicitly distinguishes:
  - when tools may use Docker-backed filesystem behavior
  - when tools require Docker sandboxing to run at all
- Current code exempts only `web_browse` and `web_fetch` from strict sandbox enforcement.

### 4. Docker sandbox lifecycle

- Docker support is now a first-class agent runtime feature, not just a bash wrapper.
- Config fields include:
  - `--docker-command`
  - `--docker-container`
  - `--docker-image`
  - `--no-docker-auto`
- Runtime Docker commands include:
  - `/docker help`
  - `/docker debug`
  - `/docker list`
  - `/docker create IMAGE NAME [COMMAND]`
  - `/docker use [NAME]`
  - `/docker describe NAME`
  - `/docker stop [NAME]`
  - `/docker destroy NAME`
- DS4 sandbox containers are tagged `ds4:sandbox`.
- `/docker use` can show the current sandbox or switch to one by name.
- `/docker use` starts a stopped sandbox when needed.
- `/docker stop` clears active sandbox selection if the stopped container was current.
- `/docker destroy` only removes stopped, inactive DS4-tagged sandboxes and asks for confirmation.

### 5. Startup Docker sandbox activation

- Startup selection logic now lives in `agent_config_prepare_startup_docker_sandbox_with_input`.
- If `--docker-container` is provided, startup activates that sandbox.
- Otherwise, if auto-selection is enabled, interactive startup can prompt for a sandbox.
- In non-interactive mode, auto-selection chooses the first listed tagged sandbox.
- The code now attempts real activation at startup, not just name selection.
- On activation failure, it clears `docker_container` and `docker_image`.

### 6. Persistent Docker shell plus direct exec split

- Docker support uses two execution styles:
  - persistent `docker exec -i ... /bin/sh`
  - one-shot direct docker exec/capture
- The persistent shell is used for:
  - ranged file reads
  - whole-file reads in some helper paths
  - list/search/edit-related filesystem operations
- One-shot exec is used for:
  - streamed writes
  - bash jobs
  - sandbox lifecycle commands
- `ds4_agent_docker.md` documents this call graph and matches the code structure.

### 7. Docker robustness improvements

- Docker stderr is captured and surfaced back into tool results.
- Ranged Docker reads use `awk` in the sandbox and return line-numbered output.
- Whole-file Docker reads use an uncapped capture path to avoid truncation.
- Docker writes stream bytes through stdin rather than embedding content in shell text.
- Shell command env handling has been consolidated/hardened.
- Mount refresh has a fingerprint fast path to avoid unnecessary recreate work.

### 8. Workspace-aware Docker mount refresh

- Docker sandbox mounts are tied to current workspace roots and temp directory.
- Changing workspaces can trigger sandbox recreation to keep mounts aligned.
- Refresh logic computes a fingerprint over:
  - docker command
  - active container
  - temp dir
  - workspace roots
- If unchanged, mount refresh is skipped.
- If changed, labeled sandboxes are inspected and recreated to match current mounts.

### 9. `/thinking` runtime mode

- Runtime command: `/thinking off|default|max`
- Alt+Tab cycles thinking mode interactively.
- Thinking mode is shown in status/footer paths.
- `agent_think_mode_confirmation()` maps user-facing text to:
  - `off`
  - `default`
  - `max`

### 10. Resident subagents / multi-session manager

- New module split:
  - `ds4_agent_subagent.c`
  - `ds4_agent_subagent.h`
  - `ds4_agent_internal.h`
- The interactive runtime now runs through a `ds4_agent_subagents` manager.
- The manager creates a main session plus additional resident subagent sessions.
- Each slot contains:
  - its own `agent_worker`
  - config copy
  - prompt queue
  - background output buffer
  - unread/report/budget metadata
- A shared `model_gate` mutex serializes model-facing work across sessions.
- Nested subagent creation is blocked.
- Autonomous subagents get a model/tool round budget.

### 11. Subagent command surface

- `/subagent new [--tab|--background|--auto] [--thinking off|default|max] [name] [prompt]`
- `/subagent list`
- `/subagent switch <id|name>`
- `/subagent send <id|name> <prompt>`
- `/subagent stop <id|name>`
- `/subagent close <id|name>`
- `/subagent report <id|name>`
- `/subagent import <id|name>`

### 12. Subagent mission envelope and behavior

- Creating a subagent with a prompt generates a mission envelope message.
- That envelope includes:
  - subagent name
  - goal
  - autonomy mode
  - allowed tools
  - write policy
  - budget
  - stop conditions
  - report format
- The envelope explicitly says not to create other subagents.

### 13. Subagent UX and status

- Subagent list output includes:
  - active marker
  - id
  - name
  - state
  - context used/total
  - dirty/unread/approval/report markers
  - thinking mode
  - budget used/limit
- Footer building now includes subagent badges.
- Background output and unread session state are surfaced in footer/status paths.
- Approval prompts for subagents are prefixed with subagent identity.

### 14. Subagent thinking-mode inheritance and override

- New subagents inherit the active session’s thinking mode by default.
- `/subagent new` accepts `--thinking off|default|max` and `--thinking=<value>`.
- The public/status surfaces now carry subagent thinking mode per session.
- This behavior is backed by dedicated unit tests in `ds4_agent_subagent.c`.

### 15. Session recovery

- Startup option: `--recover SHA`
- Recovery path exists in `agent_worker_recover_session`.
- Recovery restores a saved session at startup and prints a recovery banner.
- Commit history mentions terminal-related recovery fixes; current code should be checked for final user-visible caveats.

### 16. Web / remote CDP support

- `ds4_web_config` now accepts:
  - `cdp_host`
  - `cdp_port`
- New helper module:
  - `ds4_web_remote_cdp.c`
  - `ds4_web_remote_cdp.h`
- The web layer can target an existing remote CDP endpoint rather than always launching local Chrome.
- WebSocket debugger URLs are rewritten when using remote CDP.
- If remote CDP is configured but unreachable, browser launch fallback is not attempted.
- Search URL changed from Google to DuckDuckGo in `ds4_web_google_search`.

### 17. Build system changes

- `Makefile` now includes `ds4_agent_subagent` and `ds4_web_remote_cdp` in agent builds.
- `ds4-agent` and `ds4_agent_test` switch to CPU object variants when `cpu` is part of `MAKECMDGOALS`.
- `ds4_agent` alias now maps to `ds4-agent`.
- Separate CPU test objects were added for:
  - `ds4_agent_test_cpu.o`
  - `ds4_agent_subagent_test_cpu.o`

## Supporting doc/help changes

- `README.md` now documents:
  - `--workspace`
  - workspace ordering semantics
  - runtime approval behavior
  - strict sandbox default
  - `/workspace` runtime commands
- `QA_BEFORE_RELEASES.md` adds a new manual QA checklist for:
  - workspace jail enforcement
  - multiple roots
  - path approval UX
  - root ordering
  - macOS read restrictions
- `ds4_help.c` adds CLI help for the new agent options and slash commands.

## Things to verify before finalizing DS4_AGENT.md

- Final user-visible footer/status strings for unread subagent output and thinking mode.
- Whether `command_output` behavior changed materially enough to document beyond existing behavior.
- Whether `.gitignore` additions are branch-intentional or just local workspace hygiene.

## Verified caveats from current code

- Strict sandbox exemption is narrower in code than one commit subject implies: `web_browse` and `web_fetch` are exempt; there is no current `web_search` tool name in dispatch.

## Likely final document structure

- Overview
- Runtime model
- Workspace jail and file access
- Docker sandbox model
- Tool execution behavior
- Thinking mode
- Subagents
- Session lifecycle and recovery
- Web/CDP integration
- Build/test impact
- Release/QA notes
