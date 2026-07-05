## Context

`ds4-agent` starts by detecting whether Docker sandboxing is available, then calls `agent_config_autoload_first_docker_sandbox()` when `docker_auto` is enabled. That helper only lists tagged sandbox names and stores the first name in `cfg->docker_container`. Later startup paths and the footer treat the presence of `docker_container` as enough to show the sandbox as active, even though the selected container may still be stopped.

The existing `/docker use` command already performs the safer behavior we want: inspect the named sandbox, verify it is tagged correctly, start it if it is stopped, then switch the worker and persistent shell to that running container. The startup path should reuse that activation contract instead of maintaining a weaker name-only selection path.

## Goals / Non-Goals

**Goals:**
- Ask the user which Docker sandbox to use during normal interactive startup when Docker auto-selection is enabled and tagged sandboxes exist.
- Ensure a confirmed sandbox is running before `ds4-agent` treats it as selected and before the footer presents it as active.
- Keep the startup path aligned with the existing `/docker use` semantics so one code path owns inspection and start-if-needed behavior.
- Preserve predictable behavior for non-interactive mode and explicitly selected `--docker-container` runs.

**Non-Goals:**
- Redesign all Docker slash commands or sandbox lifecycle management.
- Add background health polling for sandboxes that are stopped externally after startup.
- Change the meaning of strict sandbox mode beyond ensuring startup selection is real.

## Decisions

### 1. Prompt only in the interactive Docker-auto startup path

Startup prompting should happen only when all of the following are true: Docker support is available, `docker_auto` is enabled, no explicit `docker_container` was passed, the process is interactive, and at least one tagged sandbox exists.

Why:
- This keeps the new UX exactly where the current silent auto-select happens.
- It avoids blocking scripts or batch usage.
- It preserves user intent when `--docker-container` already names the sandbox to use.

Alternatives considered:
- Always prompt whenever Docker is available. Rejected because it would break non-interactive and explicitly configured flows.
- Keep auto-selecting the first sandbox. Rejected because it preserves the misleading green footer and offers no user control.

### 2. Reuse one shared sandbox activation path for startup and `/docker use`

The startup selection flow should hand the chosen sandbox name to a shared helper that performs inspect, tag validation, start-if-stopped, config updates, and persistent shell handoff. `/docker use` should continue using the same activation helper.

Why:
- The existing `/docker use` path already encodes the right container-state contract.
- Sharing one helper avoids startup-only drift and duplicate Docker parsing logic.
- Tests can target one activation function instead of two similar code paths.

Alternatives considered:
- Reimplement a startup-only version of `/docker use`. Rejected because it would duplicate container inspection/start logic and likely diverge again.

### 3. Do not mark the sandbox active until activation succeeds

The startup path must not populate the active sandbox state or show the green footer until the selected sandbox has passed inspection and, if needed, has been started successfully. If activation fails, startup should report the failure and leave the session in the no-sandbox state.

Why:
- The core bug is state/UI drift: a name-only selection currently looks active.
- Leaving the state empty on failure naturally keeps the footer honest without inventing a separate startup-only flag.

Alternatives considered:
- Continue storing the selected name and add a second “running” flag just for the footer. Rejected because it leaves two partially overlapping notions of active sandbox in the runtime.

## Risks / Trade-offs

- [Startup prompt adds one extra interactive step] → Limit it to the current auto-select path and allow the user to skip selection for that launch.
- [Refactoring `/docker use` into a shared helper could introduce regressions] → Keep focused tests around manual `/docker use` behavior and add startup-selection coverage that exercises the same helper.
- [Docker inspect/list calls at startup may add a small delay] → Reuse the existing lightweight list/inspect helpers and only inspect sandboxes when prompting is actually needed.

## Migration Plan

No data migration is required.

Rollout steps:
1. Replace the startup auto-select helper with an interactive chooser that gathers available tagged sandboxes and allows skip/cancel behavior for that launch.
2. Extract or reuse a shared activation helper so startup and `/docker use` both start stopped sandboxes before selecting them.
3. Add tests that cover prompted selection, skipped selection, stopped sandbox startup, and startup failure leaving the footer/state unset.

Rollback strategy:
- Restore the previous name-only auto-select helper if the prompt or activation refactor causes unacceptable startup regressions.

## Open Questions

- None.
