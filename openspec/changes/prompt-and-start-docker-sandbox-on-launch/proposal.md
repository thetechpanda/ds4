## Why

`ds4-agent` currently detects Docker availability at startup and silently selects the first tagged sandbox name, but it does not ensure that container is running before treating it as active. That leaves users with a green sandbox footer even when the selected sandbox has not been started, which is misleading and breaks the expected startup contract.

## What Changes

- Replace silent startup auto-selection with an explicit startup prompt that asks which available Docker sandbox to use.
- After the user confirms a sandbox, start it if it is stopped before making it the active sandbox for the session.
- Keep the startup footer and sandbox state aligned so `ds4-agent` only presents a sandbox as active after selection and activation succeed.
- Preserve existing non-interactive and explicitly configured startup flows unless the user is in the normal interactive Docker-auto path.

## Capabilities

### New Capabilities
- `agent-startup-docker-sandbox`: Defines interactive startup selection of Docker sandboxes and the requirement that a chosen sandbox be running before the agent treats it as active.

### Modified Capabilities
- None.

## Impact

- Affected code: `ds4_agent.c` startup flow, Docker sandbox listing and inspection helpers, sandbox selection/activation logic, and footer/status updates.
- Affected UX: interactive `ds4-agent` startup when Docker support is available and one or more tagged sandboxes exist.
- Verification: agent tests should cover prompting, selecting, skipping, and starting a stopped sandbox before the footer reports it as active.
