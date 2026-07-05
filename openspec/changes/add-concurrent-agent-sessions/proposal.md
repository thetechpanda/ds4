## Why

`ds4_agent.c` can save and restore conversations, but the live runtime only owns one active `agent_worker` and one mutable `ds4_session` at a time. Subagents need independent resident context so the user can delegate work without overwriting the main session or paying disk restore costs for every switch.

## What Changes

- Introduce resident in-memory agent sessions so two or more `ds4_session` instances can coexist under one loaded `ds4_engine`.
- Add subagent lifecycle operations for creating, listing, switching to, interrupting, and disposing independent sessions.
- Keep each subagent's transcript, KV state, tool state, output buffer, status, and identity independent from the main session.
- Place subagent implementation code behind a new `ds_agent_subagent.c` module with a clean C ABI suitable for FFI callers.
- Use a scheduler or shared inference lock for model execution until backend-level parallel inference is proven safe.
- Preserve existing `/new`, `/switch`, `/save`, `/list`, tool execution, Docker sandbox, and non-interactive behavior unless the user explicitly invokes subagent features.

## Capabilities

### New Capabilities
- `agent-subagents`: Defines resident concurrent agent sessions, active-session switching, background subagent execution, and the safety contract for sharing one loaded DS4 engine.

### Modified Capabilities
- None.

## Impact

- Affected code: new `ds_agent_subagent.c` module, likely a small declaration surface for FFI consumers, and `ds4_agent.c` integration points for worker ownership, interactive runtime loop, slash-command dispatch, status/footer rendering, session persistence integration, output routing, and worker tests.
- Affected engine/session APIs: primarily uses existing `ds4_engine` and `ds4_session` boundaries; may add a narrow agent-side scheduler/lock rather than changing core tensor internals.
- Affected runtime behavior: memory usage increases with each live subagent because each resident session owns its KV/graph state.
- Compatibility: existing single-session operation remains the default path.
