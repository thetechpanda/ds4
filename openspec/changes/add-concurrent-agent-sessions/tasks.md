## 1. Manager And Session Slot Foundation

- [x] 1.1 Add `ds_agent_subagent.c` as the implementation home for subagent manager, slot, autonomy, scheduling, event, and output-buffering code.
- [x] 1.2 Add a small FFI-safe declaration surface for the subagent module using opaque handles, plain C-compatible request/status/event structs, and explicit ownership rules.
- [x] 1.3 Add `agent_session_slot` and subagent manager types that own a shared `ds4_engine *`, active slot id, slot list, and model execution gate.
- [x] 1.4 Move single-worker initialization in `run_agent()` behind subagent module helpers while preserving one default `main` slot.
- [x] 1.5 Add manager helpers for active worker lookup, slot lookup by id/name, slot creation, slot destruction, and manager shutdown.
- [x] 1.6 Ensure `agent_worker_free()` is called for every slot and that shutdown joins all worker threads without leaking session, Docker, web, or output resources.

## 2. Shared Engine Scheduling

- [x] 2.1 Add agent-side wrappers around `ds4_session_sync()`, `ds4_session_eval()`, speculative eval if used by the agent, and power updates.
- [x] 2.2 Wire worker turn execution and compaction through the wrappers so shared engine/backend execution is serialized.
- [x] 2.3 Keep file, bash, Docker, and web tool work outside the model execution gate unless the tool path calls back into model inference.
- [x] 2.4 Add status text for sessions waiting on the shared model gate.

## 3. Interactive Runtime Multiplexing

- [x] 3.1 Replace direct `worker` references in the interactive loop with active-worker manager accessors.
- [x] 3.2 Poll wake fds for all live slots and drain output from active and background workers without corrupting linenoise rendering.
- [x] 3.3 Buffer background output per slot and show compact active-session-safe notifications for background progress, completion, errors, and approval requests.
- [x] 3.4 Update prompt/footer rendering to include the active session name/id and visible background session indicators.
- [x] 3.5 Move prompt queues to session slots so ordinary queued prompts stay attached to the active session that received them.
- [x] 3.6 Keep the non-interactive path single-slot unless explicit subagent protocol support is added later.

## 4. Subagent Commands

- [x] 4.1 Add slash-command recognition and help text for `/subagent new`, `/subagent list`, `/subagent switch`, `/subagent send`, `/subagent stop`, `/subagent close`, `/subagent report`, and `/subagent import`.
- [x] 4.2 Implement `/subagent new [name] [prompt]` to create a resident slot with a mission envelope and optionally submit an initial prompt.
- [x] 4.3 Implement `/subagent list` with id/name, active marker, worker state, token usage, dirty flag, queued output, and approval-blocked status.
- [x] 4.4 Implement `/subagent switch <id|name>` to change the active slot and replay buffered output/status for that slot.
- [x] 4.5 Implement `/subagent send <id|name> <prompt>` to submit work to a specific idle subagent or report that it is busy.
- [x] 4.6 Implement `/subagent stop <id|name>` and `/subagent close <id|name>` with dirty-session save checks where needed.
- [x] 4.7 Implement `/subagent report <id|name>` and `/subagent import <id|name>` so the main session can inspect or import a subagent's final report without importing its full transcript.

## 5. Autonomous Task Policy

- [x] 5.1 Define subagent autonomy modes for tab-only, background assistant, and autonomous worker behavior.
- [x] 5.2 Add mission envelope construction with goal, allowed tools, write policy, model/tool round budget, stop conditions, and report format.
- [x] 5.3 Make autonomous workers stop on done, blocked, interrupted, or budget-exhausted states and persist a report event.
- [x] 5.4 Prevent subagents from spawning other subagents in the first implementation.
- [x] 5.5 Expose autonomy mode, stop reason, budget use, and report availability through the FFI-safe status/event API.

## 6. Session-Scoped Operations

- [x] 6.1 Keep `/save`, `/compact`, `/history`, `/new`, dirty-session checks, and ordinary prompt submission scoped to the active slot.
- [x] 6.2 Preserve existing saved-session `/switch <sha>` behavior for the active slot.
- [x] 6.3 Route web and path approval prompts with requesting-session identity and answer only the requesting worker.
- [x] 6.4 Keep bash jobs, Docker shell state, auto-allowed paths, and tool output files slot-local.
- [x] 6.5 Keep global settings such as power and shared engine options visibly global or explicitly synchronize them across slots.

## 7. Tests And Verification

- [x] 7.1 Add unit coverage for manager slot creation, lookup, active switching, and cleanup through the subagent module API.
- [x] 7.2 Add FFI/API-level tests for create, send, stop, close, list/status, event polling, and report/import behavior.
- [x] 7.3 Add tests proving two resident workers keep independent transcripts, dirty flags, output buffers, prompt queues, and tool state.
- [x] 7.4 Add tests proving the model execution gate serializes concurrent inference requests.
- [x] 7.5 Add tests for background output notification and switch-time replay behavior.
- [x] 7.6 Add command parser tests for the `/subagent` command family and backwards compatibility for existing slash commands.
- [x] 7.7 Run `make ds4_agent_test` and `./ds4_agent_test`.
