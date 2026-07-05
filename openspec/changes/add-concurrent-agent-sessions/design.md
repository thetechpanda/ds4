## Context

The current agent runtime is intentionally single-process and single-session: the UI thread owns terminal input/output, while one worker thread owns the live `ds4_session`, transcript, tool state, Docker shell, and status. Existing `/switch` support is persistence-oriented; it loads a saved KV file into the single live worker and requires the model to be idle.

The core DS4 API already separates `ds4_engine` from `ds4_session`. The engine owns loaded model resources, while each session owns a mutable inference timeline with KV/cache, logits, checkpoint state, callbacks, and backend graph buffers. That makes resident multi-session support feasible without changing the model loading boundary, but it does not prove backend calls are reentrant.

## Goals / Non-Goals

**Goals:**
- Keep one loaded `ds4_engine` and allow multiple resident `ds4_session` instances in memory.
- Let the user create subagents that can work independently from the main session.
- Preserve each session's transcript, KV state, tool jobs, Docker/web/tool approvals, status, dirty flag, and save identity.
- Allow switching the active terminal view between resident sessions without saving and reloading from disk.
- Allow background subagents to continue work while the user returns to the main session.
- Serialize model execution through an agent-side scheduler or lock until backend parallelism is explicitly validated.
- Put subagent implementation code in `ds_agent_subagent.c`, with `ds4_agent.c` using the module through a narrow API.
- Provide a clean C ABI that can be called from FFI without depending on terminal UI structures.
- Preserve the existing single-session path as the default behavior.

**Non-Goals:**
- True simultaneous GPU/CPU model execution across sessions in the first implementation.
- A new public `ds4.h` multi-session API.
- A multi-pane TUI, remote orchestration protocol, or distributed subagent system.
- Changing the saved-session KV file format unless needed for metadata display.

## Subagent Workflow

Subagents should behave like autonomous background tabs with an explicit task contract. A user can create a named subagent with a goal, let it run while the main session remains active, inspect its status, switch into it, or import its final report back into the main session.

Example workflow:

```
> /subagent new tests "Run the agent tests and diagnose any failures"
created subagent tests; running in background

> continue editing the main design
main remains the active session

> /subagent list
* main   generating     ctx 12k/64k  dirty
  tests  tool-running   ctx 4k/64k   unread

> /subagent switch tests
tests becomes active and buffered output is shown

> /subagent switch main
main becomes active with its original context intact
```

Ordinary prompts route to the active session. `/subagent send <name> <prompt>` routes to a named background session. `/subagent report <name>` shows the subagent's final result without switching; `/subagent import <name>` appends that report to the active session as delegated context.

Each autonomous subagent should start with a mission envelope in its private transcript:

```
Subagent name: tests
Goal: Run the agent tests and diagnose failures.
Allowed tools: read, search, bash.
Write policy: no file edits unless explicitly allowed.
Budget: stop after the configured tool/model round limit.
Stop condition: done, blocked, interrupted, or budget exhausted.
Report format: concise result with evidence.
```

## Decisions

### Use an agent manager that owns session slots

Add an `agent_manager` in `ds4_agent.c` that owns the shared `ds4_engine *`, shared configuration, active slot id, a collection of `agent_session_slot` records, and a shared model execution gate. Each slot owns one `agent_worker` and a display identity such as `main`, `sub1`, or a user-supplied name.

Alternative considered: keep one worker and swap `ds4_session` snapshots in memory. That reduces worker refactoring but preserves the current single active execution model, makes background subagents impossible, and reintroduces snapshot/restore costs on every switch.

### Put subagent implementation behind `ds_agent_subagent.c`

All new subagent manager, slot, autonomy, scheduling, background-output, and FFI implementation should live in `ds_agent_subagent.c`. `ds4_agent.c` should keep only integration points: constructing the module, routing interactive commands, polling module wake fds, rendering active output, and shutting the module down.

The FFI-facing API should use opaque handles and plain C types. It should not expose `agent_worker`, `agent_config`, `agent_editor`, `agent_prompt_queue`, or terminal rendering internals. A caller should be able to create a subagent manager around an existing `ds4_engine`, create/send/stop/list sessions, consume events, and destroy the manager.

Sketch:

```
typedef struct ds_agent_subagents ds_agent_subagents;
typedef struct ds_agent_subagent_id { uint64_t value; } ds_agent_subagent_id;

int ds_agent_subagents_create(ds_agent_subagents **out, ds4_engine *engine, const ds_agent_subagent_options *opt);
void ds_agent_subagents_destroy(ds_agent_subagents *mgr);
int ds_agent_subagent_create(ds_agent_subagents *mgr, const ds_agent_subagent_create *req, ds_agent_subagent_id *out);
int ds_agent_subagent_send(ds_agent_subagents *mgr, ds_agent_subagent_id id, const char *prompt);
int ds_agent_subagent_stop(ds_agent_subagents *mgr, ds_agent_subagent_id id);
int ds_agent_subagent_close(ds_agent_subagents *mgr, ds_agent_subagent_id id);
int ds_agent_subagent_poll_event(ds_agent_subagents *mgr, ds_agent_subagent_event *out);
```

Alternative considered: keep subagent code in `ds4_agent.c` as static helpers. That would be faster to land, but it would entangle the terminal runtime with the reusable subagent API and make FFI a later extraction problem.

### Preserve worker ownership by making workers slot-local

Keep the existing invariant that only a worker thread mutates its own `ds4_session` and transcript. The manager routes UI commands, submissions, interrupts, approvals, and output consumption to the correct worker rather than directly editing worker-owned state.

Alternative considered: centralize all session mutation in the manager. That would require a larger rewrite of turn execution, tool loops, cancellation, compaction, and save behavior, and would weaken the current thread ownership model.

### Serialize model execution first

Introduce a manager-owned mutex or scheduler gate for calls that execute the model or mutate backend inference state, including `ds4_session_sync()`, `ds4_session_eval()`, speculative eval, and session power changes. Tool I/O can continue concurrently outside that gate.

Alternative considered: run session inference fully parallel because sessions own separate graph/KV buffers. This may work on some backends, but CUDA/Metal/ROCm code contains global caches, command queues, streaming state, and engine-level mutable settings. The first implementation should choose correctness and predictable behavior over unverified parallelism.

### Route terminal output by active session

The active session renders normally through the existing editor path. Background sessions buffer output and publish compact notifications when they produce new model/tool text, request approval, finish, fail, or need user attention. Switching sessions drains the selected slot's buffered transcript/output into the terminal and updates the footer.

Alternative considered: print all session output immediately with prefixes. That is simpler, but it would fight the existing linenoise/status-footer rendering and make background subagents disruptive during interactive editing.

### Keep persistence semantics session-local

`/save`, `/compact`, `/history`, `/new`, and dirty-session checks operate on the active slot by default. Existing disk-backed `/switch <sha>` continues to load a saved session into the active slot or into a new named slot when explicitly requested by a subagent command.

Alternative considered: make `/switch` always switch resident slots and add a different command for saved sessions. That is more surprising for existing users and risks breaking muscle memory.

### Add explicit subagent commands

Add slash commands under a narrow namespace such as:
- `/subagent new [name] [prompt]`
- `/subagent list`
- `/subagent switch <id|name>`
- `/subagent send <id|name> <prompt>`
- `/subagent stop <id|name>`
- `/subagent close <id|name>`
- `/subagent report <id|name>`
- `/subagent import <id|name>`

The exact command spelling can be adjusted during implementation, but subagent behavior should remain opt-in and not overload existing saved-session commands.

### Define autonomy as a task contract

A subagent is not autonomous merely because it has a resident session. Autonomy comes from a mission envelope plus a run policy: tool permissions, write policy, round budget, stop conditions, and report behavior. The first implementation should support background-assistant and autonomous-worker modes, but should not allow subagents to spawn other subagents.

Alternative considered: let every subagent run as a full agent with unrestricted tools. That is powerful, but too blurry for a first version and risky for file edits, Docker jobs, and approval prompts.

## Risks / Trade-offs

- Memory growth per subagent -> expose live token/context status per slot, make close/stop commands obvious, and avoid creating background sessions implicitly.
- Model execution can starve one session while another is in long prefill -> scheduler should report queued/inference states and allow interruption; future work can add fairer token-level scheduling.
- Backend reentrancy assumptions are wrong -> first implementation serializes all model execution and adds tests around the scheduler boundary.
- Background approval prompts can block a worker -> route approval requests to the active UI with clear session identity and keep the blocked subagent visible in `/subagent list`.
- Shared configuration fields can leak between sessions -> copy or isolate per-slot mutable config where needed, and treat engine-global settings such as power as global in UI/status.
- Docker shell sharing can cross-contaminate jobs -> keep Docker shell and bash job lists slot-local unless a deliberate shared-sandbox mode is added later.
- FFI users can depend on unstable internal structs -> expose only opaque handles, stable value structs, explicit ownership rules, and string/error buffers controlled by the API.

## Migration Plan

1. Add `ds_agent_subagent.c` and its narrow declaration/FFI surface.
2. Add manager and slot scaffolding while keeping a single default `main` slot.
3. Move the existing interactive and non-interactive code paths onto module accessors that return or act on the active worker.
4. Add the model execution gate around DS4 session inference calls.
5. Add subagent lifecycle commands, autonomy modes, report/import behavior, and status/list rendering.
6. Add tests for the FFI-facing API, slot creation, active switching, output isolation, dirty-session checks, and model-gate serialization.
7. Keep rollback simple: the `main` slot should still exercise the old one-worker behavior when no subagent commands are used.

## Open Questions

- Should background output replay show the full buffered model/tool output or a compact tail by default when switching into a subagent?
- Should `/subagent new name prompt` immediately submit the prompt, or should creation and submission remain separate commands?
- Should saved-session `/switch` load into the active slot only, or should there be an explicit `/subagent load <sha>` convenience?
- Should the FFI declaration live in a new `ds_agent_subagent.h`, or should it be embedded in an existing agent-facing header while keeping implementation in `ds_agent_subagent.c`?
