## Context

The interactive agent currently surfaces background subagent output in two different ways:

- The footer shows only coarse session metadata such as `session: <name>#<id> | bg unread`.
- Inactive subagent output also emits transcript notices like `[subagent <name>] <bytes> bytes of background output queued`.

That split makes background activity both noisy and under-specified. The information the user cares about is persistent state, not a stream event: which subagent has unread output, how much output is queued, and what that subagent is doing now.

The existing code already has most of the required data:

- `ds_agent_subagent_list()` can enumerate all subagents and includes `name`, `state`, `queued_output`, and `queued_output_bytes`.
- `build_status_text()` and `build_footer_text()` already own the compact bottom-bar rendering path.
- `ds_agent_subagents_drain_outputs()` is where transcript notices are currently produced for background output.

This change is small in scope but crosses the subagent manager and footer renderer, so it benefits from a short design before implementation.

## Goals / Non-Goals

**Goals:**

- Move background subagent output visibility from transcript notices to persistent footer badges.
- Show badges in the requested compact format: `[s:<name> <bytes> <status>]`.
- Use cumulative queued background bytes per subagent so the badge reflects current unread volume.
- Use footer-friendly status labels that are stable and concise.
- Keep the footer readable when multiple subagents are present.

**Non-Goals:**

- Redesign the main footer status layout beyond the subagent metadata area.
- Change subagent lifecycle, queueing behavior, or replay behavior when switching sessions.
- Introduce a general multi-row status system for subagents.
- Remove all subagent transcript events; error/stopped events may still remain transcript-visible if they are treated as actionable alerts rather than passive background status.

## Decisions

### 1. Footer badges should be built from subagent-manager status, not embedded into `agent_status`

The active-worker `agent_status` struct only carries flattened summary fields such as `background_sessions` and `unread_sessions`. It does not model per-subagent names or queued byte counts.

We will keep `agent_status` focused on the active worker and let the footer query richer subagent state separately. The practical shape is that the footer-building path receives either:

- the `ds_agent_subagents` manager directly, or
- a preformatted footer badge suffix built from `ds_agent_subagent_list()`.

Rationale:

- The footer is rendering session-manager state, not just active-worker state.
- The richer per-subagent data already exists in `ds_agent_subagent_list()`.
- This avoids widening `agent_status` with arrays or badge-specific fields that do not belong to worker status snapshots.

Alternatives considered:

- Extend `agent_status` with per-subagent badge data.
  Rejected because it mixes active-worker telemetry with cross-session UI state and complicates status copying.
- Continue using only `background_sessions` / `unread_sessions`.
  Rejected because it cannot express the requested `[s:<name> <bytes> <status>]` behavior.

### 2. Badge eligibility should include any subagent with queued replay/output state, including the active subagent

The proposal is specifically about surfacing queued subagent output in the footer. The implementation should therefore render badges for any subagent where `queued_output_bytes > 0`, even if that subagent is currently active.

Rationale:

- This is the closest replacement for the existing transcript message while still covering replay-visible edge cases.
- It preserves footer space by not showing every idle background tab.
- It answers the main user question: who has unread output, and how much?

Alternatives considered:

- Restrict badges to inactive/background sessions only.
  Rejected because the user explicitly wants queued replay state to remain visible even for the active subagent.
- Show all non-idle subagents.
  Rejected for the first pass because it broadens the feature from “replace notices” to “full subagent monitor”.
- Show all background subagents at all times.
  Rejected because it wastes horizontal space and makes the footer noisy again.

### 3. Footer labels should map internal states to short UI words, with approval taking priority

Internal subagent states are currently `idle`, `running`, `waiting-model`, `approval`, `error`, and `stopped`. The footer should use a small, user-facing vocabulary:

- `idle` -> `idle`
- `running` -> `working`
- `waiting-model` -> `waiting`
- `approval` -> `approval`
- `error` -> `error`
- `stopped` -> `stopped`

If a subagent is approval-blocked and also has queued bytes, the badge should surface `approval` rather than a more generic activity label. Approval is a user-blocking state and should take precedence over `working` or `waiting`.

Rationale:

- `working` matches the requested wording better than `running`.
- `waiting-model` is too long and too implementation-specific for a compact badge.
- Approval is actionable and should be surfaced before less urgent execution-state wording.
- The footer should prefer human-scannable labels over canonical internal enum names.

Alternatives considered:

- Reuse internal strings unchanged.
  Rejected because `waiting-model` is awkward in a compact badge and `running` is less user-friendly than `working`.

### 4. Background-output transcript notices should be removed, but alert-style transcript lines may remain

The passive notice `[subagent <name>] <bytes> bytes of background output queued` should stop being appended to transcript notifications once the footer badges exist.

However, terminal events such as worker errors or explicit stop states may continue to emit transcript lines if they remain operationally useful.

Rationale:

- Passive queue-growth information becomes redundant once it is always visible in the footer.
- Error and stop events are qualitatively different because they may require immediate user attention.

Alternatives considered:

- Remove all subagent transcript notifications.
  Rejected because the proposal only justifies moving passive queued-output notices.

### 5. Footer width pressure should be handled with bounded badge rendering

The footer already has truncation logic for writable-path metadata and queue previews. Subagent badges should follow the same principle: render as many complete badges as fit, then truncate the suffix rather than wrapping the status line.

Expected behavior:

- Build badges in a stable order based on the subagent list order.
- Append complete badges until the horizontal budget is exhausted.
- If at least one additional badge cannot fit, append a compact truncation marker such as ` ...`.

Rationale:

- Multi-row footer expansion would complicate redraw behavior.
- Rendering partial badges would be harder to scan than omitting the tail.
- Stable ordering reduces visual churn between redraws.

### 6. Badge styling should stay plain text except for existing footer styling

The first version should not introduce special colors or emphasis rules for unread badges beyond whatever styling the footer already applies to status text.

Rationale:

- The feature goal is better information architecture, not a broader visual redesign.
- Plain-text badges are easier to reason about while the width/truncation behavior is being established.
- This keeps the implementation aligned with the existing footer styling model.

Alternatives considered:

- Add dedicated unread badge colors immediately.
  Rejected because the user prefers the first version to stay within existing footer styling.

Alternatives considered:

- Allow the footer to wrap across multiple lines.
  Rejected because the footer is redrawn frequently and currently treats the status row as compact, predictable UI.
- Show only the “largest” or “most recent” badge.
  Rejected because it hides information unexpectedly and makes the footer feel unstable.

## Risks / Trade-offs

- [Footer crowding when many subagents have unread output] -> Limit badges to subagents with queued bytes and truncate after full badges.
- [State string mismatch between internal terms and footer labels] -> Keep a dedicated footer-label mapping instead of reusing internal enum names.
- [Rendering cost on frequent redraws] -> Reuse `ds_agent_subagent_list()` data and keep formatting linear in subagent count.
- [Losing visibility of background output in plain transcript logs] -> Preserve explicit replay on session switch; only passive queue notices move to the footer.
- [Ambiguity about whether stopped/error sessions should keep badges after output is replayed] -> Tie badge visibility to queued bytes, not historical state, for the initial implementation.
- [Approval-blocked sessions being visually lost among ordinary working badges] -> Make `approval` the highest-priority footer state label whenever approval is pending.

## Migration Plan

There is no data migration or rollout sequencing requirement for this change.

Implementation can proceed in three safe steps:

1. Add a footer-side formatter that turns subagent status items into compact badge text.
2. Thread that badge text into the existing footer-building path.
3. Remove the passive background-output transcript notice once the footer rendering is in place.

Rollback is straightforward: restore the existing transcript notice and remove the badge suffix from the footer.

## Open Questions

- Should the eventual spec describe the approval-priority rule as a general badge-state precedence order, or only as a special case for approval vs working/waiting?
