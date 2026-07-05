## 1. Footer Badge Data And Formatting

- [x] 1.1 Add a footer-side subagent badge formatter that reads per-subagent queued-output state from the subagent manager or `ds_agent_subagent_list()`.
- [x] 1.2 Implement badge eligibility so any subagent with `queued_output_bytes > 0`, including the active subagent, is included in footer badge rendering.
- [x] 1.3 Implement badge text formatting as `[s:<name> <bytes> <status>]` with footer label mapping for idle, working, waiting, approval, error, and stopped states.
- [x] 1.4 Implement status precedence so approval-blocked subagents render `approval` even when they also have queued output and an execution state.
- [x] 1.5 Integrate the badge suffix into the existing footer-building path without changing queued prompt preview behavior or introducing new badge-specific styling.
- [x] 1.6 Add bounded footer truncation for queued-output badges so only complete badges render in stable subagent-list order and overflow becomes a compact truncation marker.

## 2. Transcript And Notification Behavior

- [x] 2.1 Remove the passive transcript notice that reports only queued background-output byte growth for subagents.
- [x] 2.2 Preserve existing replay behavior when switching to a subagent with queued output so footer badges complement, rather than replace, replay visibility.
- [x] 2.3 Keep actionable transcript-visible subagent alerts, such as error or stopped notifications, separate from passive queued-output badge rendering.

## 3. Verification

- [x] 3.1 Add or update unit tests for subagent status extraction and footer badge formatting, including active-subagent badges, approval precedence, and zero-byte omission.
- [x] 3.2 Add or update footer-width tests to verify whole-badge truncation without partial badge rendering or wrapping.
- [x] 3.3 Add or update coverage for removing passive queued-output transcript notices while preserving separate error or stopped notifications.
- [x] 3.4 Run the relevant test target(s) for the agent/subagent footer path and confirm the new footer badge behavior matches the spec.
