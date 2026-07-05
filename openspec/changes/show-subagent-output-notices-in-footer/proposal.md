## Why

Background subagents currently report queued output by printing transcript notices such as `[subagent <name>] <bytes> bytes of background output queued`. That makes passive status updates noisy and easy to miss during active work, even though the information is better suited to the persistent bottom status area.

## What Changes

- Replace transcript-style background-output notices for subagents with compact bottom-bar badges.
- Show one badge per relevant subagent in the footer using the format `[s:<name> <bytes> <status>]`.
- Surface the cumulative queued background-output bytes for each subagent, rather than only a generic unread indicator.
- Show the subagent's current state in the badge using footer-friendly status text such as idle, working, waiting, approval, error, or stopped.
- Keep the footer compact by treating subagent badges as status metadata instead of full transcript events.

## Capabilities

### New Capabilities
- `subagent-footer-status`: Show per-subagent queued-output and state badges in the bottom status area so background activity is visible without adding transcript noise.

### Modified Capabilities

## Impact

- Affected code is centered on subagent output/event handling and interactive footer rendering, especially `ds_agent_subagent.c`, `ds_agent_subagent.h`, and the footer-building path in `ds4_agent.c`.
- User-visible interactive behavior changes in the agent TUI/footer, but no external API or dependency changes are expected.
