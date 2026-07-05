## ADDED Requirements

### Requirement: Footer shows queued subagent output badges
The interactive agent footer SHALL show a compact badge for each subagent whose queued output byte count is greater than zero. Badge visibility SHALL be based on queued output state, not on whether the subagent is active or inactive.

#### Scenario: Background subagent has queued output
- **WHEN** an inactive subagent accumulates background output and its queued output byte count becomes greater than zero
- **THEN** the footer shows a badge for that subagent

#### Scenario: Active subagent has queued replay state
- **WHEN** the active subagent has queued output byte count greater than zero
- **THEN** the footer shows a badge for that active subagent

#### Scenario: Subagent has no queued output
- **WHEN** a subagent's queued output byte count is zero
- **THEN** the footer does not show a queued-output badge for that subagent

### Requirement: Footer badges use compact queued-output text
Each queued-output badge SHALL use the format `[s:<name> <bytes> <status>]`, where `<name>` is the subagent name, `<bytes>` is the cumulative queued output byte count, and `<status>` is a footer-friendly state label.

#### Scenario: Queued-output badge is rendered
- **WHEN** the footer renders a badge for a subagent with queued output
- **THEN** the badge text includes the subagent name, cumulative queued byte count, and footer state label in the format `[s:<name> <bytes> <status>]`

#### Scenario: Footer-friendly status labels are used
- **WHEN** a badge is rendered for a subagent state of idle, running, waiting-model, approval-blocked, error, or stopped
- **THEN** the badge uses the footer label `idle`, `working`, `waiting`, `approval`, `error`, or `stopped` respectively

### Requirement: Approval state takes precedence in badge status
If a subagent both has queued output and is blocked on an approval request, the queued-output badge SHALL display `approval` as the status label instead of a less urgent execution label such as `working` or `waiting`.

#### Scenario: Approval-blocked subagent also has queued output
- **WHEN** a subagent has queued output byte count greater than zero and is blocked on a pending approval request
- **THEN** the footer badge for that subagent displays `approval` as its status label

### Requirement: Footer badge styling remains within existing footer styling
Queued-output badges SHALL remain plain text except for the existing footer styling already applied by the interactive status area. The system MUST NOT introduce dedicated unread-badge color treatment in this change.

#### Scenario: Queued-output badge is visible
- **WHEN** the footer renders one or more queued-output badges
- **THEN** the badges appear using plain text within the existing footer styling model

### Requirement: Footer truncates queued-output badges by whole badge
When the footer does not have enough horizontal space to render all queued-output badges, the footer SHALL append as many complete badges as fit in stable subagent-list order and then show a compact truncation marker instead of wrapping or rendering partial badges.

#### Scenario: All badges fit in the footer
- **WHEN** the footer has enough width for every queued-output badge
- **THEN** the footer renders all badges in stable subagent-list order

#### Scenario: Not all badges fit in the footer
- **WHEN** additional queued-output badges would exceed the footer width budget
- **THEN** the footer renders only complete badges that fit and indicates truncation without wrapping or rendering a partial badge

### Requirement: Passive queued-output transcript notices are removed
The system SHALL stop appending passive transcript notices that only report queued background-output byte growth once footer queued-output badges are available.

#### Scenario: Background output is queued for a subagent
- **WHEN** a subagent's queued output byte count increases because new background output is buffered
- **THEN** the transcript does not receive a passive notice that only reports queued output bytes for that subagent

#### Scenario: Error or stopped notifications remain separate
- **WHEN** a subagent enters an error or stopped state
- **THEN** the system may still emit transcript-visible alert lines for those states independently of queued-output badge rendering
