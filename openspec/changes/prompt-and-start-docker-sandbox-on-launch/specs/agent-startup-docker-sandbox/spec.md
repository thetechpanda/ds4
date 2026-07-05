## ADDED Requirements

### Requirement: Interactive startup asks which sandbox to use
When `ds4-agent` starts in the normal interactive Docker-auto path and tagged Docker sandboxes are available, the agent SHALL ask the user which sandbox to use for that launch instead of silently selecting the first sandbox name.

#### Scenario: Available sandboxes at interactive startup
- **WHEN** Docker support is available, auto-selection is enabled, no sandbox was explicitly provided on the command line, and one or more `ds4:sandbox` containers exist
- **THEN** `ds4-agent` prompts the user to choose a sandbox before treating any sandbox as active

#### Scenario: User skips sandbox selection
- **WHEN** the startup prompt is shown and the user declines to choose a sandbox for that launch
- **THEN** `ds4-agent` continues startup without an active sandbox

### Requirement: Confirmed startup sandbox must be running before activation
When the user confirms a sandbox at startup, `ds4-agent` SHALL inspect that sandbox and start it if it is stopped before making it the active sandbox for the session.

#### Scenario: Selected sandbox is already running
- **WHEN** the user chooses a tagged sandbox whose container state is already `running`
- **THEN** `ds4-agent` activates that sandbox for the session without starting a different container

#### Scenario: Selected sandbox is stopped
- **WHEN** the user chooses a tagged sandbox whose container state is not `running`
- **THEN** `ds4-agent` starts that sandbox and only activates it after the start succeeds

#### Scenario: Sandbox start fails
- **WHEN** `ds4-agent` cannot start the sandbox chosen at startup
- **THEN** it reports the activation failure and leaves the session without an active sandbox

### Requirement: Startup sandbox indicator must match activation state
`ds4-agent` SHALL only present the sandbox as active in startup-visible state, including the footer, after sandbox activation has succeeded.

#### Scenario: Startup activation succeeds
- **WHEN** a chosen sandbox has been validated and is running
- **THEN** the footer shows that sandbox as active for the session

#### Scenario: No active sandbox after startup choice
- **WHEN** the user skips selection or sandbox activation fails
- **THEN** the footer remains in the no-sandbox state instead of showing a green active sandbox indicator
