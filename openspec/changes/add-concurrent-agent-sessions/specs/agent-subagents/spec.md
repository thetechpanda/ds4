## ADDED Requirements

### Requirement: Resident Agent Sessions
The agent SHALL support multiple resident agent sessions in one process while sharing a single loaded DS4 engine.

#### Scenario: Create resident subagent
- **WHEN** the user creates a subagent from an interactive agent session
- **THEN** the agent creates a new resident session with its own worker-owned `ds4_session`, transcript, status, output buffer, tool state, and persistence identity

#### Scenario: Main session remains loaded
- **WHEN** a subagent is created
- **THEN** the existing main session remains resident and can be resumed without loading its KV state from disk

### Requirement: Subagent Module Boundary
The subagent implementation SHALL live behind a dedicated `ds_agent_subagent.c` module with a narrow integration surface.

#### Scenario: New subagent code placement
- **WHEN** the subagent manager, session slot, autonomy, scheduling, output buffering, or FFI behavior is implemented
- **THEN** the implementation code is placed in `ds_agent_subagent.c` rather than expanding `ds4_agent.c`

#### Scenario: Agent runtime integration
- **WHEN** `ds4_agent.c` needs to use subagent behavior
- **THEN** it calls the subagent module API instead of depending on subagent internals

### Requirement: FFI-Safe Subagent API
The subagent module SHALL expose a clean C-compatible API suitable for FFI callers.

#### Scenario: External caller manages subagents
- **WHEN** an FFI caller uses the subagent API
- **THEN** it can create and destroy a manager, create subagents, send prompts, stop sessions, close sessions, list status, and consume events using opaque handles and plain C-compatible values

#### Scenario: Internal structures remain hidden
- **WHEN** an FFI caller includes the subagent API declarations
- **THEN** it does not need access to `agent_worker`, `agent_editor`, `agent_prompt_queue`, terminal rendering state, or other private `ds4_agent.c` implementation details

### Requirement: Independent Session Context
Each resident agent session SHALL maintain independent model context and conversation history.

#### Scenario: Submit prompt to subagent
- **WHEN** the user sends a prompt to a subagent
- **THEN** the prompt is appended only to that subagent's transcript and evaluated against that subagent's `ds4_session`

#### Scenario: Switch back to main session
- **WHEN** the user switches from a subagent back to the main session
- **THEN** the main session's transcript, KV state, dirty flag, and pending output are restored as the active view without inheriting subagent context

### Requirement: Background Subagent Execution
Subagents SHALL be able to continue work while another session is active.

#### Scenario: Background subagent generates output
- **WHEN** a background subagent produces model or tool output while another session is active
- **THEN** the agent records that output under the subagent and notifies the user without corrupting the active line editor

#### Scenario: Switch to background subagent
- **WHEN** the user switches to a subagent that produced background output
- **THEN** the agent makes that subagent the active session and presents its buffered output and current status

### Requirement: Autonomous Delegated Tasks
The agent SHALL support subagents that run delegated tasks autonomously according to an explicit mission envelope.

#### Scenario: Create autonomous subagent
- **WHEN** the user creates an autonomous subagent with a goal
- **THEN** the subagent receives a private mission envelope containing its name, goal, allowed tools, write policy, budget, stop conditions, and report format

#### Scenario: Autonomous subagent completes
- **WHEN** an autonomous subagent reaches done, blocked, interrupted, or budget-exhausted state
- **THEN** it stops its autonomous loop and records a report that can be viewed without switching sessions

### Requirement: Prompt Routing And Reports
The agent SHALL route prompts, queues, and reports according to explicit session ownership.

#### Scenario: Ordinary prompt routes to active session
- **WHEN** the user submits an ordinary prompt
- **THEN** the prompt is queued or submitted only for the active session

#### Scenario: Directed prompt routes to named subagent
- **WHEN** the user sends a prompt with a directed subagent command
- **THEN** the prompt is queued or submitted only for the addressed subagent

#### Scenario: Import subagent report
- **WHEN** the user imports a subagent report into the active session
- **THEN** the report is appended to the active session as delegated context without importing the subagent's full private transcript

### Requirement: Serialized Shared Engine Execution
The agent SHALL protect shared DS4 engine and backend inference execution from unsafe concurrent access.

#### Scenario: Two sessions request inference
- **WHEN** two resident sessions request model inference at the same time
- **THEN** the agent serializes calls that execute or mutate backend inference state until backend parallel execution is explicitly supported

#### Scenario: Tool work outside model execution
- **WHEN** a subagent is waiting on file, bash, Docker, or web tool work that does not execute the model
- **THEN** other sessions may continue non-conflicting work without waiting for that tool operation to finish

### Requirement: Session-Scoped Tool State
Tool state SHALL remain scoped to the session that created it unless explicitly designed as global state.

#### Scenario: Subagent starts bash job
- **WHEN** a subagent starts a bash job
- **THEN** job status, stop requests, output files, and compaction reminders are associated with that subagent and not with the active main session

#### Scenario: Subagent requests approval
- **WHEN** a background subagent needs user approval for web or path access
- **THEN** the approval request identifies the requesting subagent and blocks only that subagent until answered or interrupted

### Requirement: Backward Compatible Single-Session Mode
Existing single-session agent behavior SHALL remain the default when no subagent commands are used.

#### Scenario: Existing commands without subagents
- **WHEN** the user runs `/new`, `/switch`, `/save`, `/compact`, `/list`, or submits ordinary prompts without creating subagents
- **THEN** the agent preserves the existing single active session behavior

#### Scenario: Existing saved session switch
- **WHEN** the user runs the existing saved-session switch command
- **THEN** the agent loads the requested saved session according to existing persistence semantics unless the user invokes an explicit subagent load operation
