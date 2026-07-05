## 1. Startup Selection Flow

- [x] 1.1 Replace the startup name-only Docker auto-select path with an interactive chooser for available tagged sandboxes in the normal interactive launch flow
- [x] 1.2 Preserve non-interactive and explicit `--docker-container` startup behavior so the new prompt only appears in the Docker-auto path

## 2. Shared Sandbox Activation

- [x] 2.1 Extract or reuse a shared helper that inspects a named sandbox, validates the `ds4:sandbox` label, starts it when stopped, and only then marks it active
- [x] 2.2 Route both startup selection and `/docker use` through the same activation helper to keep sandbox state transitions consistent

## 3. Status And Verification

- [x] 3.1 Ensure startup state and footer rendering only show a green sandbox after activation succeeds
- [x] 3.2 Add focused tests for startup prompting, skip behavior, selecting a running sandbox, selecting a stopped sandbox, and activation failure leaving no active sandbox
