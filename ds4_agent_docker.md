# ds4_agent Docker Call Graph

This note maps the Docker-specific helpers in `ds4_agent.c` for agent coding.
There are two execution styles:

- Persistent shell: one worker-owned `docker exec -i <container> /bin/sh` used by
  read-like filesystem commands. Commands are delimited with sentinel lines.
- Direct exec: a one-shot Docker CLI process used for stdin-streaming writes,
  bash jobs, and sandbox management commands.

## Reading From Docker

```mermaid
flowchart TD
    A[Model tool call] --> B[agent_tool_dispatch]
    B --> C{tool}

    C -->|read range or whole file| D[agent_read_range]
    D --> E{agent_tool_use_docker_filesystem}
    E -->|sandbox active, not whole file| F[agent_docker_read_range]
    F --> G[agent_docker_shell_exec_argv: awk range]

    E -->|sandbox active, whole file| H[agent_docker_read_file_bytes]
    H --> I[agent_docker_shell_exec_argv: cat file]

    C -->|edit before patch| J[agent_tool_edit]
    J --> E

    C -->|list/search| K["agent_tool_list / agent_tool_search"]
    K --> E
    E -->|sandbox active, list/search command| G

    G --> L[agent_docker_shell_exec]
    I --> L
    L --> M[agent_docker_shell_find_complete_sentinel]
    L --> N[agent_docker_shell_find_sentinel_candidate]
    L --> O[agent_docker_shell_sentinel_tail_len]
    L --> P[agent_docker_shell_emit_output]
    P --> Q[agent_buf result]
    Q --> R[tool observation]

    S["agent_worker_init or /docker use/create"] --> T[agent_docker_shell_start]
    T --> L
    U["/docker stop/use/create or worker free"] --> V[agent_docker_shell_stop]
```

Read variants:

- Ranged `read`: `agent_docker_read_range()` runs `awk` in the persistent shell
  so only the requested line window crosses back to the host.
- Whole-file `read` and edit pre-read: `agent_docker_read_file_bytes()` runs
  `cat` with unbounded capture so large files are not capped by the normal tool
  output buffer.
- `list` and `search`: use the same persistent shell execution layer so broad
  filesystem scans stay in the container.

## Writing To Docker

```mermaid
flowchart TD
    A[Model tool call] --> B[agent_tool_dispatch]
    B --> C{tool}

    C -->|write| D[agent_tool_write]
    C -->|edit| E[agent_tool_edit]
    D --> FS{agent_tool_use_docker_filesystem}
    E --> RS{agent_tool_use_docker_filesystem}
    RS -->|sandbox active, pre-read| F[agent_docker_read_file_bytes: pre-read]
    F --> G[apply edit on host memory]
    G --> FS
    FS -->|sandbox active, write bytes| H[agent_docker_write_file_bytes]

    H --> I[agent_docker_shell_exec_argv: test -d parent]
    I --> J[agent_docker_shell_exec]
    H --> K[agent_docker_exec: docker exec -i]
    K --> L["sh -c cat-stdin-to-target"]
    L --> M[file bytes streamed on stdin]
    M --> N[sandbox file updated]

    O["/workspace add/remove"] --> P[agent_docker_refresh_mounts]
    P --> Q[agent_docker_mount_fingerprint]
    P --> R[agent_docker_list_sandbox_names]
    P --> S[agent_docker_inspect_sandboxes_sync]
    P --> T[agent_docker_capture: stop/rm/run/create]
    T --> U[agent_docker_exec]

    V["/docker create"] --> W[agent_command_docker_create]
    W --> U
    W --> X[agent_docker_shell_stop/start]
    Y["/docker use"] --> Z[agent_command_docker_use]
    Z --> AA[agent_docker_inspect_sandbox]
    Z --> X
    AB["/docker stop"] --> AC[agent_command_docker_stop]
    AC --> U
    AC --> AD[agent_docker_shell_stop]
    AE["/docker destroy"] --> AF[agent_command_docker_destroy]
    AF --> AG[agent_docker_capture: rm]
    AG --> U
```

Write variants:

- File write: `agent_docker_write_file_bytes()` first validates the parent
  directory through the persistent shell, then switches to direct `docker exec`
  for the actual byte stream. This avoids interpolating file contents into a
  shell command and keeps large stdin/stdout traffic from deadlocking.
- Edit write: `agent_tool_edit()` reads the current sandbox file, computes the
  replacement in host memory, then writes through the same byte-streaming path.
- Mount and sandbox state writes: `/workspace` changes rebuild labeled sandbox
  containers through `agent_docker_refresh_mounts()`, while `/docker create`,
  `/docker use`, `/docker stop`, and `/docker destroy` use direct Docker CLI
  helpers and reset the persistent shell when the active container changes.
