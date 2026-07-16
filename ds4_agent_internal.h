#ifndef DS4_AGENT_INTERNAL_H
#define DS4_AGENT_INTERNAL_H

#include "ds4.h"
#include "ds4_web.h"
#include "ds4_agent_subagent.h"

#include <limits.h>
#include <pthread.h>
#include <poll.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

#define AGENT_ASK_QUESTION_TEXT_MAX 2048
#define AGENT_ASK_QUESTION_CHOICE_MAX 256
#define AGENT_ASK_QUESTION_MAX_CHOICES 16
#define AGENT_ASK_QUESTION_ANSWER_MAX 4096

#define AGENT_SKILL_NAME_MAX 256
#define AGENT_SKILL_DESC_MAX 1024
#define AGENT_SKILL_PATH_MAX PATH_MAX

typedef struct agent_skill {
    char name[AGENT_SKILL_NAME_MAX];
    char description[AGENT_SKILL_DESC_MAX];
    char path[AGENT_SKILL_PATH_MAX];
    struct agent_skill *next;
} agent_skill;

typedef struct agent_skill_registry {
    pthread_mutex_t mu;
    agent_skill *head;
    int count;
    unsigned refs;
    uint64_t generation;
} agent_skill_registry;

typedef struct agent_worker agent_worker;

/* Agent tool list */
typedef enum {
    AGENT_TOOL_READ,
    AGENT_TOOL_MORE,
    AGENT_TOOL_WRITE,
    AGENT_TOOL_LIST,
    AGENT_TOOL_EDIT,
    AGENT_TOOL_SEARCH,
    AGENT_TOOL_WEB_BROWSE,
    AGENT_TOOL_WEB_FETCH,
    AGENT_TOOL_BASH,
    AGENT_TOOL_BASH_STATUS,
    AGENT_TOOL_BASH_STOP,
    AGENT_TOOL_MKDIR,
    AGENT_TOOL_SKILL_LIST,
    AGENT_TOOL_COUNT
} agent_tool_registry;

/* Agent tool classes for prompt building and meta-tool expansion. */
typedef enum {
    AGENT_TOOLS_CLASS_READ,   /* read, more, list, search */
    AGENT_TOOLS_CLASS_WRITE,  /* read, more, list, search, write, edit, mkdir */
    AGENT_TOOLS_CLASS_WEB,    /* web_browse, web_fetch */
    AGENT_TOOLS_CLASS_BASH,   /* bash, bash_status, bash_stop */
    AGENT_TOOLS_CLASS_COUNT
} agent_tools_class;

/* Map from agent_tools_class to the set of concrete tool indices included
 * in that class.  The first element is the count of tools in the class,
 * followed by the indices. */
static const int agent_tools_class_indices[AGENT_TOOLS_CLASS_COUNT][8] = {
    [AGENT_TOOLS_CLASS_READ]  = {4, AGENT_TOOL_READ, AGENT_TOOL_MORE, AGENT_TOOL_LIST, AGENT_TOOL_SEARCH},
    [AGENT_TOOLS_CLASS_WRITE] = {7, AGENT_TOOL_READ, AGENT_TOOL_MORE, AGENT_TOOL_LIST, AGENT_TOOL_SEARCH, AGENT_TOOL_WRITE, AGENT_TOOL_EDIT, AGENT_TOOL_MKDIR},
    [AGENT_TOOLS_CLASS_WEB]   = {2, AGENT_TOOL_WEB_BROWSE, AGENT_TOOL_WEB_FETCH},
    [AGENT_TOOLS_CLASS_BASH]  = {3, AGENT_TOOL_BASH, AGENT_TOOL_BASH_STATUS, AGENT_TOOL_BASH_STOP},
};

typedef struct {
    bool allow_all;                          /* policy is "all" */
    bool allow_none;                         /* policy is "none" (or "off") */
    bool allowed[AGENT_TOOL_COUNT];          /* per-tool allow bits */
} ds4_agent_tool_policy;

/* Parse a tool-access string into a normalized policy.
 * Accepts: "all", "none", "off", comma-separated tool names.
 * Meta-tool expansions:
 *   "read"  -> read, more, list, search
 *   "write" -> read, more, list, search, write, edit, mkdir
 *   "web"   -> web_browse, web_fetch
 *   "bash"  -> bash, bash_status, bash_stop
 * Returns 0 on success, -1 on parse error (unknown tool name).
 */
int ds4_agent_tool_policy_parse(const char *input, ds4_agent_tool_policy *out);

/* Return a human-readable summary of the policy (e.g. "all", "none",
 * "read, write, bash") into buf (len >= 256). */
void ds4_agent_tool_policy_format(const ds4_agent_tool_policy *pol, char *buf, size_t len);

/* Check whether a concrete tool name is allowed by the policy. */
bool ds4_agent_tool_policy_allows(const ds4_agent_tool_policy *pol, const char *tool_name);

typedef struct {
    const char *prompt;
    const char *system;
    const char *trace_path;
    int n_predict;
    int ctx_size;
    float temperature;
    float top_p;
    float min_p;
    uint64_t seed;
    ds4_think_mode think_mode;
} agent_generation_options;

typedef struct {
    char **v;
    int len;
    int cap;
} agent_path_list;

typedef struct {
    char *path;
    uint64_t last_read_at;
    uint64_t last_write_at;
} agent_temp_file;

typedef struct {
    agent_temp_file *v;
    int len;
    int cap;
} agent_temp_file_list;

typedef struct {
    ds4_engine_options engine;
    agent_generation_options gen;
    const char *chdir_path;
    const char *docker_command;
    const char *docker_container;
    bool docker_available;
    bool docker_debug;
    char launch_working_directory[PATH_MAX];
    agent_path_list working_directory_args;
    agent_path_list working_directories;
    agent_path_list skill_dirs;
    agent_skill_registry *skill_registry;
    char temp_directory[PATH_MAX];
    char web_cdp_host[256];
    int web_cdp_port;
    const char *recover_session;
    bool non_interactive;
    bool strict_sandbox;
    bool docker_auto;
    bool command_output;
    volatile bool preserve_agent_files;
    char default_tools[256];
} agent_config;

typedef enum {
    AGENT_WORKER_IDLE,
    AGENT_WORKER_WAITING_MODEL,
    AGENT_WORKER_PREFILL,
    AGENT_WORKER_GENERATING,
    AGENT_WORKER_COMPACTING,
    AGENT_WORKER_DRAINING,
    AGENT_WORKER_SAVING,
    AGENT_WORKER_ERROR,
    AGENT_WORKER_STOPPED,
} agent_worker_state;

typedef struct {
    agent_worker_state state;
    int prefill_done;
    int prefill_total;
    unsigned prefill_label;
    double prefill_tps;
    int generated;
    double gen_tps;
    bool greedy_sampling;
    ds4_think_mode think_mode;
    int ctx_used;
    int ctx_size;
    int power_percent;
    uint64_t session_id;
    char session_name[DS4_AGENT_SUBAGENT_NAME_MAX];
    int background_sessions;
    int unread_sessions;
    char error[256];
    char workspace[PATH_MAX];
    char docker_container[256];
    char tool_permissions[5];
    char writable_workspace_paths[2048];
    char writable_temp_paths[1024];
    char writable_auto_paths[2048];
} agent_status;

typedef struct agent_bash_job agent_bash_job;

typedef struct {
    int stdin_fd;
    int stdout_fd;
    pid_t pid;
    unsigned long long seq;
    pthread_mutex_t mu;
    bool active;
} agent_docker_shell;

struct agent_worker {
    ds4_engine *engine;
    agent_config *cfg;
    ds4_session *session;
    ds4_tokens transcript;
    char *cache_dir;
    char *sysprompt_path;
    char session_sha[41];
    char *session_title;
    uint64_t session_created_at;
    char *legacy_session_path_to_delete;
    bool user_activity;
    bool session_dirty;
    pthread_t thread;
    pthread_mutex_t mu;
    pthread_cond_t cond;
    int wake_fd[2];
    FILE *trace;
    bool wake_pending;
    bool stop;
    bool interrupt;
    bool initialized;
    bool save_requested;
    bool compact_requested;
    bool power_requested;
    int requested_power;
    int progress_base;
    double progress_started_at;
    int last_system_prompt_reminder_at;
    char *cmd_text;
    agent_status status;
    char *out;
    size_t out_len;
    size_t out_cap;
    ds4_web *web;
    bool web_approval_pending;
    bool web_approval_answered;
    bool web_approval_result;
    char web_approval_message[256];
    char web_approval_error[160];
    bool path_approval_pending;
    bool path_approval_answered;
    bool path_approval_result;
    char path_approval_message[PATH_MAX + 256];
    char path_approval_options[3][PATH_MAX];
    int path_approval_option_count;
    char path_approval_choice[PATH_MAX];
    char path_approval_error[160];
    bool question_pending;
    bool question_answered;
    bool question_interrupted;
    char question_message[AGENT_ASK_QUESTION_TEXT_MAX];
    char question_choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX];
    int question_choice_count;
    char *question_answer;
    char question_error[160];
    bool queued_user_drain_pending;
    bool queued_user_drain_answered;
    char *queued_user_drain_text;
    bool datetime_context_injected;
    char more_path[PATH_MAX];
    int more_next_line;
    bool more_bare;
    bool more_valid;
    char docker_mount_fingerprint[4096];
    agent_bash_job *bash_jobs;
    int next_bash_job_id;
    agent_path_list working_directories;
    agent_path_list auto_allowed_paths;
    agent_temp_file_list temp_files;
    bool raw_mode_needs_restore;
    agent_docker_shell docker_shell;
    pthread_mutex_t *model_gate;
    int model_tool_round_budget;
    int model_tool_round_used;
    bool subagents_disabled;
    char autonomy_stop_reason[DS4_AGENT_SUBAGENT_TEXT_MAX];
    uint64_t session_slot_id;
    char session_slot_name[DS4_AGENT_SUBAGENT_NAME_MAX];
    int background_sessions;
    int unread_sessions;
    ds4_agent_tool_policy tool_policy;
    agent_skill_registry *skill_registry;
    uint64_t skills_prompt_generation;
};

#define DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET (-1)

typedef struct {
    char *ptr;
    size_t len;
    size_t cap;
    bool truncated;
} agent_buf;

typedef struct {
    char **v;
    size_t len;
    size_t cap;
} agent_prompt_queue;

void *xmalloc(size_t n);
void *xrealloc(void *ptr, size_t n);
char *xstrdup(const char *s);
char *xstrndup(const char *s, size_t n);

void agent_path_list_append(agent_path_list *list, const char *path);
void agent_path_list_free(agent_path_list *list);

void agent_buf_append_full(agent_buf *b, const char *s, size_t n);
void agent_buf_puts(agent_buf *b, const char *s);
char *agent_buf_take(agent_buf *b);

void agent_prompt_queue_push(agent_prompt_queue *q, const char *text);
char *agent_prompt_queue_pop(agent_prompt_queue *q);
void agent_prompt_queue_push_front(agent_prompt_queue *q, char *text);
char *agent_prompt_queue_take_all(agent_prompt_queue *q);
const char *agent_prompt_queue_peek(const agent_prompt_queue *q);
void agent_prompt_queue_free(agent_prompt_queue *q);

bool agent_slash_command_with_args(const char *cmd, const char *name);
void agent_format_ctx_size(int ctx_size, char *buf, size_t len);
bool agent_prompt_yes_no(const char *prompt);

int agent_worker_init(agent_worker *w, ds4_engine *engine, agent_config *cfg);
void agent_worker_free(agent_worker *w);
bool agent_worker_needs_save(agent_worker *w);
bool agent_worker_save_session(agent_worker *w, char *err, size_t err_len);

bool worker_is_idle(agent_worker *w);
void worker_get_status(agent_worker *w, agent_status *status);
bool worker_submit(agent_worker *w, const char *text);
void worker_interrupt(agent_worker *w);
bool worker_check_raw_mode_restore(agent_worker *w);
void worker_consume(agent_worker *w, char **out, size_t *out_len,
                    agent_status *status);
bool worker_take_web_approval_request(agent_worker *w, char *message,
                                      size_t message_len);
void worker_answer_web_approval(agent_worker *w, bool allow, const char *err);
bool worker_take_path_approval_request(agent_worker *w, char *message,
                                       size_t message_len,
                                       char options[3][PATH_MAX],
                                       int *option_count);
void worker_answer_path_approval(agent_worker *w, bool allow,
                                 const char *choice, const char *err);
bool worker_take_question_request(agent_worker *w,
                                  char *message,
                                  size_t message_len,
                                  char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                  int *choice_count);
void worker_answer_question(agent_worker *w,
                            bool interrupted,
                            const char *answer,
                            const char *err);
bool worker_take_queued_user_drain_request(agent_worker *w);
void worker_answer_queued_user_drain(agent_worker *w, char *text);
void drain_wake_fd(int fd);

int ds4_agent_subagents_create_for_agent(ds4_agent_subagents **out,
                                        ds4_engine *engine,
                                        const agent_config *cfg);
agent_worker *ds4_agent_subagents_active_worker(ds4_agent_subagents *mgr);
void ds4_agent_subagents_update_worker_metadata(ds4_agent_subagents *mgr);
agent_prompt_queue *ds4_agent_subagents_active_queue(ds4_agent_subagents *mgr);
size_t ds4_agent_subagents_worker_count(ds4_agent_subagents *mgr);
int ds4_agent_subagents_worker_fd_at(ds4_agent_subagents *mgr, size_t worker_idx);
void ds4_agent_subagents_drain_wake_fds(ds4_agent_subagents *mgr,
                                       struct pollfd *pfd,
                                       size_t count);
bool ds4_agent_subagents_check_raw_mode_restore(ds4_agent_subagents *mgr);
void ds4_agent_subagents_drain_outputs(ds4_agent_subagents *mgr,
                                      char **active_out,
                                      size_t *active_out_len,
                                      agent_status *active_status,
                                      char **notifications);
char *ds4_agent_subagents_take_active_replay(ds4_agent_subagents *mgr);
bool ds4_agent_subagents_take_web_approval(ds4_agent_subagents *mgr,
                                          ds4_agent_subagent_id *id,
                                          char *msg,
                                          size_t msg_len);
void ds4_agent_subagents_answer_web_approval(ds4_agent_subagents *mgr,
                                            ds4_agent_subagent_id id,
                                            bool allow,
                                            const char *err);
bool ds4_agent_subagents_take_path_approval(ds4_agent_subagents *mgr,
                                           ds4_agent_subagent_id *id,
                                           char *msg,
                                           size_t msg_len,
                                           char options[3][PATH_MAX],
                                           int *option_count);
void ds4_agent_subagents_answer_path_approval(ds4_agent_subagents *mgr,
                                             ds4_agent_subagent_id id,
                                             bool allow,
                                             const char *choice,
                                             const char *err);
bool ds4_agent_subagents_take_question(ds4_agent_subagents *mgr,
                                       ds4_agent_subagent_id *id,
                                       char *msg,
                                       size_t msg_len,
                                       char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                       int *choice_count);
void ds4_agent_subagents_answer_question(ds4_agent_subagents *mgr,
                                         ds4_agent_subagent_id id,
                                         bool interrupted,
                                         const char *answer,
                                         const char *err);
bool ds4_agent_subagents_take_queued_user_drain(ds4_agent_subagents *mgr);
void ds4_agent_subagents_submit_ready(ds4_agent_subagents *mgr);
bool ds4_agent_subagents_handle_command(ds4_agent_subagents *mgr,
                                       char *cmd,
                                       bool busy);

/* --- Skill system API --- */
int agent_skill_register(agent_worker *w, const char *path);
int agent_skill_register_dir(agent_worker *w, const char *dir);
int agent_skills_init(agent_worker *w);
agent_skill *agent_skill_find(agent_worker *w, const char *name);
void agent_skill_delete(agent_worker *w, const char *name);
void agent_skill_list_all(agent_worker *w, void (*cb)(const agent_skill *s, void *ctx), void *ctx);
char *agent_build_skills_prompt(agent_worker *w);
bool agent_skill_name_valid(const char *name);
int agent_skill_parse_file(const char *path, char *name_out, size_t name_size,
                           char *desc_out, size_t desc_size);

#ifdef DS4_AGENT_TEST
extern int agent_test_failures;
void agent_test_assert(bool cond, const char *expr,
                       const char *file, int line);
void ds4_agent_subagent_unit_tests_run(void);
#define AGENT_TEST_ASSERT(expr) \
    agent_test_assert((expr), #expr, __FILE__, __LINE__)
#endif

#endif
