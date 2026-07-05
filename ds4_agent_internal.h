#ifndef DS4_AGENT_INTERNAL_H
#define DS4_AGENT_INTERNAL_H

#include "ds4.h"
#include "ds4_web.h"
#include "ds_agent_subagent.h"

#include <limits.h>
#include <pthread.h>
#include <poll.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

typedef struct agent_worker agent_worker;

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
    ds4_engine_options engine;
    agent_generation_options gen;
    const char *chdir_path;
    const char *docker_build;
    const char *docker_command;
    const char *docker_container;
    const char *docker_image;
    bool docker_available;
    bool docker_debug;
    char launch_working_directory[PATH_MAX];
    agent_path_list working_directory_args;
    agent_path_list working_directories;
    char temp_directory[PATH_MAX];
    char web_cdp_host[256];
    int web_cdp_port;
    const char *recover_session;
    bool non_interactive;
    bool strict_sandbox;
    bool docker_auto;
    bool command_output;
    bool docker_allow_one_shot;
    volatile bool preserve_agent_files;
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
    char session_name[DS_AGENT_SUBAGENT_NAME_MAX];
    int background_sessions;
    int unread_sessions;
    char error[256];
    char workspace[PATH_MAX];
    char docker_container[256];
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
    bool raw_mode_needs_restore;
    agent_docker_shell docker_shell;
    pthread_mutex_t *model_gate;
    int model_tool_round_budget;
    int model_tool_round_used;
    bool subagents_disabled;
    char autonomy_stop_reason[DS_AGENT_SUBAGENT_TEXT_MAX];
    uint64_t session_slot_id;
    char session_slot_name[DS_AGENT_SUBAGENT_NAME_MAX];
    int background_sessions;
    int unread_sessions;
};

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
bool worker_take_queued_user_drain_request(agent_worker *w);
void worker_answer_queued_user_drain(agent_worker *w, char *text);
void drain_wake_fd(int fd);

int ds_agent_subagents_create_for_agent(ds_agent_subagents **out,
                                        ds4_engine *engine,
                                        const agent_config *cfg);
agent_worker *ds_agent_subagents_active_worker(ds_agent_subagents *mgr);
void ds_agent_subagents_update_worker_metadata(ds_agent_subagents *mgr);
agent_prompt_queue *ds_agent_subagents_active_queue(ds_agent_subagents *mgr);
size_t ds_agent_subagents_worker_count(ds_agent_subagents *mgr);
int ds_agent_subagents_worker_fd_at(ds_agent_subagents *mgr, size_t worker_idx);
void ds_agent_subagents_drain_wake_fds(ds_agent_subagents *mgr,
                                       struct pollfd *pfd,
                                       size_t count);
bool ds_agent_subagents_check_raw_mode_restore(ds_agent_subagents *mgr);
void ds_agent_subagents_drain_outputs(ds_agent_subagents *mgr,
                                      char **active_out,
                                      size_t *active_out_len,
                                      agent_status *active_status,
                                      char **notifications);
char *ds_agent_subagents_take_active_replay(ds_agent_subagents *mgr);
bool ds_agent_subagents_take_web_approval(ds_agent_subagents *mgr,
                                          ds_agent_subagent_id *id,
                                          char *msg,
                                          size_t msg_len);
void ds_agent_subagents_answer_web_approval(ds_agent_subagents *mgr,
                                            ds_agent_subagent_id id,
                                            bool allow,
                                            const char *err);
bool ds_agent_subagents_take_path_approval(ds_agent_subagents *mgr,
                                           ds_agent_subagent_id *id,
                                           char *msg,
                                           size_t msg_len,
                                           char options[3][PATH_MAX],
                                           int *option_count);
void ds_agent_subagents_answer_path_approval(ds_agent_subagents *mgr,
                                             ds_agent_subagent_id id,
                                             bool allow,
                                             const char *choice,
                                             const char *err);
bool ds_agent_subagents_take_queued_user_drain(ds_agent_subagents *mgr);
void ds_agent_subagents_submit_ready(ds_agent_subagents *mgr);
bool ds_agent_subagents_handle_command(ds_agent_subagents *mgr,
                                       char *cmd,
                                       bool busy);

#ifdef DS4_AGENT_TEST
extern int agent_test_failures;
void agent_test_assert(bool cond, const char *expr,
                       const char *file, int line);
void ds_agent_subagent_unit_tests_run(void);
#define AGENT_TEST_ASSERT(expr) \
    agent_test_assert((expr), #expr, __FILE__, __LINE__)
#endif

#endif
