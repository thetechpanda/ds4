#ifndef DS4_AGENT_SUBAGENT_H
#define DS4_AGENT_SUBAGENT_H

#include "ds4.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define DS4_AGENT_SUBAGENT_NAME_MAX 64
#define DS4_AGENT_SUBAGENT_TEXT_MAX 512
#define DS4_AGENT_SUBAGENT_ERROR_MAX 256

typedef struct ds4_agent_subagents ds4_agent_subagents;

typedef struct {
    uint64_t value;
} ds4_agent_subagent_id;

typedef enum {
    DS4_AGENT_SUBAGENT_AUTONOMY_TAB = 0,
    DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND = 1,
    DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS = 2,
} ds4_agent_subagent_autonomy;

typedef enum {
    DS4_AGENT_SUBAGENT_EVENT_NONE = 0,
    DS4_AGENT_SUBAGENT_EVENT_CREATED,
    DS4_AGENT_SUBAGENT_EVENT_OUTPUT,
    DS4_AGENT_SUBAGENT_EVENT_STATUS,
    DS4_AGENT_SUBAGENT_EVENT_APPROVAL,
    DS4_AGENT_SUBAGENT_EVENT_REPORT,
    DS4_AGENT_SUBAGENT_EVENT_CLOSED,
    DS4_AGENT_SUBAGENT_EVENT_ERROR,
} ds4_agent_subagent_event_type;

typedef enum {
    DS4_AGENT_SUBAGENT_STATE_IDLE = 0,
    DS4_AGENT_SUBAGENT_STATE_RUNNING,
    DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL,
    DS4_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED,
    DS4_AGENT_SUBAGENT_STATE_ERROR,
    DS4_AGENT_SUBAGENT_STATE_STOPPED,
} ds4_agent_subagent_state;

typedef struct {
    int default_context_size;
    int default_round_budget;
} ds4_agent_subagent_options;

typedef struct {
    const char *name;
    const char *prompt;
    ds4_agent_subagent_autonomy autonomy;
    ds4_think_mode think_mode;
    bool think_mode_set;
    const char *allowed_tools;
    int round_budget;
    const char *stop_conditions;
    const char *report_format;
} ds4_agent_subagent_create_request;

typedef struct {
    ds4_agent_subagent_id id;
    char name[DS4_AGENT_SUBAGENT_NAME_MAX];
    bool active;
    ds4_agent_subagent_state state;
    ds4_agent_subagent_autonomy autonomy;
    int ctx_used;
    int ctx_size;
    bool dirty;
    bool approval_blocked;
    int budget_used;
    int budget_limit;
    ds4_think_mode think_mode;
    bool report_available;
    char stop_reason[DS4_AGENT_SUBAGENT_TEXT_MAX];
    char tool_permissions[5];
    /* Worker metrics for footer display */
    int prefill_done;
    int prefill_total;
    double prefill_tps;
    double gen_tps;
    bool engine_owner;
} ds4_agent_subagent_status;

typedef struct {
    ds4_agent_subagent_event_type type;
    ds4_agent_subagent_id id;
    char name[DS4_AGENT_SUBAGENT_NAME_MAX];
    char text[DS4_AGENT_SUBAGENT_TEXT_MAX];
} ds4_agent_subagent_event;

int ds4_agent_subagents_create(ds4_agent_subagents **out,
                              ds4_engine *engine,
                              const ds4_agent_subagent_options *opt);
void ds4_agent_subagents_destroy(ds4_agent_subagents *mgr);
void ds4_agent_subagents_set_engine_owner(ds4_agent_subagents *mgr,
                                          uint64_t session_id);

int ds4_agent_subagent_create(ds4_agent_subagents *mgr,
                             const ds4_agent_subagent_create_request *req,
                             ds4_agent_subagent_id *out);
int ds4_agent_subagent_send(ds4_agent_subagents *mgr,
                            ds4_agent_subagent_id id,
                            const char *prompt);
int ds4_agent_subagent_stop(ds4_agent_subagents *mgr, ds4_agent_subagent_id id);
int ds4_agent_subagent_close(ds4_agent_subagents *mgr, ds4_agent_subagent_id id);
int ds4_agent_subagent_switch(ds4_agent_subagents *mgr, ds4_agent_subagent_id id,
                             char **out, size_t *out_len);
int ds4_agent_subagent_list(ds4_agent_subagents *mgr,
                           ds4_agent_subagent_status *out,
                           size_t cap,
                           size_t *len);
int ds4_agent_subagent_poll_event(ds4_agent_subagents *mgr,
                                  ds4_agent_subagent_event *out);
int ds4_agent_subagent_report(ds4_agent_subagents *mgr,
                              ds4_agent_subagent_id id,
                              char *buf,
                              size_t len);
int ds4_agent_subagent_import_report(ds4_agent_subagents *mgr,
                                     ds4_agent_subagent_id from,
                                     ds4_agent_subagent_id into);
const char *ds4_agent_subagents_last_error(const ds4_agent_subagents *mgr);

#endif
