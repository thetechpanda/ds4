#ifndef DS_AGENT_SUBAGENT_H
#define DS_AGENT_SUBAGENT_H

#include "ds4.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define DS_AGENT_SUBAGENT_NAME_MAX 64
#define DS_AGENT_SUBAGENT_TEXT_MAX 512
#define DS_AGENT_SUBAGENT_ERROR_MAX 256

typedef struct ds_agent_subagents ds_agent_subagents;

typedef struct {
    uint64_t value;
} ds_agent_subagent_id;

typedef enum {
    DS_AGENT_SUBAGENT_AUTONOMY_TAB = 0,
    DS_AGENT_SUBAGENT_AUTONOMY_BACKGROUND = 1,
    DS_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS = 2,
} ds_agent_subagent_autonomy;

typedef enum {
    DS_AGENT_SUBAGENT_EVENT_NONE = 0,
    DS_AGENT_SUBAGENT_EVENT_CREATED,
    DS_AGENT_SUBAGENT_EVENT_OUTPUT,
    DS_AGENT_SUBAGENT_EVENT_STATUS,
    DS_AGENT_SUBAGENT_EVENT_APPROVAL,
    DS_AGENT_SUBAGENT_EVENT_REPORT,
    DS_AGENT_SUBAGENT_EVENT_CLOSED,
    DS_AGENT_SUBAGENT_EVENT_ERROR,
} ds_agent_subagent_event_type;

typedef enum {
    DS_AGENT_SUBAGENT_STATE_IDLE = 0,
    DS_AGENT_SUBAGENT_STATE_RUNNING,
    DS_AGENT_SUBAGENT_STATE_WAITING_MODEL,
    DS_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED,
    DS_AGENT_SUBAGENT_STATE_ERROR,
    DS_AGENT_SUBAGENT_STATE_STOPPED,
} ds_agent_subagent_state;

typedef struct {
    int default_context_size;
    int default_round_budget;
} ds_agent_subagent_options;

typedef struct {
    const char *name;
    const char *prompt;
    ds_agent_subagent_autonomy autonomy;
    ds4_think_mode think_mode;
    bool think_mode_set;
    const char *allowed_tools;
    const char *write_policy;
    int round_budget;
    const char *stop_conditions;
    const char *report_format;
} ds_agent_subagent_create_request;

typedef struct {
    ds_agent_subagent_id id;
    char name[DS_AGENT_SUBAGENT_NAME_MAX];
    bool active;
    ds_agent_subagent_state state;
    ds_agent_subagent_autonomy autonomy;
    int ctx_used;
    int ctx_size;
    bool dirty;
    bool queued_output;
    size_t queued_output_bytes;
    bool approval_blocked;
    int budget_used;
    int budget_limit;
    ds4_think_mode think_mode;
    bool report_available;
    char stop_reason[DS_AGENT_SUBAGENT_TEXT_MAX];
} ds_agent_subagent_status;

typedef struct {
    ds_agent_subagent_event_type type;
    ds_agent_subagent_id id;
    char name[DS_AGENT_SUBAGENT_NAME_MAX];
    char text[DS_AGENT_SUBAGENT_TEXT_MAX];
} ds_agent_subagent_event;

int ds_agent_subagents_create(ds_agent_subagents **out,
                              ds4_engine *engine,
                              const ds_agent_subagent_options *opt);
void ds_agent_subagents_destroy(ds_agent_subagents *mgr);

int ds_agent_subagent_create(ds_agent_subagents *mgr,
                             const ds_agent_subagent_create_request *req,
                             ds_agent_subagent_id *out);
int ds_agent_subagent_send(ds_agent_subagents *mgr,
                           ds_agent_subagent_id id,
                           const char *prompt);
int ds_agent_subagent_stop(ds_agent_subagents *mgr, ds_agent_subagent_id id);
int ds_agent_subagent_close(ds_agent_subagents *mgr, ds_agent_subagent_id id);
int ds_agent_subagent_switch(ds_agent_subagents *mgr, ds_agent_subagent_id id);
int ds_agent_subagent_list(ds_agent_subagents *mgr,
                           ds_agent_subagent_status *out,
                           size_t cap,
                           size_t *len);
int ds_agent_subagent_poll_event(ds_agent_subagents *mgr,
                                 ds_agent_subagent_event *out);
int ds_agent_subagent_report(ds_agent_subagents *mgr,
                             ds_agent_subagent_id id,
                             char *buf,
                             size_t len);
int ds_agent_subagent_import_report(ds_agent_subagents *mgr,
                                    ds_agent_subagent_id from,
                                    ds_agent_subagent_id into);
const char *ds_agent_subagents_last_error(const ds_agent_subagents *mgr);

#endif
