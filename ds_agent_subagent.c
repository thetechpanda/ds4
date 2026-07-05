#include "ds4_agent_internal.h"

#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct agent_session_slot {
    ds_agent_subagent_id id;
    char name[DS_AGENT_SUBAGENT_NAME_MAX];
    ds_agent_subagent_autonomy autonomy;
    agent_worker worker;
    bool has_worker;
    agent_config cfg;
    agent_prompt_queue queue;
    agent_buf background;
    bool unread;
    bool close_requested;
    bool report_available;
    char *report;
    char stop_reason[DS_AGENT_SUBAGENT_TEXT_MAX];
    int budget_limit;
    int budget_used;
} agent_session_slot;

struct ds_agent_subagents {
    ds4_engine *engine;
    pthread_mutex_t model_gate;
    agent_session_slot *slots;
    size_t len;
    size_t cap;
    uint64_t next_id;
    uint64_t active_id;
    ds_agent_subagent_event *events;
    size_t event_len;
    size_t event_cap;
    char last_error[DS_AGENT_SUBAGENT_ERROR_MAX];
    int default_ctx_size;
    int default_round_budget;
};

static const char *ds_agent_subagent_autonomy_name(ds_agent_subagent_autonomy mode) {
    switch (mode) {
    case DS_AGENT_SUBAGENT_AUTONOMY_TAB: return "tab";
    case DS_AGENT_SUBAGENT_AUTONOMY_BACKGROUND: return "background";
    case DS_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS: return "autonomous";
    default: return "unknown";
    }
}

static const char *ds_agent_subagent_think_mode_name(ds4_think_mode mode) {
    switch (mode) {
    case DS4_THINK_NONE: return "off";
    case DS4_THINK_HIGH: return "default";
    case DS4_THINK_MAX: return "max";
    default: return ds4_think_mode_name(mode);
    }
}

static bool ds_agent_subagent_parse_think_mode(const char *text,
                                               ds4_think_mode *out) {
    if (!text || !text[0] || !out) return false;
    if (!strcmp(text, "off")) {
        *out = DS4_THINK_NONE;
        return true;
    }
    if (!strcmp(text, "default")) {
        *out = DS4_THINK_HIGH;
        return true;
    }
    if (!strcmp(text, "max")) {
        *out = DS4_THINK_MAX;
        return true;
    }
    return false;
}

static void ds_agent_subagents_set_error(ds_agent_subagents *mgr,
                                         const char *fmt, ...) {
    if (!mgr) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(mgr->last_error, sizeof(mgr->last_error), fmt, ap);
    va_end(ap);
}

const char *ds_agent_subagents_last_error(const ds_agent_subagents *mgr) {
    if (!mgr || !mgr->last_error[0]) return "";
    return mgr->last_error;
}

static bool ds_agent_subagent_id_equal(ds_agent_subagent_id a,
                                       ds_agent_subagent_id b) {
    return a.value == b.value;
}

static agent_session_slot *ds_agent_subagents_slot_by_id(ds_agent_subagents *mgr,
                                                        ds_agent_subagent_id id) {
    if (!mgr || !id.value) return NULL;
    for (size_t i = 0; i < mgr->len; i++) {
        if (ds_agent_subagent_id_equal(mgr->slots[i].id, id))
            return &mgr->slots[i];
    }
    return NULL;
}

static agent_session_slot *ds_agent_subagents_slot_by_name(ds_agent_subagents *mgr,
                                                          const char *name) {
    if (!mgr || !name || !name[0]) return NULL;
    for (size_t i = 0; i < mgr->len; i++) {
        if (!strcmp(mgr->slots[i].name, name)) return &mgr->slots[i];
    }
    return NULL;
}

static agent_session_slot *ds_agent_subagents_active_slot(ds_agent_subagents *mgr) {
    if (!mgr) return NULL;
    return ds_agent_subagents_slot_by_id(mgr,
        (ds_agent_subagent_id){.value = mgr->active_id});
}

agent_worker *ds_agent_subagents_active_worker(ds_agent_subagents *mgr) {
    agent_session_slot *slot = ds_agent_subagents_active_slot(mgr);
    return slot && slot->has_worker ? &slot->worker : NULL;
}

void ds_agent_subagents_update_worker_metadata(ds_agent_subagents *mgr) {
    if (!mgr) return;
    int background = mgr->len > 0 ? (int)mgr->len - 1 : 0;
    int unread = 0;
    for (size_t i = 0; i < mgr->len; i++) {
        if (mgr->slots[i].id.value != mgr->active_id &&
            (mgr->slots[i].unread || mgr->slots[i].background.len > 0))
            unread++;
    }
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        pthread_mutex_lock(&slot->worker.mu);
        slot->worker.session_slot_id = slot->id.value;
        snprintf(slot->worker.session_slot_name,
                 sizeof(slot->worker.session_slot_name), "%s", slot->name);
        slot->worker.background_sessions = background;
        slot->worker.unread_sessions = unread;
        pthread_mutex_unlock(&slot->worker.mu);
    }
}

agent_prompt_queue *ds_agent_subagents_active_queue(ds_agent_subagents *mgr) {
    agent_session_slot *slot = ds_agent_subagents_active_slot(mgr);
    return slot ? &slot->queue : NULL;
}

static bool ds_agent_subagents_active_is_main(ds_agent_subagents *mgr) {
    agent_session_slot *slot = ds_agent_subagents_active_slot(mgr);
    return slot && !strcmp(slot->name, "main");
}

static agent_session_slot *ds_agent_subagents_resolve(ds_agent_subagents *mgr,
                                                     const char *spec) {
    if (!mgr || !spec || !spec[0]) return NULL;
    char *end = NULL;
    unsigned long long numeric = strtoull(spec, &end, 10);
    if (spec[0] && end && *end == '\0' && numeric > 0) {
        agent_session_slot *slot = ds_agent_subagents_slot_by_id(
            mgr, (ds_agent_subagent_id){.value = (uint64_t)numeric});
        if (slot) return slot;
    }
    return ds_agent_subagents_slot_by_name(mgr, spec);
}

static void ds_agent_subagents_push_event(ds_agent_subagents *mgr,
                                          ds_agent_subagent_event_type type,
                                          const agent_session_slot *slot,
                                          const char *text) {
    if (!mgr) return;
    if (mgr->event_len == mgr->event_cap) {
        mgr->event_cap = mgr->event_cap ? mgr->event_cap * 2 : 8;
        mgr->events = xrealloc(mgr->events,
                               mgr->event_cap * sizeof(mgr->events[0]));
    }
    ds_agent_subagent_event *ev = &mgr->events[mgr->event_len++];
    memset(ev, 0, sizeof(*ev));
    ev->type = type;
    if (slot) {
        ev->id = slot->id;
        snprintf(ev->name, sizeof(ev->name), "%s", slot->name);
    }
    snprintf(ev->text, sizeof(ev->text), "%s", text ? text : "");
}

int ds_agent_subagent_poll_event(ds_agent_subagents *mgr,
                                 ds_agent_subagent_event *out) {
    if (!mgr || !out || mgr->event_len == 0) return 0;
    *out = mgr->events[0];
    memmove(mgr->events, mgr->events + 1,
            (mgr->event_len - 1) * sizeof(mgr->events[0]));
    mgr->event_len--;
    return 1;
}

static void ds_agent_subagents_ensure_slot_cap(ds_agent_subagents *mgr) {
    if (mgr->len == mgr->cap) {
        mgr->cap = mgr->cap ? mgr->cap * 2 : 4;
        mgr->slots = xrealloc(mgr->slots, mgr->cap * sizeof(mgr->slots[0]));
    }
}

static const char *ds_agent_subagent_state_name(ds_agent_subagent_state state) {
    switch (state) {
    case DS_AGENT_SUBAGENT_STATE_IDLE: return "idle";
    case DS_AGENT_SUBAGENT_STATE_RUNNING: return "running";
    case DS_AGENT_SUBAGENT_STATE_WAITING_MODEL: return "waiting-model";
    case DS_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED: return "approval";
    case DS_AGENT_SUBAGENT_STATE_ERROR: return "error";
    case DS_AGENT_SUBAGENT_STATE_STOPPED: return "stopped";
    default: return "unknown";
    }
}

static ds_agent_subagent_state ds_agent_subagent_state_from_worker(
    const agent_session_slot *slot,
    const agent_status *st) {
    if (!slot || !slot->has_worker) return DS_AGENT_SUBAGENT_STATE_IDLE;
    if (st && (st->state == AGENT_WORKER_ERROR))
        return DS_AGENT_SUBAGENT_STATE_ERROR;
    if (st && st->state == AGENT_WORKER_STOPPED)
        return DS_AGENT_SUBAGENT_STATE_STOPPED;
    if (st && st->state == AGENT_WORKER_WAITING_MODEL)
        return DS_AGENT_SUBAGENT_STATE_WAITING_MODEL;
    if (st && (st->state == AGENT_WORKER_PREFILL ||
               st->state == AGENT_WORKER_GENERATING ||
               st->state == AGENT_WORKER_COMPACTING ||
               st->state == AGENT_WORKER_DRAINING ||
               st->state == AGENT_WORKER_SAVING))
        return DS_AGENT_SUBAGENT_STATE_RUNNING;
    if (slot->worker.web_approval_pending || slot->worker.path_approval_pending)
        return DS_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED;
    return DS_AGENT_SUBAGENT_STATE_IDLE;
}

static void ds_agent_subagent_config_copy(agent_config *dst,
                                          const agent_config *src) {
    memset(dst, 0, sizeof(*dst));
    if (src) *dst = *src;
    dst->working_directory_args.v = NULL;
    dst->working_directory_args.len = 0;
    dst->working_directory_args.cap = 0;
    dst->working_directories.v = NULL;
    dst->working_directories.len = 0;
    dst->working_directories.cap = 0;
    if (src) {
        for (int i = 0; i < src->working_directory_args.len; i++)
            agent_path_list_append(&dst->working_directory_args,
                                   src->working_directory_args.v[i]);
        for (int i = 0; i < src->working_directories.len; i++)
            agent_path_list_append(&dst->working_directories,
                                   src->working_directories.v[i]);
    }
}

static void ds_agent_subagent_config_free(agent_config *cfg) {
    if (!cfg) return;
    agent_path_list_free(&cfg->working_directory_args);
    agent_path_list_free(&cfg->working_directories);
    memset(cfg, 0, sizeof(*cfg));
}

static void ds_agent_subagent_slot_free(agent_session_slot *slot) {
    if (!slot) return;
    if (slot->has_worker) {
        agent_worker_free(&slot->worker);
        slot->has_worker = false;
    }
    agent_prompt_queue_free(&slot->queue);
    free(slot->background.ptr);
    free(slot->report);
    ds_agent_subagent_config_free(&slot->cfg);
    memset(slot, 0, sizeof(*slot));
}

int ds_agent_subagents_create(ds_agent_subagents **out,
                              ds4_engine *engine,
                              const ds_agent_subagent_options *opt) {
    if (!out) return -1;
    ds_agent_subagents *mgr = xmalloc(sizeof(*mgr));
    memset(mgr, 0, sizeof(*mgr));
    mgr->engine = engine;
    mgr->next_id = 1;
    mgr->default_ctx_size = opt ? opt->default_context_size : 0;
    mgr->default_round_budget = opt && opt->default_round_budget > 0 ?
        opt->default_round_budget : 16;
    pthread_mutex_init(&mgr->model_gate, NULL);
    *out = mgr;
    return 0;
}

int ds_agent_subagents_create_for_agent(ds_agent_subagents **out,
                                        ds4_engine *engine,
                                        const agent_config *cfg) {
    ds_agent_subagent_options opt = {
        .default_context_size = cfg ? cfg->gen.ctx_size : 0,
        .default_round_budget = 16,
    };
    if (ds_agent_subagents_create(out, engine, &opt) != 0) return -1;
    ds_agent_subagent_create_request req = {
        .name = "main",
        .autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    ds_agent_subagent_id id = {0};
    ds_agent_subagents *mgr = *out;
    ds_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = mgr->next_id++;
    slot->autonomy = req.autonomy;
    slot->budget_limit = mgr->default_round_budget;
    snprintf(slot->name, sizeof(slot->name), "%s", req.name);
    ds_agent_subagent_config_copy(&slot->cfg, cfg);
    if (agent_worker_init(&slot->worker, engine, &slot->cfg) != 0) {
        ds_agent_subagents_set_error(mgr, "failed to initialize main session");
        ds_agent_subagent_slot_free(slot);
        mgr->len--;
        return -1;
    }
    slot->has_worker = true;
    slot->worker.model_gate = &mgr->model_gate;
    slot->worker.model_tool_round_budget = 0;
    mgr->active_id = slot->id.value;
    id = slot->id;
    (void)id;
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_CREATED,
                                  slot, "created main session");
    return 0;
}

void ds_agent_subagents_destroy(ds_agent_subagents *mgr) {
    if (!mgr) return;
    for (size_t i = 0; i < mgr->len; i++)
        ds_agent_subagent_slot_free(&mgr->slots[i]);
    free(mgr->slots);
    free(mgr->events);
    pthread_mutex_destroy(&mgr->model_gate);
    free(mgr);
}

static void ds_agent_subagent_make_default_name(ds_agent_subagents *mgr,
                                                char *buf,
                                                size_t len) {
    unsigned n = 1;
    do {
        snprintf(buf, len, "sub%u", n++);
    } while (ds_agent_subagents_slot_by_name(mgr, buf));
}

static char *ds_agent_subagent_mission_envelope(
    const char *name,
    const char *goal,
    ds_agent_subagent_autonomy autonomy,
    const char *allowed_tools,
    const char *write_policy,
    int budget,
    const char *stop_conditions,
    const char *report_format) {
    agent_buf b = {0};
    agent_buf_puts(&b, "Subagent mission envelope\n\n");
    agent_buf_puts(&b, "Subagent name: ");
    agent_buf_puts(&b, name ? name : "");
    agent_buf_puts(&b, "\nGoal: ");
    agent_buf_puts(&b, goal && goal[0] ? goal : "Wait for directed work.");
    agent_buf_puts(&b, "\nAutonomy mode: ");
    agent_buf_puts(&b, ds_agent_subagent_autonomy_name(autonomy));
    agent_buf_puts(&b, "\nAllowed tools: ");
    agent_buf_puts(&b, allowed_tools && allowed_tools[0] ?
                   allowed_tools : "read, search, bash, web_fetch, web_browse");
    agent_buf_puts(&b, "\nWrite policy: ");
    agent_buf_puts(&b, write_policy && write_policy[0] ?
                   write_policy : "no file edits unless explicitly allowed");
    char tmp[96];
    snprintf(tmp, sizeof(tmp), "\nBudget: stop after %d model/tool rounds.",
             budget > 0 ? budget : 16);
    agent_buf_puts(&b, tmp);
    agent_buf_puts(&b, "\nStop conditions: ");
    agent_buf_puts(&b, stop_conditions && stop_conditions[0] ?
                   stop_conditions : "done, blocked, interrupted, or budget exhausted");
    agent_buf_puts(&b, "\nReport format: ");
    agent_buf_puts(&b, report_format && report_format[0] ?
                   report_format : "concise result with evidence");
    agent_buf_puts(&b, "\n\nDo not create other subagents. Work only inside this mission.");
    return agent_buf_take(&b);
}

int ds_agent_subagent_create(ds_agent_subagents *mgr,
                             const ds_agent_subagent_create_request *req,
                             ds_agent_subagent_id *out) {
    if (!mgr) return -1;
    const char *requested = req ? req->name : NULL;
    char generated[DS_AGENT_SUBAGENT_NAME_MAX];
    if (!requested || !requested[0]) {
        ds_agent_subagent_make_default_name(mgr, generated, sizeof(generated));
        requested = generated;
    }
    if (ds_agent_subagents_slot_by_name(mgr, requested)) {
        ds_agent_subagents_set_error(mgr, "subagent name already exists: %s",
                                     requested);
        return -1;
    }

    ds_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = mgr->next_id++;
    slot->autonomy = req ? req->autonomy : DS_AGENT_SUBAGENT_AUTONOMY_TAB;
    slot->budget_limit = req && req->round_budget > 0 ?
        req->round_budget : mgr->default_round_budget;
    snprintf(slot->name, sizeof(slot->name), "%s", requested);

    agent_session_slot *active = ds_agent_subagents_active_slot(mgr);
    const agent_config *base = active ?
        (active->has_worker ? active->worker.cfg : &active->cfg) : NULL;
    ds_agent_subagent_config_copy(&slot->cfg, base);
    if (req && req->think_mode_set)
        slot->cfg.gen.think_mode = req->think_mode;
    if (slot->cfg.gen.ctx_size <= 0 && mgr->default_ctx_size > 0)
        slot->cfg.gen.ctx_size = mgr->default_ctx_size;

    if (mgr->engine) {
        if (agent_worker_init(&slot->worker, mgr->engine, &slot->cfg) != 0) {
            ds_agent_subagents_set_error(mgr, "failed to initialize subagent %s",
                                         slot->name);
            ds_agent_subagent_slot_free(slot);
            mgr->len--;
            return -1;
        }
        slot->has_worker = true;
        slot->worker.model_gate = &mgr->model_gate;
        slot->worker.model_tool_round_budget =
            slot->autonomy == DS_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS ?
            slot->budget_limit : 0;
        slot->worker.subagents_disabled = true;
    }

    if (!mgr->active_id) mgr->active_id = slot->id.value;
    if (out) *out = slot->id;
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_CREATED,
                                  slot, "created subagent");
    if (req && req->prompt && req->prompt[0]) {
        char *mission = ds_agent_subagent_mission_envelope(
            slot->name, req->prompt, slot->autonomy, req->allowed_tools,
            req->write_policy, slot->budget_limit, req->stop_conditions,
            req->report_format);
        agent_prompt_queue_push(&slot->queue, mission);
        free(mission);
    }
    return 0;
}

int ds_agent_subagent_send(ds_agent_subagents *mgr,
                           ds_agent_subagent_id id,
                           const char *prompt) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    if (!prompt || !prompt[0]) {
        ds_agent_subagents_set_error(mgr, "empty prompt");
        return -1;
    }
    if (slot->has_worker && !worker_is_idle(&slot->worker)) {
        ds_agent_subagents_set_error(mgr, "subagent %s is busy", slot->name);
        return -1;
    }
    agent_prompt_queue_push(&slot->queue, prompt);
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "prompt queued");
    return 0;
}

int ds_agent_subagent_stop(ds_agent_subagents *mgr, ds_agent_subagent_id id) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    snprintf(slot->stop_reason, sizeof(slot->stop_reason), "interrupted");
    if (slot->has_worker) worker_interrupt(&slot->worker);
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "stop requested");
    return 0;
}

int ds_agent_subagent_close(ds_agent_subagents *mgr, ds_agent_subagent_id id) {
    if (!mgr) return -1;
    size_t idx = SIZE_MAX;
    for (size_t i = 0; i < mgr->len; i++) {
        if (ds_agent_subagent_id_equal(mgr->slots[i].id, id)) {
            idx = i;
            break;
        }
    }
    if (idx == SIZE_MAX) {
        ds_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    if (mgr->len == 1) {
        ds_agent_subagents_set_error(mgr, "cannot close the last session");
        return -1;
    }
    agent_session_slot closing = mgr->slots[idx];
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_CLOSED,
                                  &closing, "closed subagent");
    if (mgr->active_id == closing.id.value) {
        mgr->active_id = 0;
        for (size_t i = 0; i < mgr->len; i++) {
            if (i != idx) {
                mgr->active_id = mgr->slots[i].id.value;
                break;
            }
        }
    }
    ds_agent_subagent_slot_free(&mgr->slots[idx]);
    for (size_t i = idx + 1; i < mgr->len; i++)
        mgr->slots[i - 1] = mgr->slots[i];
    mgr->len--;
    if (mgr->len < mgr->cap)
        memset(&mgr->slots[mgr->len], 0, sizeof(mgr->slots[mgr->len]));
    return 0;
}

int ds_agent_subagent_switch(ds_agent_subagents *mgr, ds_agent_subagent_id id) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    mgr->active_id = id.value;
    slot->unread = false;
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "switched active session");
    return 0;
}

int ds_agent_subagent_list(ds_agent_subagents *mgr,
                           ds_agent_subagent_status *out,
                           size_t cap,
                           size_t *len) {
    if (!mgr) return -1;
    if (len) *len = mgr->len;
    size_t n = out && cap < mgr->len ? cap : mgr->len;
    for (size_t i = 0; out && i < n; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        agent_status st = {0};
        if (slot->has_worker) worker_get_status(&slot->worker, &st);
        memset(&out[i], 0, sizeof(out[i]));
        out[i].id = slot->id;
        snprintf(out[i].name, sizeof(out[i].name), "%s", slot->name);
        out[i].active = slot->id.value == mgr->active_id;
        out[i].state = ds_agent_subagent_state_from_worker(slot, &st);
        out[i].autonomy = slot->autonomy;
        out[i].ctx_used = st.ctx_used;
        out[i].ctx_size = st.ctx_size;
        out[i].dirty = slot->has_worker && slot->worker.session_dirty;
        out[i].queued_output = slot->unread || slot->background.len > 0;
        out[i].queued_output_bytes = slot->background.len;
        out[i].approval_blocked = slot->has_worker &&
            (slot->worker.web_approval_pending || slot->worker.path_approval_pending);
        out[i].budget_used = slot->has_worker ?
            slot->worker.model_tool_round_used : slot->budget_used;
        out[i].budget_limit = slot->budget_limit;
        out[i].think_mode = slot->has_worker ? st.think_mode : slot->cfg.gen.think_mode;
        out[i].report_available = slot->report_available;
        snprintf(out[i].stop_reason, sizeof(out[i].stop_reason), "%s",
                 slot->stop_reason[0] ? slot->stop_reason :
                 (slot->has_worker ? slot->worker.autonomy_stop_reason : ""));
    }
    return 0;
}

int ds_agent_subagent_report(ds_agent_subagents *mgr,
                             ds_agent_subagent_id id,
                             char *buf,
                             size_t len) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (!slot || !buf || len == 0) return -1;
    const char *report = slot->report && slot->report[0] ?
        slot->report : "No subagent report is available yet.";
    snprintf(buf, len, "%s", report);
    return 0;
}

int ds_agent_subagent_import_report(ds_agent_subagents *mgr,
                                    ds_agent_subagent_id from,
                                    ds_agent_subagent_id into) {
    agent_session_slot *src = ds_agent_subagents_slot_by_id(mgr, from);
    agent_session_slot *dst = ds_agent_subagents_slot_by_id(mgr, into);
    if (!src || !dst) {
        ds_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    const char *report = src->report && src->report[0] ?
        src->report : "No subagent report is available yet.";
    agent_buf b = {0};
    agent_buf_puts(&b, "Delegated subagent report from ");
    agent_buf_puts(&b, src->name);
    agent_buf_puts(&b, ":\n\n");
    agent_buf_puts(&b, report);
    char *prompt = agent_buf_take(&b);
    agent_prompt_queue_push(&dst->queue, prompt);
    free(prompt);
    ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_REPORT,
                                  dst, "report imported");
    return 0;
}

static bool ds_agent_subagent_refresh_report(agent_session_slot *slot,
                                             const char *out,
                                             size_t out_len,
                                             const agent_status *st) {
    if (!slot || !out || !out_len || !st) return false;
    if (st->state != AGENT_WORKER_IDLE &&
        st->state != AGENT_WORKER_ERROR &&
        st->state != AGENT_WORKER_STOPPED)
        return false;
    bool had_report = slot->report_available;
    size_t start = out_len > 4096 ? out_len - 4096 : 0;
    free(slot->report);
    slot->report = xstrndup(out + start, out_len - start);
    slot->report_available = true;
    if (!slot->stop_reason[0]) {
        snprintf(slot->stop_reason, sizeof(slot->stop_reason), "%s",
                 st->state == AGENT_WORKER_ERROR ? "blocked" :
                 st->state == AGENT_WORKER_STOPPED ? "interrupted" : "done");
    }
    return !had_report && slot->report_available;
}

void ds_agent_subagents_submit_ready(ds_agent_subagents *mgr) {
    if (!mgr) return;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (slot->id.value == mgr->active_id) continue;
        if (!slot->has_worker || !slot->queue.len || !worker_is_idle(&slot->worker))
            continue;
        char *queued = agent_prompt_queue_take_all(&slot->queue);
        if (worker_submit(&slot->worker, queued)) {
            ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_STATUS,
                                          slot, "submitted queued prompt");
        } else {
            agent_prompt_queue_push_front(&slot->queue, queued);
            queued = NULL;
        }
        free(queued);
    }
}

size_t ds_agent_subagents_worker_count(ds_agent_subagents *mgr) {
    if (!mgr) return 0;
    size_t n = 0;
    for (size_t i = 0; i < mgr->len; i++)
        if (mgr->slots[i].has_worker) n++;
    return n;
}

static agent_session_slot *ds_agent_subagents_worker_slot_at(ds_agent_subagents *mgr,
                                                            size_t worker_idx) {
    if (!mgr) return NULL;
    size_t seen = 0;
    for (size_t i = 0; i < mgr->len; i++) {
        if (!mgr->slots[i].has_worker) continue;
        if (seen++ == worker_idx) return &mgr->slots[i];
    }
    return NULL;
}

int ds_agent_subagents_worker_fd_at(ds_agent_subagents *mgr,
                                    size_t worker_idx) {
    agent_session_slot *slot =
        ds_agent_subagents_worker_slot_at(mgr, worker_idx);
    return slot && slot->has_worker ? slot->worker.wake_fd[0] : -1;
}

void ds_agent_subagents_drain_wake_fds(ds_agent_subagents *mgr,
                                       struct pollfd *pfd,
                                       size_t count) {
    if (!mgr || !pfd) return;
    for (size_t i = 0; i < count; i++) {
        agent_session_slot *slot = ds_agent_subagents_worker_slot_at(mgr, i);
        if (slot && (pfd[i].revents & POLLIN))
            drain_wake_fd(slot->worker.wake_fd[0]);
    }
}

bool ds_agent_subagents_check_raw_mode_restore(ds_agent_subagents *mgr) {
    if (!mgr) return false;
    bool needs = false;
    for (size_t i = 0; i < mgr->len; i++) {
        if (mgr->slots[i].has_worker &&
            worker_check_raw_mode_restore(&mgr->slots[i].worker))
            needs = true;
    }
    return needs;
}

void ds_agent_subagents_drain_outputs(ds_agent_subagents *mgr,
                                      char **active_out,
                                      size_t *active_out_len,
                                      agent_status *active_status,
                                      char **notifications) {
    if (active_out) *active_out = NULL;
    if (active_out_len) *active_out_len = 0;
    if (notifications) *notifications = NULL;
    if (!mgr) return;
    agent_buf notes = {0};
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        char *out = NULL;
        size_t out_len = 0;
        agent_status st = {0};
        worker_consume(&slot->worker, &out, &out_len, &st);
        if (slot->id.value == mgr->active_id) {
            if (active_status) *active_status = st;
            if (active_out) *active_out = out;
            if (active_out_len) *active_out_len = out_len;
            if (ds_agent_subagent_refresh_report(slot, out, out_len, &st))
                ds_agent_subagents_push_event(mgr,
                    DS_AGENT_SUBAGENT_EVENT_REPORT, slot, "report available");
            out = NULL;
        } else if (out && out_len) {
            agent_buf_append_full(&slot->background, out, out_len);
            slot->unread = true;
            if (ds_agent_subagent_refresh_report(slot, out, out_len, &st))
                ds_agent_subagents_push_event(mgr,
                    DS_AGENT_SUBAGENT_EVENT_REPORT, slot, "report available");
            ds_agent_subagents_push_event(mgr, DS_AGENT_SUBAGENT_EVENT_OUTPUT,
                                          slot, "background output queued");
        }
        if (slot->has_worker &&
            (st.state == AGENT_WORKER_ERROR || st.state == AGENT_WORKER_STOPPED)) {
            char line[192];
            snprintf(line, sizeof(line), "\n[subagent %s] %s\n", slot->name,
                     st.state == AGENT_WORKER_ERROR ?
                     (st.error[0] ? st.error : "worker error") : "stopped");
            agent_buf_puts(&notes, line);
        }
        free(out);
    }
    ds_agent_subagents_update_worker_metadata(mgr);
    if (active_status) {
        agent_session_slot *active = ds_agent_subagents_active_slot(mgr);
        if (active && active->has_worker)
            worker_get_status(&active->worker, active_status);
    }
    if (notifications && notes.ptr && notes.len)
        *notifications = agent_buf_take(&notes);
    free(notes.ptr);
}

char *ds_agent_subagents_take_active_replay(ds_agent_subagents *mgr) {
    agent_session_slot *slot = ds_agent_subagents_active_slot(mgr);
    if (!slot || !slot->background.len) return NULL;
    char *out = agent_buf_take(&slot->background);
    slot->unread = false;
    return out;
}

bool ds_agent_subagents_take_web_approval(ds_agent_subagents *mgr,
                                          ds_agent_subagent_id *id,
                                          char *msg,
                                          size_t msg_len) {
    if (!mgr || !msg || msg_len == 0) return false;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        char inner[256] = {0};
        if (worker_take_web_approval_request(&slot->worker, inner,
                                             sizeof(inner))) {
            if (id) *id = slot->id;
            snprintf(msg, msg_len, "[subagent %s] %s", slot->name, inner);
            ds_agent_subagents_push_event(mgr,
                DS_AGENT_SUBAGENT_EVENT_APPROVAL, slot, "web approval requested");
            return true;
        }
    }
    return false;
}

void ds_agent_subagents_answer_web_approval(ds_agent_subagents *mgr,
                                            ds_agent_subagent_id id,
                                            bool allow,
                                            const char *err) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (slot && slot->has_worker)
        worker_answer_web_approval(&slot->worker, allow, err);
}

bool ds_agent_subagents_take_path_approval(ds_agent_subagents *mgr,
                                           ds_agent_subagent_id *id,
                                           char *msg,
                                           size_t msg_len,
                                           char options[3][PATH_MAX],
                                           int *option_count) {
    if (!mgr || !msg || msg_len == 0) return false;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        char inner[PATH_MAX + 256] = {0};
        if (worker_take_path_approval_request(&slot->worker, inner,
                                              sizeof(inner), options,
                                              option_count)) {
            if (id) *id = slot->id;
            snprintf(msg, msg_len, "[subagent %s] %s", slot->name, inner);
            ds_agent_subagents_push_event(mgr,
                DS_AGENT_SUBAGENT_EVENT_APPROVAL, slot, "path approval requested");
            return true;
        }
    }
    return false;
}

void ds_agent_subagents_answer_path_approval(ds_agent_subagents *mgr,
                                             ds_agent_subagent_id id,
                                             bool allow,
                                             const char *choice,
                                             const char *err) {
    agent_session_slot *slot = ds_agent_subagents_slot_by_id(mgr, id);
    if (slot && slot->has_worker)
        worker_answer_path_approval(&slot->worker, allow, choice, err);
}

bool ds_agent_subagents_take_queued_user_drain(ds_agent_subagents *mgr) {
    if (!mgr) return false;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        if (worker_take_queued_user_drain_request(&slot->worker)) {
            char *queued = agent_prompt_queue_take_all(&slot->queue);
            worker_answer_queued_user_drain(&slot->worker, queued);
            return true;
        }
    }
    return false;
}

static void ds_agent_subagent_print_list(ds_agent_subagents *mgr) {
    size_t n = 0;
    ds_agent_subagent_list(mgr, NULL, 0, &n);
    ds_agent_subagent_status *items = xmalloc((n ? n : 1) * sizeof(items[0]));
    ds_agent_subagent_list(mgr, items, n, &n);
    printf("subagents:\n");
    for (size_t i = 0; i < n; i++) {
        char used[32], total[32];
        agent_format_ctx_size(items[i].ctx_used, used, sizeof(used));
        agent_format_ctx_size(items[i].ctx_size, total, sizeof(total));
        printf("%c %" PRIu64 " %-16s %-14s ctx %s/%s%s%s%s%s",
               items[i].active ? '*' : ' ',
               items[i].id.value,
               items[i].name,
               ds_agent_subagent_state_name(items[i].state),
               used,
               total,
               items[i].dirty ? " dirty" : "",
               items[i].queued_output ? " unread" : "",
               items[i].approval_blocked ? " approval" : "",
               items[i].report_available ? " report" : "");
        printf(" thinking %s",
               ds_agent_subagent_think_mode_name(items[i].think_mode));
        if (items[i].budget_limit > 0)
            printf(" budget %d/%d", items[i].budget_used, items[i].budget_limit);
        printf("\n");
    }
    free(items);
}

static void ds_agent_subagent_usage(void) {
    puts("usage:");
    puts("  /subagent new [--tab|--background|--auto] [--thinking off|default|max] [name] [prompt]");
    puts("  /subagent list");
    puts("  /subagent switch <id|name>");
    puts("  /subagent send <id|name> <prompt>");
    puts("  /subagent stop <id|name>");
    puts("  /subagent close <id|name>");
    puts("  /subagent report <id|name>");
    puts("  /subagent import <id|name>");
}

static char *ds_agent_subagent_next_arg(char **args) {
    if (!args || !*args) return NULL;
    char *p = *args;
    while (*p == ' ' || *p == '\t') p++;
    if (!*p) {
        *args = p;
        return NULL;
    }
    char *start = p;
    while (*p && *p != ' ' && *p != '\t') p++;
    if (*p) *p++ = '\0';
    *args = p;
    return start;
}

bool ds_agent_subagents_handle_command(ds_agent_subagents *mgr,
                                       char *cmd,
                                       bool busy) {
    if (!mgr || !cmd || !agent_slash_command_with_args(cmd, "/subagent"))
        return false;
    char *args = cmd + strlen("/subagent");
    char *op = ds_agent_subagent_next_arg(&args);
    if (!op || !strcmp(op, "help")) {
        ds_agent_subagent_usage();
        return true;
    }
    if (!strcmp(op, "list")) {
        ds_agent_subagent_print_list(mgr);
        return true;
    }
    if (!strcmp(op, "new")) {
        if (!ds_agent_subagents_active_is_main(mgr)) {
            printf("subagents cannot create nested subagents\n");
            return true;
        }
        ds_agent_subagent_autonomy mode = DS_AGENT_SUBAGENT_AUTONOMY_BACKGROUND;
        ds4_think_mode think_mode = DS4_THINK_HIGH;
        bool think_mode_set = false;
        char *name = NULL;
        for (;;) {
            char *arg = ds_agent_subagent_next_arg(&args);
            if (!arg) break;
            if (!strcmp(arg, "--tab")) {
                mode = DS_AGENT_SUBAGENT_AUTONOMY_TAB;
                continue;
            }
            if (!strcmp(arg, "--background")) {
                mode = DS_AGENT_SUBAGENT_AUTONOMY_BACKGROUND;
                continue;
            }
            if (!strcmp(arg, "--auto")) {
                mode = DS_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS;
                continue;
            }
            if (!strcmp(arg, "--thinking")) {
                char *value = ds_agent_subagent_next_arg(&args);
                if (!ds_agent_subagent_parse_think_mode(value, &think_mode)) {
                    printf("usage: /subagent new [--tab|--background|--auto] [--thinking off|default|max] [name] [prompt]\n");
                    return true;
                }
                think_mode_set = true;
                continue;
            }
            if (!strncmp(arg, "--thinking=", 11)) {
                if (!ds_agent_subagent_parse_think_mode(arg + 11, &think_mode)) {
                    printf("usage: /subagent new [--tab|--background|--auto] [--thinking off|default|max] [name] [prompt]\n");
                    return true;
                }
                think_mode_set = true;
                continue;
            }
            name = arg;
            break;
        }
        while (args && (*args == ' ' || *args == '\t')) args++;
        const char *prompt = args && args[0] ? args : NULL;
        if (!prompt && mode == DS_AGENT_SUBAGENT_AUTONOMY_BACKGROUND)
            mode = DS_AGENT_SUBAGENT_AUTONOMY_TAB;
        ds_agent_subagent_create_request req = {
            .name = name,
            .prompt = prompt,
            .autonomy = mode,
            .think_mode = think_mode,
            .think_mode_set = think_mode_set,
        };
        ds_agent_subagent_id id = {0};
        if (ds_agent_subagent_create(mgr, &req, &id) != 0)
            printf("subagent create failed: %s\n",
                   ds_agent_subagents_last_error(mgr));
        else
            printf("created subagent %" PRIu64 " %s%s\n", id.value,
                   ds_agent_subagents_slot_by_id(mgr, id)->name,
                   prompt ? "; running in background" : "");
        return true;
    }
    if (!strcmp(op, "switch")) {
        char *which = ds_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds_agent_subagents_resolve(mgr, which);
        if (!slot) {
            printf("subagent switch failed: unknown session %s\n",
                   which ? which : "");
        } else if (ds_agent_subagent_switch(mgr, slot->id) != 0) {
            printf("subagent switch failed: %s\n",
                   ds_agent_subagents_last_error(mgr));
        } else {
            printf("switched to subagent %s\n", slot->name);
        }
        return true;
    }
    if (!strcmp(op, "send")) {
        char *which = ds_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds_agent_subagents_resolve(mgr, which);
        while (args && (*args == ' ' || *args == '\t')) args++;
        if (!slot || !args || !args[0]) {
            printf("usage: /subagent send <id|name> <prompt>\n");
        } else if (ds_agent_subagent_send(mgr, slot->id, args) != 0) {
            printf("subagent send failed: %s\n",
                   ds_agent_subagents_last_error(mgr));
        } else {
            printf("sent prompt to subagent %s\n", slot->name);
        }
        return true;
    }
    if (!strcmp(op, "stop")) {
        char *which = ds_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds_agent_subagents_resolve(mgr, which);
        if (!slot) printf("subagent stop failed: unknown session\n");
        else if (ds_agent_subagent_stop(mgr, slot->id) != 0)
            printf("subagent stop failed: %s\n", ds_agent_subagents_last_error(mgr));
        else printf("stop requested for subagent %s\n", slot->name);
        return true;
    }
    if (!strcmp(op, "close")) {
        char *which = ds_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds_agent_subagents_resolve(mgr, which);
        if (!slot) {
            printf("subagent close failed: unknown session\n");
        } else if (slot->has_worker && !worker_is_idle(&slot->worker)) {
            printf("subagent close failed: %s is busy; stop it first\n", slot->name);
        } else {
            if (slot->has_worker && agent_worker_needs_save(&slot->worker)) {
                if (agent_prompt_yes_no("Save subagent before closing? (y/n) ")) {
                    char err[160] = {0};
                    if (!agent_worker_save_session(&slot->worker, err, sizeof(err))) {
                        printf("save failed: %s\n", err);
                        return true;
                    }
                }
            }
            char closed[DS_AGENT_SUBAGENT_NAME_MAX];
            snprintf(closed, sizeof(closed), "%s", slot->name);
            if (ds_agent_subagent_close(mgr, slot->id) != 0)
                printf("subagent close failed: %s\n",
                       ds_agent_subagents_last_error(mgr));
            else
                printf("closed subagent %s\n", closed);
        }
        return true;
    }
    if (!strcmp(op, "report") || !strcmp(op, "import")) {
        char *which = ds_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds_agent_subagents_resolve(mgr, which);
        if (!slot) {
            printf("subagent %s failed: unknown session\n", op);
            return true;
        }
        if (!strcmp(op, "report")) {
            char report[4096];
            if (ds_agent_subagent_report(mgr, slot->id, report, sizeof(report)) != 0)
                printf("subagent report failed\n");
            else
                printf("%s\n", report);
        } else {
            agent_session_slot *active = ds_agent_subagents_active_slot(mgr);
            if (!active || ds_agent_subagent_import_report(mgr, slot->id,
                                                           active->id) != 0)
                printf("subagent import failed: %s\n",
                       ds_agent_subagents_last_error(mgr));
            else if (busy)
                printf("report queued for active session\n");
            else
                printf("report imported into active session\n");
        }
        return true;
    }
    ds_agent_subagent_usage();
    return true;
}

#ifdef DS4_AGENT_TEST
static void test_agent_fake_worker_init(agent_worker *w, agent_config *cfg) {
    memset(w, 0, sizeof(*w));
    w->cfg = cfg;
    w->wake_fd[0] = -1;
    w->wake_fd[1] = -1;
    w->docker_shell.stdin_fd = -1;
    w->docker_shell.stdout_fd = -1;
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->cond, NULL);
    pthread_mutex_init(&w->docker_shell.mu, NULL);
    AGENT_TEST_ASSERT(pipe(w->wake_fd) == 0);
    w->status.state = AGENT_WORKER_IDLE;
}

static agent_session_slot *test_agent_subagent_add_fake_slot(
    ds_agent_subagents *mgr,
    const char *name,
    uint64_t id) {
    ds_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = id;
    slot->autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB;
    slot->budget_limit = 9;
    snprintf(slot->name, sizeof(slot->name), "%s", name);
    slot->cfg.gen.ctx_size = 1234;
    slot->cfg.gen.think_mode = DS4_THINK_HIGH;
    test_agent_fake_worker_init(&slot->worker, &slot->cfg);
    slot->has_worker = true;
    return slot;
}

static void test_agent_subagent_create_think_mode_inherits_or_overrides(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds_agent_subagent_id main_id = {0};
    ds_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_MAX,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    ds_agent_subagent_id inherited_id = {0};
    ds_agent_subagent_create_request inherited_req = {
        .name = "inherit",
        .autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds_agent_subagent_create(mgr, &inherited_req, &inherited_id) == 0);

    ds_agent_subagent_id explicit_id = {0};
    ds_agent_subagent_create_request explicit_req = {
        .name = "explicit",
        .autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_NONE,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds_agent_subagent_create(mgr, &explicit_req, &explicit_id) == 0);

    ds_agent_subagent_status st[4];
    size_t n = 0;
    AGENT_TEST_ASSERT(ds_agent_subagent_list(mgr, st, 4, &n) == 0);
    AGENT_TEST_ASSERT(n == 3);
    AGENT_TEST_ASSERT(st[0].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[1].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[2].think_mode == DS4_THINK_NONE);
    ds_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_new_command_accepts_thinking_flag(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds_agent_subagent_id main_id = {0};
    ds_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_MAX,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    char cmd1[] = "/subagent new --thinking off explicit";
    AGENT_TEST_ASSERT(ds_agent_subagents_handle_command(mgr, cmd1, false));
    AGENT_TEST_ASSERT(mgr->len == 2);
    AGENT_TEST_ASSERT(mgr->slots[1].cfg.gen.think_mode == DS4_THINK_NONE);

    char cmd2[] = "/subagent new inherited";
    AGENT_TEST_ASSERT(ds_agent_subagents_handle_command(mgr, cmd2, false));
    AGENT_TEST_ASSERT(mgr->len == 3);
    AGENT_TEST_ASSERT(mgr->slots[2].cfg.gen.think_mode == DS4_THINK_MAX);

    ds_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_switch_preserves_thinking_modes(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    agent_session_slot *beta =
        test_agent_subagent_add_fake_slot(mgr, "beta", 2);
    mgr->active_id = main_slot->id.value;

    main_slot->cfg.gen.think_mode = DS4_THINK_MAX;
    main_slot->worker.status.think_mode = DS4_THINK_MAX;
    beta->cfg.gen.think_mode = DS4_THINK_NONE;
    beta->worker.status.think_mode = DS4_THINK_NONE;

    AGENT_TEST_ASSERT(ds_agent_subagent_switch(mgr, beta->id) == 0);

    ds_agent_subagent_status st[2];
    size_t n = 0;
    AGENT_TEST_ASSERT(ds_agent_subagent_list(mgr, st, 2, &n) == 0);
    AGENT_TEST_ASSERT(n == 2);
    AGENT_TEST_ASSERT(!st[0].active);
    AGENT_TEST_ASSERT(st[0].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[1].active);
    AGENT_TEST_ASSERT(st[1].think_mode == DS4_THINK_NONE);

    ds_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_slot_isolation(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *alpha =
        test_agent_subagent_add_fake_slot(mgr, "alpha", 1);
    agent_session_slot *beta =
        test_agent_subagent_add_fake_slot(mgr, "beta", 2);
    mgr->active_id = alpha->id.value;

    ds4_tokens_push(&alpha->worker.transcript, 11);
    ds4_tokens_push(&beta->worker.transcript, 22);
    alpha->worker.session_dirty = true;
    beta->worker.session_dirty = false;
    agent_prompt_queue_push(&alpha->queue, "alpha prompt");
    agent_prompt_queue_push(&beta->queue, "beta prompt");
    agent_buf_puts(&alpha->background, "alpha output");
    agent_buf_puts(&beta->background, "beta output");
    agent_path_list_append(&alpha->worker.auto_allowed_paths, "/tmp/alpha");
    agent_path_list_append(&beta->worker.auto_allowed_paths, "/tmp/beta");

    AGENT_TEST_ASSERT(alpha->worker.transcript.len == 1);
    AGENT_TEST_ASSERT(beta->worker.transcript.len == 1);
    AGENT_TEST_ASSERT(alpha->worker.transcript.v[0] == 11);
    AGENT_TEST_ASSERT(beta->worker.transcript.v[0] == 22);
    AGENT_TEST_ASSERT(alpha->worker.session_dirty);
    AGENT_TEST_ASSERT(!beta->worker.session_dirty);
    AGENT_TEST_ASSERT(!strcmp(alpha->queue.v[0], "alpha prompt"));
    AGENT_TEST_ASSERT(!strcmp(beta->queue.v[0], "beta prompt"));
    AGENT_TEST_ASSERT(!strcmp(alpha->background.ptr, "alpha output"));
    AGENT_TEST_ASSERT(!strcmp(beta->background.ptr, "beta output"));
    AGENT_TEST_ASSERT(!strcmp(alpha->worker.auto_allowed_paths.v[0], "/tmp/alpha"));
    AGENT_TEST_ASSERT(!strcmp(beta->worker.auto_allowed_paths.v[0], "/tmp/beta"));
    ds_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_background_replay(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    agent_session_slot *bg =
        test_agent_subagent_add_fake_slot(mgr, "bg", 2);
    mgr->active_id = main_slot->id.value;
    bg->worker.out = xstrdup("background chunk\n");
    bg->worker.out_len = strlen(bg->worker.out);
    bg->worker.out_cap = bg->worker.out_len + 1;
    bg->worker.status.state = AGENT_WORKER_IDLE;

    char *active_out = NULL;
    size_t active_out_len = 0;
    agent_status st = {0};
    char *notes = NULL;
    ds_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(active_out == NULL || active_out_len == 0);
    AGENT_TEST_ASSERT(notes == NULL || notes[0] == '\0');
    AGENT_TEST_ASSERT(bg->unread);
    free(active_out);
    free(notes);

    AGENT_TEST_ASSERT(ds_agent_subagent_switch(mgr, bg->id) == 0);
    char *replay = ds_agent_subagents_take_active_replay(mgr);
    AGENT_TEST_ASSERT(replay != NULL);
    AGENT_TEST_ASSERT(!strcmp(replay, "background chunk\n"));
    AGENT_TEST_ASSERT(!bg->unread);
    free(replay);
    ds_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_error_notifications_remain_visible(void) {
    ds_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    agent_session_slot *bg =
        test_agent_subagent_add_fake_slot(mgr, "bg", 2);
    mgr->active_id = main_slot->id.value;
    bg->worker.status.state = AGENT_WORKER_ERROR;
    snprintf(bg->worker.status.error, sizeof(bg->worker.status.error),
             "%s", "worker error");

    char *active_out = NULL;
    size_t active_out_len = 0;
    agent_status st = {0};
    char *notes = NULL;
    ds_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(notes != NULL);
    AGENT_TEST_ASSERT(strstr(notes, "[subagent bg] worker error") != NULL);
    free(active_out);
    free(notes);
    ds_agent_subagents_destroy(mgr);
}

void ds_agent_subagent_unit_tests_run(void) {
    test_agent_subagent_slot_isolation();
    test_agent_subagent_background_replay();
    test_agent_subagent_error_notifications_remain_visible();
    test_agent_subagent_create_think_mode_inherits_or_overrides();
    test_agent_subagent_new_command_accepts_thinking_flag();
    test_agent_subagent_switch_preserves_thinking_modes();
}
#endif
