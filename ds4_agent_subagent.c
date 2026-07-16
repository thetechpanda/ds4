#include "ds4_agent_internal.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct agent_session_slot {
    ds4_agent_subagent_id id;
    char name[DS4_AGENT_SUBAGENT_NAME_MAX];
    ds4_agent_subagent_autonomy autonomy;
    agent_worker worker;
    bool has_worker;
    agent_config cfg;
    agent_prompt_queue queue;
    agent_buf manager_output;
    agent_buf background;
    bool unread;
    bool close_requested;
    bool report_available;
    char *report;
    char stop_reason[DS4_AGENT_SUBAGENT_TEXT_MAX];
    int budget_limit;
    int budget_used;
    ds4_agent_tool_policy tool_policy;
} agent_session_slot;

struct ds4_agent_subagents {
    ds4_engine *engine;
    pthread_mutex_t model_gate;
    agent_session_slot *slots;
    size_t len;
    size_t cap;
    uint64_t next_id;
    uint64_t active_id;
    ds4_agent_subagent_event *events;
    size_t event_len;
    size_t event_cap;
    char last_error[DS4_AGENT_SUBAGENT_ERROR_MAX];
    int default_ctx_size;
    int default_round_budget;
};

static const char *ds4_agent_subagent_autonomy_name(ds4_agent_subagent_autonomy mode) {
    switch (mode) {
    case DS4_AGENT_SUBAGENT_AUTONOMY_TAB: return "tab";
    case DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND: return "background";
    case DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS: return "autonomous";
    default: return "unknown";
    }
}

static const char *ds4_agent_subagent_think_mode_name(ds4_think_mode mode) {
    switch (mode) {
    case DS4_THINK_NONE: return "off";
    case DS4_THINK_HIGH: return "default";
    case DS4_THINK_MAX: return "max";
    default: return ds4_think_mode_name(mode);
    }
}

static bool ds4_agent_subagent_parse_think_mode(const char *text,
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

static bool ds4_agent_subagent_parse_round_budget(const char *text, int *out) {
    if (!text || !text[0] || !out) return false;
    errno = 0;
    char *end = NULL;
    long value = strtol(text, &end, 10);
    if (errno || !end || *end || value > INT_MAX) return false;
    if (value == 0 || value < -1) return false;
    *out = (int)value;
    return true;
}

static void ds4_agent_subagents_set_error(ds4_agent_subagents *mgr,
                                         const char *fmt, ...) {
    if (!mgr) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(mgr->last_error, sizeof(mgr->last_error), fmt, ap);
    va_end(ap);
}

const char *ds4_agent_subagents_last_error(const ds4_agent_subagents *mgr) {
    if (!mgr || !mgr->last_error[0]) return "";
    return mgr->last_error;
}

static bool ds4_agent_subagent_id_equal(ds4_agent_subagent_id a,
                                       ds4_agent_subagent_id b) {
    return a.value == b.value;
}

static agent_session_slot *ds4_agent_subagents_slot_by_id(ds4_agent_subagents *mgr,
                                                        ds4_agent_subagent_id id) {
    if (!mgr || !id.value) return NULL;
    for (size_t i = 0; i < mgr->len; i++) {
        if (ds4_agent_subagent_id_equal(mgr->slots[i].id, id))
            return &mgr->slots[i];
    }
    return NULL;
}

static agent_session_slot *ds4_agent_subagents_slot_by_name(ds4_agent_subagents *mgr,
                                                          const char *name) {
    if (!mgr || !name || !name[0]) return NULL;
    for (size_t i = 0; i < mgr->len; i++) {
        if (!strcmp(mgr->slots[i].name, name)) return &mgr->slots[i];
    }
    return NULL;
}

static agent_session_slot *ds4_agent_subagents_active_slot(ds4_agent_subagents *mgr) {
    if (!mgr) return NULL;
    return ds4_agent_subagents_slot_by_id(mgr,
        (ds4_agent_subagent_id){.value = mgr->active_id});
}

static void ds4_agent_subagent_slot_vprintf(agent_session_slot *slot,
                                           const char *fmt,
                                           va_list ap) {
    if (!slot || !fmt || !fmt[0]) return;
    char stack[512];
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int n = vsnprintf(stack, sizeof(stack), fmt, ap_copy);
    va_end(ap_copy);
    if (n <= 0) return;
    if ((size_t)n < sizeof(stack)) {
        agent_buf_append_full(&slot->manager_output, stack, (size_t)n);
        return;
    }
    char *heap = xmalloc((size_t)n + 1);
    vsnprintf(heap, (size_t)n + 1, fmt, ap);
    agent_buf_append_full(&slot->manager_output, heap, (size_t)n);
    free(heap);
}

static void ds4_agent_subagent_slot_printf(agent_session_slot *slot,
                                          const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    ds4_agent_subagent_slot_vprintf(slot, fmt, ap);
    va_end(ap);
}

static void ds4_agent_subagents_active_printf(ds4_agent_subagents *mgr,
                                             const char *fmt, ...) {
    agent_session_slot *slot = ds4_agent_subagents_active_slot(mgr);
    if (!slot) return;
    va_list ap;
    va_start(ap, fmt);
    ds4_agent_subagent_slot_vprintf(slot, fmt, ap);
    va_end(ap);
}

static void ds4_agent_subagent_append_combined_output(agent_buf *dst,
                                                     agent_session_slot *slot,
                                                     const char *worker_out,
                                                     size_t worker_out_len) {
    if (!dst || !slot) return;
    if (slot->manager_output.len)
        agent_buf_append_full(dst, slot->manager_output.ptr,
                              slot->manager_output.len);
    if (worker_out && worker_out_len)
        agent_buf_append_full(dst, worker_out, worker_out_len);
}

agent_worker *ds4_agent_subagents_active_worker(ds4_agent_subagents *mgr) {
    agent_session_slot *slot = ds4_agent_subagents_active_slot(mgr);
    return slot && slot->has_worker ? &slot->worker : NULL;
}

void ds4_agent_subagents_update_worker_metadata(ds4_agent_subagents *mgr) {
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

agent_prompt_queue *ds4_agent_subagents_active_queue(ds4_agent_subagents *mgr) {
    agent_session_slot *slot = ds4_agent_subagents_active_slot(mgr);
    return slot ? &slot->queue : NULL;
}

static bool ds4_agent_subagents_active_is_main(ds4_agent_subagents *mgr) {
    agent_session_slot *slot = ds4_agent_subagents_active_slot(mgr);
    return slot && !strcmp(slot->name, "main");
}

static agent_session_slot *ds4_agent_subagents_resolve(ds4_agent_subagents *mgr,
                                                     const char *spec) {
    if (!mgr || !spec || !spec[0]) return NULL;
    char *end = NULL;
    unsigned long long numeric = strtoull(spec, &end, 10);
    if (spec[0] && end && *end == '\0' && numeric > 0) {
        agent_session_slot *slot = ds4_agent_subagents_slot_by_id(
            mgr, (ds4_agent_subagent_id){.value = (uint64_t)numeric});
        if (slot) return slot;
    }
    return ds4_agent_subagents_slot_by_name(mgr, spec);
}

static void ds4_agent_subagents_push_event(ds4_agent_subagents *mgr,
                                          ds4_agent_subagent_event_type type,
                                          const agent_session_slot *slot,
                                          const char *text) {
    if (!mgr) return;
    if (mgr->event_len == mgr->event_cap) {
        mgr->event_cap = mgr->event_cap ? mgr->event_cap * 2 : 8;
        mgr->events = xrealloc(mgr->events,
                               mgr->event_cap * sizeof(mgr->events[0]));
    }
    ds4_agent_subagent_event *ev = &mgr->events[mgr->event_len++];
    memset(ev, 0, sizeof(*ev));
    ev->type = type;
    if (slot) {
        ev->id = slot->id;
        snprintf(ev->name, sizeof(ev->name), "%s", slot->name);
    }
    snprintf(ev->text, sizeof(ev->text), "%s", text ? text : "");
}

int ds4_agent_subagent_poll_event(ds4_agent_subagents *mgr,
                                 ds4_agent_subagent_event *out) {
    if (!mgr || !out || mgr->event_len == 0) return 0;
    *out = mgr->events[0];
    memmove(mgr->events, mgr->events + 1,
            (mgr->event_len - 1) * sizeof(mgr->events[0]));
    mgr->event_len--;
    return 1;
}

static void ds4_agent_subagents_ensure_slot_cap(ds4_agent_subagents *mgr) {
    if (mgr->len == mgr->cap) {
        mgr->cap = mgr->cap ? mgr->cap * 2 : 4;
        mgr->slots = xrealloc(mgr->slots, mgr->cap * sizeof(mgr->slots[0]));
    }
}

static const char *ds4_agent_subagent_state_name(ds4_agent_subagent_state state) {
    switch (state) {
    case DS4_AGENT_SUBAGENT_STATE_IDLE: return "idle";
    case DS4_AGENT_SUBAGENT_STATE_RUNNING: return "running";
    case DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL: return "waiting-model";
    case DS4_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED: return "approval";
    case DS4_AGENT_SUBAGENT_STATE_ERROR: return "error";
    case DS4_AGENT_SUBAGENT_STATE_STOPPED: return "stopped";
    default: return "unknown";
    }
}

static ds4_agent_subagent_state ds4_agent_subagent_state_from_worker(
    const agent_session_slot *slot,
    const agent_status *st) {
    if (!slot || !slot->has_worker) return DS4_AGENT_SUBAGENT_STATE_IDLE;
    if (st && (st->state == AGENT_WORKER_ERROR))
        return DS4_AGENT_SUBAGENT_STATE_ERROR;
    if (st && st->state == AGENT_WORKER_STOPPED)
        return DS4_AGENT_SUBAGENT_STATE_STOPPED;
    if (st && st->state == AGENT_WORKER_WAITING_MODEL)
        return DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL;
    if (st && (st->state == AGENT_WORKER_PREFILL ||
               st->state == AGENT_WORKER_GENERATING ||
               st->state == AGENT_WORKER_COMPACTING ||
               st->state == AGENT_WORKER_DRAINING ||
               st->state == AGENT_WORKER_SAVING))
        return DS4_AGENT_SUBAGENT_STATE_RUNNING;
    if (slot->worker.web_approval_pending || slot->worker.path_approval_pending)
        return DS4_AGENT_SUBAGENT_STATE_APPROVAL_BLOCKED;
    return DS4_AGENT_SUBAGENT_STATE_IDLE;
}

static void ds4_agent_subagent_config_copy(agent_config *dst,
                                          const agent_config *src) {
    memset(dst, 0, sizeof(*dst));
    if (src) *dst = *src;
    dst->working_directory_args.v = NULL;
    dst->working_directory_args.len = 0;
    dst->working_directory_args.cap = 0;
    dst->working_directories.v = NULL;
    dst->working_directories.len = 0;
    dst->working_directories.cap = 0;
    dst->skill_dirs.v = NULL;
    dst->skill_dirs.len = 0;
    dst->skill_dirs.cap = 0;
    if (src) {
        for (int i = 0; i < src->working_directory_args.len; i++)
            agent_path_list_append(&dst->working_directory_args,
                                   src->working_directory_args.v[i]);
        for (int i = 0; i < src->working_directories.len; i++)
            agent_path_list_append(&dst->working_directories,
                                   src->working_directories.v[i]);
        for (int i = 0; i < src->skill_dirs.len; i++)
            agent_path_list_append(&dst->skill_dirs, src->skill_dirs.v[i]);
    }
}

static void ds4_agent_subagent_config_free(agent_config *cfg) {
    if (!cfg) return;
    agent_path_list_free(&cfg->working_directory_args);
    agent_path_list_free(&cfg->working_directories);
    agent_path_list_free(&cfg->skill_dirs);
    memset(cfg, 0, sizeof(*cfg));
}

static void ds4_agent_subagent_slot_free(agent_session_slot *slot) {
    if (!slot) return;
    if (slot->has_worker) {
        agent_worker_free(&slot->worker);
        slot->has_worker = false;
    }
    agent_prompt_queue_free(&slot->queue);
    free(slot->manager_output.ptr);
    free(slot->background.ptr);
    free(slot->report);
    ds4_agent_subagent_config_free(&slot->cfg);
    memset(slot, 0, sizeof(*slot));
}

int ds4_agent_subagents_create(ds4_agent_subagents **out,
                              ds4_engine *engine,
                              const ds4_agent_subagent_options *opt) {
    if (!out) return -1;
    ds4_agent_subagents *mgr = xmalloc(sizeof(*mgr));
    memset(mgr, 0, sizeof(*mgr));
    mgr->engine = engine;
    mgr->next_id = 1;
    mgr->default_ctx_size = opt ? opt->default_context_size : 0;
    mgr->default_round_budget = opt &&
        (opt->default_round_budget > 0 || opt->default_round_budget == -1) ?
        opt->default_round_budget :
        DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET;
    pthread_mutex_init(&mgr->model_gate, NULL);
    *out = mgr;
    return 0;
}

int ds4_agent_subagents_create_for_agent(ds4_agent_subagents **out,
                                        ds4_engine *engine,
                                        const agent_config *cfg) {
    ds4_agent_subagent_options opt = {
        .default_context_size = cfg ? cfg->gen.ctx_size : 0,
        .default_round_budget = DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET,
    };
    if (ds4_agent_subagents_create(out, engine, &opt) != 0) return -1;
    ds4_agent_subagent_create_request req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    ds4_agent_subagent_id id = {0};
    ds4_agent_subagents *mgr = *out;
    ds4_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = mgr->next_id++;
    slot->autonomy = req.autonomy;
    slot->budget_limit = mgr->default_round_budget;
    ds4_agent_tool_policy_parse(cfg->default_tools[0] ? cfg->default_tools : "read", &slot->tool_policy);
    snprintf(slot->name, sizeof(slot->name), "%s", req.name);
    ds4_agent_subagent_config_copy(&slot->cfg, cfg);
    if (agent_worker_init(&slot->worker, engine, &slot->cfg) != 0) {
        ds4_agent_subagents_set_error(mgr, "failed to initialize main session");
        ds4_agent_subagent_slot_free(slot);
        mgr->len--;
        return -1;
    }
    slot->has_worker = true;
    slot->worker.tool_policy = slot->tool_policy;
    slot->worker.model_gate = &mgr->model_gate;
    slot->worker.model_tool_round_budget = 0;
    mgr->active_id = slot->id.value;
    id = slot->id;
    (void)id;
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_CREATED,
                                  slot, "created main session");
    return 0;
}

void ds4_agent_subagents_destroy(ds4_agent_subagents *mgr) {
    if (!mgr) return;
    for (size_t i = 0; i < mgr->len; i++)
        ds4_agent_subagent_slot_free(&mgr->slots[i]);
    free(mgr->slots);
    free(mgr->events);
    pthread_mutex_destroy(&mgr->model_gate);
    free(mgr);
}

static void ds4_agent_subagent_make_default_name(ds4_agent_subagents *mgr,
                                                char *buf,
                                                size_t len) {
    unsigned n = 1;
    do {
        snprintf(buf, len, "sub%u", n++);
    } while (ds4_agent_subagents_slot_by_name(mgr, buf));
}

static char *ds4_agent_subagent_mission_envelope(
    const char *name,
    const char *goal,
    ds4_agent_subagent_autonomy autonomy,
    const ds4_agent_tool_policy *tool_policy,
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
    agent_buf_puts(&b, ds4_agent_subagent_autonomy_name(autonomy));
    agent_buf_puts(&b, "\nAllowed tools: ");
    /* Render the effective tool list from the parsed policy. */
    if (tool_policy) {
        char buf[256];
        ds4_agent_tool_policy_format(tool_policy, buf, sizeof(buf));
        agent_buf_puts(&b, buf);
    } else {
        agent_buf_puts(&b, "none");
    }
    char tmp[96];
    if (budget < 0) {
        snprintf(tmp, sizeof(tmp),
                 "\nBudget: disabled; no model/tool round limit.");
    } else {
        snprintf(tmp, sizeof(tmp), "\nBudget: stop after %d model/tool rounds.",
                 budget > 0 ? budget :
                 DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET);
    }
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

int ds4_agent_subagent_create(ds4_agent_subagents *mgr,
                             const ds4_agent_subagent_create_request *req,
                             ds4_agent_subagent_id *out) {
    if (!mgr) return -1;
    const char *requested = req ? req->name : NULL;
    char generated[DS4_AGENT_SUBAGENT_NAME_MAX];
    if (!requested || !requested[0]) {
        ds4_agent_subagent_make_default_name(mgr, generated, sizeof(generated));
        requested = generated;
    }
    if (ds4_agent_subagents_slot_by_name(mgr, requested)) {
        ds4_agent_subagents_set_error(mgr, "subagent name already exists: %s",
                                     requested);
        return -1;
    }

    ds4_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = mgr->next_id++;
    /* Parse tool-access policy from request; default to parent policy. */
    if (req && req->allowed_tools && req->allowed_tools[0]) {
        if (ds4_agent_tool_policy_parse(req->allowed_tools, &slot->tool_policy) != 0) {
            ds4_agent_subagents_set_error(mgr,
                "invalid tool-access policy '%s' for subagent %s",
                req->allowed_tools, requested);
            ds4_agent_subagent_slot_free(slot);
            mgr->len--;
            return -1;
        }
    } else {
        agent_session_slot *active = ds4_agent_subagents_active_slot(mgr);
        if (active)
            slot->tool_policy = active->tool_policy;
        else
            slot->tool_policy.allow_none = true;
    }
    slot->autonomy = req ? req->autonomy : DS4_AGENT_SUBAGENT_AUTONOMY_TAB;
    slot->budget_limit = req &&
        (req->round_budget > 0 || req->round_budget == -1) ?
        req->round_budget : mgr->default_round_budget;
    snprintf(slot->name, sizeof(slot->name), "%s", requested);

    agent_session_slot *active = ds4_agent_subagents_active_slot(mgr);
    const agent_config *base = active ?
        (active->has_worker ? active->worker.cfg : &active->cfg) : NULL;
    ds4_agent_subagent_config_copy(&slot->cfg, base);
    if (req && req->think_mode_set)
        slot->cfg.gen.think_mode = req->think_mode;
    if (slot->cfg.gen.ctx_size <= 0 && mgr->default_ctx_size > 0)
        slot->cfg.gen.ctx_size = mgr->default_ctx_size;

    if (mgr->engine) {
        if (agent_worker_init(&slot->worker, mgr->engine, &slot->cfg) != 0) {
            ds4_agent_subagents_set_error(mgr, "failed to initialize subagent %s",
                                         slot->name);
            ds4_agent_subagent_slot_free(slot);
            mgr->len--;
            return -1;
        }
        slot->has_worker = true;
        slot->worker.model_gate = &mgr->model_gate;
        slot->worker.model_tool_round_budget =
            slot->autonomy == DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS ?
            slot->budget_limit : 0;
        slot->worker.subagents_disabled = true;
        /* Sync tool-access policy to worker for dispatch enforcement. */
        slot->worker.tool_policy = slot->tool_policy;
    }

    if (!mgr->active_id) mgr->active_id = slot->id.value;
    if (out) *out = slot->id;
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_CREATED,
                                  slot, "created subagent");
    if (req && req->prompt && req->prompt[0]) {
        char *mission = ds4_agent_subagent_mission_envelope(
            slot->name, req->prompt, slot->autonomy, &slot->tool_policy,
            slot->budget_limit, req->stop_conditions, req->report_format);
        agent_prompt_queue_push(&slot->queue, mission);
        free(mission);
    }
    return 0;
}

int ds4_agent_subagent_send(ds4_agent_subagents *mgr,
                           ds4_agent_subagent_id id,
                           const char *prompt) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds4_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    if (!prompt || !prompt[0]) {
        ds4_agent_subagents_set_error(mgr, "empty prompt");
        return -1;
    }
    if (slot->has_worker && !worker_is_idle(&slot->worker)) {
        ds4_agent_subagents_set_error(mgr, "subagent %s is busy", slot->name);
        return -1;
    }
    agent_prompt_queue_push(&slot->queue, prompt);
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "prompt queued");
    return 0;
}

int ds4_agent_subagent_stop(ds4_agent_subagents *mgr, ds4_agent_subagent_id id) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds4_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    snprintf(slot->stop_reason, sizeof(slot->stop_reason), "interrupted");
    if (slot->has_worker) worker_interrupt(&slot->worker);
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "stop requested");
    return 0;
}

int ds4_agent_subagent_close(ds4_agent_subagents *mgr, ds4_agent_subagent_id id) {
    if (!mgr) return -1;
    size_t idx = SIZE_MAX;
    for (size_t i = 0; i < mgr->len; i++) {
        if (ds4_agent_subagent_id_equal(mgr->slots[i].id, id)) {
            idx = i;
            break;
        }
    }
    if (idx == SIZE_MAX) {
        ds4_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    if (mgr->len == 1) {
        ds4_agent_subagents_set_error(mgr, "cannot close the last session");
        return -1;
    }
    agent_session_slot closing = mgr->slots[idx];
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_CLOSED,
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
    ds4_agent_subagent_slot_free(&mgr->slots[idx]);
    for (size_t i = idx + 1; i < mgr->len; i++)
        mgr->slots[i - 1] = mgr->slots[i];
    mgr->len--;
    if (mgr->len < mgr->cap)
        memset(&mgr->slots[mgr->len], 0, sizeof(mgr->slots[mgr->len]));
    return 0;
}

int ds4_agent_subagent_switch(ds4_agent_subagents *mgr, ds4_agent_subagent_id id) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (!slot) {
        ds4_agent_subagents_set_error(mgr, "unknown subagent id");
        return -1;
    }
    mgr->active_id = id.value;
    slot->unread = false;
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_STATUS,
                                  slot, "switched active session");
    return 0;
}

int ds4_agent_subagent_list(ds4_agent_subagents *mgr,
                           ds4_agent_subagent_status *out,
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
        out[i].state = ds4_agent_subagent_state_from_worker(slot, &st);
        out[i].autonomy = slot->autonomy;
        out[i].ctx_used = st.ctx_used;
        out[i].ctx_size = st.ctx_size;
        out[i].dirty = slot->has_worker && slot->worker.session_dirty;
        out[i].queued_output = slot->unread || slot->background.len > 0 ||
            slot->manager_output.len > 0;
        out[i].queued_output_bytes =
            slot->background.len + slot->manager_output.len;
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

int ds4_agent_subagent_report(ds4_agent_subagents *mgr,
                             ds4_agent_subagent_id id,
                             char *buf,
                             size_t len) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (!slot || !buf || len == 0) return -1;
    const char *report = slot->report && slot->report[0] ?
        slot->report : "No subagent report is available yet.";
    snprintf(buf, len, "%s", report);
    return 0;
}

int ds4_agent_subagent_import_report(ds4_agent_subagents *mgr,
                                    ds4_agent_subagent_id from,
                                    ds4_agent_subagent_id into) {
    agent_session_slot *src = ds4_agent_subagents_slot_by_id(mgr, from);
    agent_session_slot *dst = ds4_agent_subagents_slot_by_id(mgr, into);
    if (!src || !dst) {
        ds4_agent_subagents_set_error(mgr, "unknown subagent id");
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
    ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_REPORT,
                                  dst, "report imported");
    return 0;
}

static bool ds4_agent_subagent_refresh_report(agent_session_slot *slot,
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

void ds4_agent_subagents_submit_ready(ds4_agent_subagents *mgr) {
    if (!mgr) return;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (slot->id.value == mgr->active_id) continue;
        if (!slot->has_worker || !slot->queue.len || !worker_is_idle(&slot->worker))
            continue;
        char *queued = agent_prompt_queue_take_all(&slot->queue);
        if (worker_submit(&slot->worker, queued)) {
            ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_STATUS,
                                          slot, "submitted queued prompt");
        } else {
            agent_prompt_queue_push_front(&slot->queue, queued);
            queued = NULL;
        }
        free(queued);
    }
}

size_t ds4_agent_subagents_worker_count(ds4_agent_subagents *mgr) {
    if (!mgr) return 0;
    size_t n = 0;
    for (size_t i = 0; i < mgr->len; i++)
        if (mgr->slots[i].has_worker) n++;
    return n;
}

static agent_session_slot *ds4_agent_subagents_worker_slot_at(ds4_agent_subagents *mgr,
                                                            size_t worker_idx) {
    if (!mgr) return NULL;
    size_t seen = 0;
    for (size_t i = 0; i < mgr->len; i++) {
        if (!mgr->slots[i].has_worker) continue;
        if (seen++ == worker_idx) return &mgr->slots[i];
    }
    return NULL;
}

int ds4_agent_subagents_worker_fd_at(ds4_agent_subagents *mgr,
                                    size_t worker_idx) {
    agent_session_slot *slot =
        ds4_agent_subagents_worker_slot_at(mgr, worker_idx);
    return slot && slot->has_worker ? slot->worker.wake_fd[0] : -1;
}

void ds4_agent_subagents_drain_wake_fds(ds4_agent_subagents *mgr,
                                       struct pollfd *pfd,
                                       size_t count) {
    if (!mgr || !pfd) return;
    for (size_t i = 0; i < count; i++) {
        agent_session_slot *slot = ds4_agent_subagents_worker_slot_at(mgr, i);
        if (slot && (pfd[i].revents & POLLIN))
            drain_wake_fd(slot->worker.wake_fd[0]);
    }
}

bool ds4_agent_subagents_check_raw_mode_restore(ds4_agent_subagents *mgr) {
    if (!mgr) return false;
    bool needs = false;
    for (size_t i = 0; i < mgr->len; i++) {
        if (mgr->slots[i].has_worker &&
            worker_check_raw_mode_restore(&mgr->slots[i].worker))
            needs = true;
    }
    return needs;
}

void ds4_agent_subagents_drain_outputs(ds4_agent_subagents *mgr,
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
        char *worker_out = NULL;
        size_t worker_out_len = 0;
        agent_status st = {0};
        worker_consume(&slot->worker, &worker_out, &worker_out_len, &st);
        if (slot->id.value == mgr->active_id) {
            if (active_status) *active_status = st;
            if (worker_out || worker_out_len || slot->manager_output.len) {
                agent_buf combined = {0};
                ds4_agent_subagent_append_combined_output(&combined, slot,
                                                         worker_out,
                                                         worker_out_len);
                size_t combined_len = combined.len;
                if (active_out) *active_out = agent_buf_take(&combined);
                else free(combined.ptr);
                if (active_out_len)
                    *active_out_len = combined_len;
            }
            if (ds4_agent_subagent_refresh_report(slot, worker_out,
                                                 worker_out_len, &st))
                ds4_agent_subagents_push_event(mgr,
                    DS4_AGENT_SUBAGENT_EVENT_REPORT, slot, "report available");
        } else if (worker_out_len || slot->manager_output.len) {
            ds4_agent_subagent_append_combined_output(&slot->background, slot,
                                                     worker_out,
                                                     worker_out_len);
            slot->unread = true;
            if (ds4_agent_subagent_refresh_report(slot, worker_out,
                                                 worker_out_len, &st))
                ds4_agent_subagents_push_event(mgr,
                    DS4_AGENT_SUBAGENT_EVENT_REPORT, slot, "report available");
            ds4_agent_subagents_push_event(mgr, DS4_AGENT_SUBAGENT_EVENT_OUTPUT,
                                          slot, "background output queued");
        }
        free(slot->manager_output.ptr);
        slot->manager_output.ptr = NULL;
        slot->manager_output.len = 0;
        slot->manager_output.cap = 0;
        if (slot->has_worker &&
            (st.state == AGENT_WORKER_ERROR || st.state == AGENT_WORKER_STOPPED)) {
            char line[192];
            snprintf(line, sizeof(line), "\n[subagent %s] %s\n", slot->name,
                     st.state == AGENT_WORKER_ERROR ?
                     (st.error[0] ? st.error : "worker error") : "stopped");
            agent_buf_puts(&notes, line);
        }
        free(worker_out);
    }
    ds4_agent_subagents_update_worker_metadata(mgr);
    if (active_status) {
        agent_session_slot *active = ds4_agent_subagents_active_slot(mgr);
        if (active && active->has_worker)
            worker_get_status(&active->worker, active_status);
    }
    if (notifications && notes.ptr && notes.len)
        *notifications = agent_buf_take(&notes);
    free(notes.ptr);
}

char *ds4_agent_subagents_take_active_replay(ds4_agent_subagents *mgr) {
    agent_session_slot *slot = ds4_agent_subagents_active_slot(mgr);
    if (!slot || !slot->background.len) return NULL;
    char *out = agent_buf_take(&slot->background);
    slot->unread = false;
    return out;
}

bool ds4_agent_subagents_take_web_approval(ds4_agent_subagents *mgr,
                                          ds4_agent_subagent_id *id,
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
            ds4_agent_subagents_push_event(mgr,
                DS4_AGENT_SUBAGENT_EVENT_APPROVAL, slot, "web approval requested");
            return true;
        }
    }
    return false;
}

void ds4_agent_subagents_answer_web_approval(ds4_agent_subagents *mgr,
                                            ds4_agent_subagent_id id,
                                            bool allow,
                                            const char *err) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (slot && slot->has_worker)
        worker_answer_web_approval(&slot->worker, allow, err);
}

bool ds4_agent_subagents_take_path_approval(ds4_agent_subagents *mgr,
                                           ds4_agent_subagent_id *id,
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
            ds4_agent_subagents_push_event(mgr,
                DS4_AGENT_SUBAGENT_EVENT_APPROVAL, slot, "path approval requested");
            return true;
        }
    }
    return false;
}

void ds4_agent_subagents_answer_path_approval(ds4_agent_subagents *mgr,
                                             ds4_agent_subagent_id id,
                                             bool allow,
                                             const char *choice,
                                             const char *err) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (slot && slot->has_worker)
        worker_answer_path_approval(&slot->worker, allow, choice, err);
}

bool ds4_agent_subagents_take_question(ds4_agent_subagents *mgr,
                                       ds4_agent_subagent_id *id,
                                       char *msg,
                                       size_t msg_len,
                                       char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                       int *choice_count) {
    if (!mgr || !msg || msg_len == 0) return false;
    for (size_t i = 0; i < mgr->len; i++) {
        agent_session_slot *slot = &mgr->slots[i];
        if (!slot->has_worker) continue;
        char inner[AGENT_ASK_QUESTION_TEXT_MAX] = {0};
        if (worker_take_question_request(&slot->worker, inner, sizeof(inner),
                                         choices, choice_count)) {
            if (id) *id = slot->id;
            snprintf(msg, msg_len, "[subagent %s] %s", slot->name, inner);
            ds4_agent_subagents_push_event(mgr,
                DS4_AGENT_SUBAGENT_EVENT_APPROVAL, slot, "question requested");
            return true;
        }
    }
    return false;
}

void ds4_agent_subagents_answer_question(ds4_agent_subagents *mgr,
                                         ds4_agent_subagent_id id,
                                         bool interrupted,
                                         const char *answer,
                                         const char *err) {
    agent_session_slot *slot = ds4_agent_subagents_slot_by_id(mgr, id);
    if (slot && slot->has_worker)
        worker_answer_question(&slot->worker, interrupted, answer, err);
}

bool ds4_agent_subagents_take_queued_user_drain(ds4_agent_subagents *mgr) {
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

static void ds4_agent_subagent_print_list(ds4_agent_subagents *mgr) {
    size_t n = 0;
    ds4_agent_subagent_list(mgr, NULL, 0, &n);
    ds4_agent_subagent_status *items = xmalloc((n ? n : 1) * sizeof(items[0]));
    ds4_agent_subagent_list(mgr, items, n, &n);
    ds4_agent_subagents_active_printf(mgr, "subagents:\n");
    for (size_t i = 0; i < n; i++) {
        char used[32], total[32];
        agent_format_ctx_size(items[i].ctx_used, used, sizeof(used));
        agent_format_ctx_size(items[i].ctx_size, total, sizeof(total));
        ds4_agent_subagents_active_printf(
            mgr, "%c %" PRIu64 " %-16s %-14s ctx %s/%s%s%s%s%s",
            items[i].active ? '*' : ' ',
            items[i].id.value,
            items[i].name,
            ds4_agent_subagent_state_name(items[i].state),
            used,
            total,
            items[i].dirty ? " dirty" : "",
            items[i].queued_output ? " unread" : "",
            items[i].approval_blocked ? " approval" : "",
            items[i].report_available ? " report" : "");
        ds4_agent_subagents_active_printf(
            mgr, " thinking %s",
            ds4_agent_subagent_think_mode_name(items[i].think_mode));
        if (items[i].budget_limit > 0)
            ds4_agent_subagents_active_printf(
                mgr, " budget %d/%d",
                items[i].budget_used, items[i].budget_limit);
        ds4_agent_subagents_active_printf(mgr, "\n");
    }
    free(items);
}

static void ds4_agent_subagent_emit_usage(ds4_agent_subagents *mgr) {
    /* Save/path/web approvals remain runtime-owned prompts, but ordinary
     * subagent command output should stay inside session-scoped buffers. */
    ds4_agent_subagents_active_printf(mgr, "usage:\n");
    ds4_agent_subagents_active_printf(
        mgr,
        "  /subagent new [--tab|--background|--auto] [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent list\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent switch <id|name>\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent send <id|name> <prompt>\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent stop <id|name>\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent close <id|name>\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent report <id|name>\n");
    ds4_agent_subagents_active_printf(mgr, "  /subagent import <id|name>\n");
}

static char *ds4_agent_subagent_next_arg(char **args) {
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

bool ds4_agent_subagents_handle_command(ds4_agent_subagents *mgr,
                                       char *cmd,
                                       bool busy) {
    if (!mgr || !cmd || !agent_slash_command_with_args(cmd, "/subagent"))
        return false;
    char *args = cmd + strlen("/subagent");
    char *op = ds4_agent_subagent_next_arg(&args);
    if (!op || !strcmp(op, "help")) {
        ds4_agent_subagent_emit_usage(mgr);
        return true;
    }
    if (!strcmp(op, "list")) {
        ds4_agent_subagent_print_list(mgr);
        return true;
    }
    if (!strcmp(op, "new")) {
        if (!ds4_agent_subagents_active_is_main(mgr)) {
            ds4_agent_subagents_active_printf(mgr,
                                             "subagents cannot create nested subagents\n");
            return true;
        }
        ds4_agent_subagent_autonomy mode = DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND;
        ds4_think_mode think_mode = DS4_THINK_HIGH;
        bool think_mode_set = false;
        int round_budget = 0;
        bool round_budget_set = false;
        char *allowed_tools = NULL;
        char *name = NULL;
        for (;;) {
            char *arg = ds4_agent_subagent_next_arg(&args);
            if (!arg) break;
            if (!strcmp(arg, "--tab")) {
                mode = DS4_AGENT_SUBAGENT_AUTONOMY_TAB;
                continue;
            }
            if (!strcmp(arg, "--background")) {
                mode = DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND;
                continue;
            }
            if (!strcmp(arg, "--auto")) {
                mode = DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS;
                continue;
            }
            if (!strcmp(arg, "--budget")) {
                char *value = ds4_agent_subagent_next_arg(&args);
                if (!ds4_agent_subagent_parse_round_budget(value,
                                                            &round_budget)) {
                    ds4_agent_subagents_active_printf(
                        mgr,
                        "usage: /subagent new --auto [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
                    return true;
                }
                round_budget_set = true;
                continue;
            }
            if (!strncmp(arg, "--budget=", 9)) {
                if (!ds4_agent_subagent_parse_round_budget(arg + 9,
                                                            &round_budget)) {
                    ds4_agent_subagents_active_printf(
                        mgr,
                        "usage: /subagent new --auto [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
                    return true;
                }
                round_budget_set = true;
                continue;
            }
            if (!strcmp(arg, "--thinking")) {
                char *value = ds4_agent_subagent_next_arg(&args);
                if (!ds4_agent_subagent_parse_think_mode(value, &think_mode)) {
                    ds4_agent_subagents_active_printf(
                        mgr,
                        "usage: /subagent new [--tab|--background|--auto] [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
                    return true;
                }
                think_mode_set = true;
                continue;
            }
            if (!strncmp(arg, "--thinking=", 11)) {
                if (!ds4_agent_subagent_parse_think_mode(arg + 11, &think_mode)) {
                    ds4_agent_subagents_active_printf(
                        mgr,
                        "usage: /subagent new [--tab|--background|--auto] [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
                    return true;
                }
                think_mode_set = true;
                continue;
            }
            if (!strcmp(arg, "--tools") || !strcmp(arg, "--allowed-tools")) {
                char *value = ds4_agent_subagent_next_arg(&args);
                if (!value) {
                    ds4_agent_subagents_active_printf(
                        mgr,
                        "usage: /subagent new [--tab|--background|--auto] [--budget N] [--thinking off|default|max] [--tools POLICY] [name] [prompt]\n");
                    return true;
                }
                allowed_tools = value;
                continue;
            }
            if (!strncmp(arg, "--tools=", 8)) {
                allowed_tools = arg + 8;
                continue;
            }
            if (!strncmp(arg, "--allowed-tools=", 16)) {
                allowed_tools = arg + 16;
                continue;
            }
            name = arg;
            break;
        }
        if (round_budget_set &&
            mode != DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS) {
            ds4_agent_subagents_active_printf(mgr,
                                             "--budget requires --auto\n");
            return true;
        }
        while (args && (*args == ' ' || *args == '\t')) args++;
        const char *prompt = args && args[0] ? args : NULL;
        if (!prompt && mode == DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND)
            mode = DS4_AGENT_SUBAGENT_AUTONOMY_TAB;
        ds4_agent_subagent_create_request req = {
            .name = name,
            .prompt = prompt,
            .autonomy = mode,
            .think_mode = think_mode,
            .think_mode_set = think_mode_set,
            .allowed_tools = allowed_tools,
            .round_budget = round_budget
        };
        ds4_agent_subagent_id id = {0};
        if (ds4_agent_subagent_create(mgr, &req, &id) != 0)
            ds4_agent_subagents_active_printf(
                mgr, "subagent new failed: %s\n",
                ds4_agent_subagents_last_error(mgr));
        else
            ds4_agent_subagents_active_printf(
                mgr,
                "created subagent %" PRIu64 " %s%s\n", id.value,
                ds4_agent_subagents_slot_by_id(mgr, id)->name,
                prompt ? "; running in background" : "");
        return true;
    }
    if (!strcmp(op, "switch")) {
        char *which = ds4_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds4_agent_subagents_resolve(mgr, which);
        if (!slot) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent switch failed: unknown session %s\n",
                which ? which : "");
        } else if (ds4_agent_subagent_switch(mgr, slot->id) != 0) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent switch failed: %s\n",
                ds4_agent_subagents_last_error(mgr));
        } else {
            ds4_agent_subagent_slot_printf(slot, "switched to subagent %s\n",
                                          slot->name);
        }
        return true;
    }
    if (!strcmp(op, "send")) {
        char *which = ds4_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds4_agent_subagents_resolve(mgr, which);
        while (args && (*args == ' ' || *args == '\t')) args++;
        if (!slot || !args || !args[0]) {
            ds4_agent_subagents_active_printf(
                mgr, "usage: /subagent send <id|name> <prompt>\n");
        } else if (ds4_agent_subagent_send(mgr, slot->id, args) != 0) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent send failed: %s\n",
                ds4_agent_subagents_last_error(mgr));
        } else {
            ds4_agent_subagents_active_printf(
                mgr, "sent prompt to subagent %s\n", slot->name);
        }
        return true;
    }
    if (!strcmp(op, "stop")) {
        char *which = ds4_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds4_agent_subagents_resolve(mgr, which);
        if (!slot) ds4_agent_subagents_active_printf(
                       mgr, "subagent stop failed: unknown session\n");
        else if (ds4_agent_subagent_stop(mgr, slot->id) != 0)
            ds4_agent_subagents_active_printf(
                mgr, "subagent stop failed: %s\n",
                ds4_agent_subagents_last_error(mgr));
        else ds4_agent_subagents_active_printf(
                 mgr, "stop requested for subagent %s\n", slot->name);
        return true;
    }
    if (!strcmp(op, "close")) {
        char *which = ds4_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds4_agent_subagents_resolve(mgr, which);
        if (!slot) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent close failed: unknown session\n");
        } else if (slot->has_worker && !worker_is_idle(&slot->worker)) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent close failed: %s is busy; stop it first\n",
                slot->name);
        } else {
            if (slot->has_worker && agent_worker_needs_save(&slot->worker)) {
                /* Keep the confirmation prompt on the runtime-owned interactive
                 * path; only the follow-up informational text is buffered. */
                if (agent_prompt_yes_no("Save subagent before closing? (y/n) ")) {
                    char err[160] = {0};
                    if (!agent_worker_save_session(&slot->worker, err, sizeof(err))) {
                        ds4_agent_subagents_active_printf(
                            mgr, "save failed: %s\n", err);
                        return true;
                    }
                }
            }
            char closed[DS4_AGENT_SUBAGENT_NAME_MAX];
            snprintf(closed, sizeof(closed), "%s", slot->name);
            if (ds4_agent_subagent_close(mgr, slot->id) != 0)
                ds4_agent_subagents_active_printf(
                    mgr, "subagent close failed: %s\n",
                    ds4_agent_subagents_last_error(mgr));
            else
                ds4_agent_subagents_active_printf(
                    mgr, "closed subagent %s\n", closed);
        }
        return true;
    }
    if (!strcmp(op, "report") || !strcmp(op, "import")) {
        char *which = ds4_agent_subagent_next_arg(&args);
        agent_session_slot *slot = ds4_agent_subagents_resolve(mgr, which);
        if (!slot) {
            ds4_agent_subagents_active_printf(
                mgr, "subagent %s failed: unknown session\n", op);
            return true;
        }
        if (!strcmp(op, "report")) {
            char report[4096];
            if (ds4_agent_subagent_report(mgr, slot->id, report, sizeof(report)) != 0)
                ds4_agent_subagents_active_printf(mgr, "subagent report failed\n");
            else
                ds4_agent_subagents_active_printf(mgr, "%s\n", report);
        } else {
            agent_session_slot *active = ds4_agent_subagents_active_slot(mgr);
            if (!active || ds4_agent_subagent_import_report(mgr, slot->id,
                                                           active->id) != 0)
                ds4_agent_subagents_active_printf(
                    mgr, "subagent import failed: %s\n",
                    ds4_agent_subagents_last_error(mgr));
            else if (busy)
                ds4_agent_subagent_slot_printf(
                    active, "report queued for active session\n");
            else
                ds4_agent_subagent_slot_printf(
                    active, "report imported into active session\n");
        }
        return true;
    }
    ds4_agent_subagent_emit_usage(mgr);
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
    w->initialized = true;
    w->status.state = AGENT_WORKER_IDLE;
}

static agent_session_slot *test_agent_subagent_add_fake_slot(
    ds4_agent_subagents *mgr,
    const char *name,
    uint64_t id) {
    ds4_agent_subagents_ensure_slot_cap(mgr);
    agent_session_slot *slot = &mgr->slots[mgr->len++];
    memset(slot, 0, sizeof(*slot));
    slot->id.value = id;
    slot->autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB;
    slot->budget_limit = 9;
    slot->tool_policy.allow_none = true;
    snprintf(slot->name, sizeof(slot->name), "%s", name);
    slot->cfg.gen.ctx_size = 1234;
    slot->cfg.gen.think_mode = DS4_THINK_HIGH;
    test_agent_fake_worker_init(&slot->worker, &slot->cfg);
    slot->worker.tool_policy = slot->tool_policy;
    slot->has_worker = true;
    return slot;
}

static void test_agent_subagent_create_think_mode_inherits_or_overrides(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds4_agent_subagent_id main_id = {0};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_MAX,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    ds4_agent_subagent_id inherited_id = {0};
    ds4_agent_subagent_create_request inherited_req = {
        .name = "inherit",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &inherited_req, &inherited_id) == 0);

    ds4_agent_subagent_id explicit_id = {0};
    ds4_agent_subagent_create_request explicit_req = {
        .name = "explicit",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_NONE,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &explicit_req, &explicit_id) == 0);

    ds4_agent_subagent_status st[4];
    size_t n = 0;
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, st, 4, &n) == 0);
    AGENT_TEST_ASSERT(n == 3);
    AGENT_TEST_ASSERT(st[0].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[1].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[2].think_mode == DS4_THINK_NONE);
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_new_command_accepts_thinking_flag(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds4_agent_subagent_id main_id = {0};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
        .think_mode = DS4_THINK_MAX,
        .think_mode_set = true,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    char cmd1[] = "/subagent new --thinking off explicit";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd1, false));
    AGENT_TEST_ASSERT(mgr->len == 2);
    AGENT_TEST_ASSERT(mgr->slots[1].cfg.gen.think_mode == DS4_THINK_NONE);

    char cmd2[] = "/subagent new inherited";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd2, false));
    AGENT_TEST_ASSERT(mgr->len == 3);
    AGENT_TEST_ASSERT(mgr->slots[2].cfg.gen.think_mode == DS4_THINK_MAX);

    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_new_command_accepts_tools_flag(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds4_agent_subagent_id main_id = {0};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    char cmd1[] = "/subagent new --tools read,web scout inspect docs";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd1, false));
    AGENT_TEST_ASSERT(mgr->len == 2);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(
        &mgr->slots[1].tool_policy, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(
        &mgr->slots[1].tool_policy, "web_browse"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(
        &mgr->slots[1].tool_policy, "bash"));

    char cmd2[] = "/subagent new --tools=not_a_tool bad";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd2, false));
    AGENT_TEST_ASSERT(mgr->len == 2);

    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_new_command_accepts_auto_budget(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    AGENT_TEST_ASSERT(mgr->default_round_budget ==
                      DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET);

    ds4_agent_subagent_id main_id = {0};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    char custom[] = "/subagent new --auto --budget 42 scout inspect docs";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, custom, false));
    AGENT_TEST_ASSERT(mgr->len == 2);
    AGENT_TEST_ASSERT(mgr->slots[1].autonomy ==
                      DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS);
    AGENT_TEST_ASSERT(mgr->slots[1].budget_limit == 42);
    AGENT_TEST_ASSERT(mgr->slots[1].queue.len == 1);
    AGENT_TEST_ASSERT(strstr(mgr->slots[1].queue.v[0],
                             "Budget: stop after 42 model/tool rounds.") != NULL);

    char default_budget[] = "/subagent new --auto default inspect tests";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(
        mgr, default_budget, false));
    AGENT_TEST_ASSERT(mgr->len == 3);
    AGENT_TEST_ASSERT(mgr->slots[2].budget_limit ==
                      DS4_AGENT_DEFAULT_MODEL_TOOL_ROUND_BUDGET);
    AGENT_TEST_ASSERT(mgr->slots[2].budget_limit == -1);
    AGENT_TEST_ASSERT(strstr(mgr->slots[2].queue.v[0],
                             "Budget: disabled; no model/tool round limit.") != NULL);

    char disabled[] = "/subagent new --auto --budget -1 unlimited inspect repo";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, disabled, false));
    AGENT_TEST_ASSERT(mgr->len == 4);
    AGENT_TEST_ASSERT(mgr->slots[3].budget_limit == -1);
    AGENT_TEST_ASSERT(strstr(mgr->slots[3].queue.v[0],
                             "Budget: disabled; no model/tool round limit.") != NULL);

    char invalid_mode[] = "/subagent new --budget 7 manual";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(
        mgr, invalid_mode, false));
    AGENT_TEST_ASSERT(mgr->len == 4);

    char invalid_value[] = "/subagent new --auto --budget 0 invalid";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(
        mgr, invalid_value, false));
    AGENT_TEST_ASSERT(mgr->len == 4);

    char invalid_negative[] = "/subagent new --auto --budget -2 invalid";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(
        mgr, invalid_negative, false));
    AGENT_TEST_ASSERT(mgr->len == 4);

    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_switch_preserves_thinking_modes(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    agent_session_slot *beta =
        test_agent_subagent_add_fake_slot(mgr, "beta", 2);
    mgr->active_id = main_slot->id.value;

    main_slot->cfg.gen.think_mode = DS4_THINK_MAX;
    main_slot->worker.status.think_mode = DS4_THINK_MAX;
    beta->cfg.gen.think_mode = DS4_THINK_NONE;
    beta->worker.status.think_mode = DS4_THINK_NONE;

    AGENT_TEST_ASSERT(ds4_agent_subagent_switch(mgr, beta->id) == 0);

    ds4_agent_subagent_status st[2];
    size_t n = 0;
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, st, 2, &n) == 0);
    AGENT_TEST_ASSERT(n == 2);
    AGENT_TEST_ASSERT(!st[0].active);
    AGENT_TEST_ASSERT(st[0].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[1].active);
    AGENT_TEST_ASSERT(st[1].think_mode == DS4_THINK_NONE);

    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_slot_isolation(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
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
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_background_replay(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
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
    ds4_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(active_out == NULL || active_out_len == 0);
    AGENT_TEST_ASSERT(notes == NULL || notes[0] == '\0');
    AGENT_TEST_ASSERT(bg->unread);
    free(active_out);
    free(notes);

    AGENT_TEST_ASSERT(ds4_agent_subagent_switch(mgr, bg->id) == 0);
    char *replay = ds4_agent_subagents_take_active_replay(mgr);
    AGENT_TEST_ASSERT(replay != NULL);
    AGENT_TEST_ASSERT(!strcmp(replay, "background chunk\n"));
    AGENT_TEST_ASSERT(!bg->unread);
    free(replay);
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_command_output_drains_for_active_slot(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    mgr->active_id = main_slot->id.value;

    char cmd[] = "/subagent list";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd, false));

    char *active_out = NULL;
    size_t active_out_len = 0;
    agent_status st = {0};
    char *notes = NULL;
    ds4_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(active_out != NULL);
    AGENT_TEST_ASSERT(strstr(active_out, "subagents:\n") != NULL);
    AGENT_TEST_ASSERT(strstr(active_out, "main") != NULL);
    AGENT_TEST_ASSERT(main_slot->manager_output.len == 0);
    free(active_out);
    free(notes);
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_command_output_isolates_background_slot(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    agent_session_slot *main_slot =
        test_agent_subagent_add_fake_slot(mgr, "main", 1);
    agent_session_slot *beta =
        test_agent_subagent_add_fake_slot(mgr, "beta", 2);
    mgr->active_id = main_slot->id.value;

    char cmd[] = "/subagent send beta investigate";
    AGENT_TEST_ASSERT(ds4_agent_subagents_handle_command(mgr, cmd, false));

    char *active_out = NULL;
    size_t active_out_len = 0;
    agent_status st = {0};
    char *notes = NULL;
    ds4_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(active_out != NULL);
    AGENT_TEST_ASSERT(strstr(active_out,
                             "sent prompt to subagent beta\n") != NULL);
    AGENT_TEST_ASSERT(!main_slot->background.ptr || !main_slot->background.ptr[0]);
    AGENT_TEST_ASSERT(!beta->background.ptr || !beta->background.ptr[0]);
    AGENT_TEST_ASSERT(!beta->unread);
    free(active_out);
    free(notes);
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_error_notifications_remain_visible(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
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
    ds4_agent_subagents_drain_outputs(mgr, &active_out, &active_out_len,
                                     &st, &notes);
    AGENT_TEST_ASSERT(notes != NULL);
    AGENT_TEST_ASSERT(strstr(notes, "[subagent bg] worker error") != NULL);
    free(active_out);
    free(notes);
    ds4_agent_subagents_destroy(mgr);
}

void ds4_agent_subagent_unit_tests_run(void) {
    test_agent_subagent_slot_isolation();
    test_agent_subagent_background_replay();
    test_agent_subagent_command_output_drains_for_active_slot();
    test_agent_subagent_command_output_isolates_background_slot();
    test_agent_subagent_error_notifications_remain_visible();
    test_agent_subagent_create_think_mode_inherits_or_overrides();
    test_agent_subagent_new_command_accepts_thinking_flag();
    test_agent_subagent_new_command_accepts_tools_flag();
    test_agent_subagent_new_command_accepts_auto_budget();
    test_agent_subagent_switch_preserves_thinking_modes();
}
#endif
