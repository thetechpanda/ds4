#include "ds4.h"
#include "ds4_distributed.h"
#include "ds4_help.h"
#include "ds4_kvstore.h"
#include "ds4_web.h"
#include "ds4_agent_internal.h"
#include "linenoise.h"

#include <errno.h>
#include <ctype.h>
#include <dirent.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <poll.h>
#include <pthread.h>
#include <regex.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* This is intentionally not in linenoise.h, but it is part of the existing
 * multiplexed editor implementation.  The agent uses it only to restore text
 * after Enter is pressed while the model is still busy. */
int linenoiseEditInsert(struct linenoiseState *l, const char *c, size_t clen);

static int set_nonblock(int fd, bool on, int *old_flags);
static bool agent_parse_bool_default(const char *s, bool def);
static bool agent_executable_exists(const char *path);
static bool agent_command_in_path(const char *command);
static bool agent_bash_use_docker_sandbox(const agent_worker *w);
static void agent_bash_jobs_free(agent_worker *w);
static bool agent_prompt_ask_question(const char *prompt,
                                      char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                      int choice_count,
                                      char answer[AGENT_ASK_QUESTION_ANSWER_MAX],
                                      bool *interrupted);

/* ============================================================================
 * Configuration, Worker State, And Streaming Types
 * ============================================================================
 *
 * The agent is intentionally a single process: the UI thread owns terminal
 * input/output, while the worker thread owns the live DS4 session and KV state.
 * These types define the shared state and the small streaming state machines
 * used to render sampled assistant text and DSML tool calls as they arrive.
 */

static unsigned agent_next_prefill_label(void);
static const agent_path_list *agent_working_directories(const agent_worker *w);

typedef struct agent_tail_capture {
    char *buf;
    size_t cap;
    size_t start;
    size_t len;
    size_t total;
} agent_tail_capture;

typedef enum {
    AGENT_MD_PENDING_NONE,
    AGENT_MD_PENDING_STAR,
    AGENT_MD_PENDING_BACKTICK,
} agent_markdown_pending;

typedef struct agent_syntax agent_syntax;

typedef struct {
    ds4_engine *engine;
    agent_worker *worker;
    bool format_thinking;
    bool format_markdown;
    bool in_think;
    bool color_open;
    bool use_color;
    bool last_output_newline;
    bool wrote_visible_output;
    bool md_bold;
    bool md_italic;
    bool md_inline_code;
    bool md_code_block;
    bool md_fence_info;
    bool md_code_line_start;
    bool md_code_in_ml_comment;
    bool md_syntax_silent;
    bool md_syntax_has_highlight;
    agent_markdown_pending md_pending;
    size_t md_pending_len;
    const agent_syntax *md_syntax;
    char md_fence_lang[32];
    size_t md_fence_lang_len;
    const char *md_code_line_prefix;
    const char *md_code_line_prefix_color;
    bool md_code_highlight_upto;
    char *md_code_line;
    size_t md_code_line_len;
    size_t md_code_line_cap;
    char pending[16];
    size_t pending_len;
    char utf8_pending[4];
    size_t utf8_pending_len;
    size_t utf8_pending_need;
    agent_tail_capture *capture;
} agent_token_renderer;

typedef struct {
    char *name;
    char *value;
    bool is_string;
} agent_tool_arg;

typedef struct {
    char *name;
    agent_tool_arg *args;
    int argc;
    int argcap;
} agent_tool_call;

typedef struct {
    agent_tool_call *v;
    int len;
    int cap;
} agent_tool_calls;

typedef enum {
    AGENT_DSML_SEARCH,
    AGENT_DSML_STRUCTURAL,
    AGENT_DSML_PARAM_VALUE,
    AGENT_DSML_DONE,
    AGENT_DSML_ERROR,
} agent_dsml_state;

typedef struct {
    agent_dsml_state state;
    char search_tail[64];
    size_t search_len;
    char *raw;
    size_t raw_len;
    size_t raw_cap;
    size_t parse_pos;
    agent_tool_call current;
    char *param_name;
    bool param_is_string;
    size_t param_value_start;
    bool param_close_prefix;
    agent_tool_calls calls;
    char error[160];
} agent_dsml_parser;

typedef enum {
    AGENT_TOOL_PARAM_NORMAL,
    AGENT_TOOL_PARAM_PATH,
    AGENT_TOOL_PARAM_OFFSET,
    AGENT_TOOL_PARAM_CONTENT,
    AGENT_TOOL_PARAM_DIFF_OLD,
    AGENT_TOOL_PARAM_DIFF_NEW,
    AGENT_TOOL_PARAM_BASH_COMMAND,
} agent_tool_param_kind;

typedef struct {
    bool active;
    bool tool_announced;
    bool param_active;
    bool at_line_start;
    bool last_output_newline;
    agent_tool_param_kind param_kind;
    char tool_name[64];
    char param_name[64];
    char param_end_tail[64];
    size_t param_end_len;
    bool read_style;
    bool read_prefix_rendered;
    bool read_line_rendered;
    char read_path[512];
    char read_start[32];
    char read_max[32];
    char read_whole[8];
    char tool_path[512];
    bool code_param_active;
    bool ask_question_style;
    bool ask_question_question_active;
    bool ask_question_preview_started;
    bool ask_question_preview_done;
    char ask_question_preview[256];
} agent_tool_visualizer;

typedef struct {
    char tail[32];
    size_t len;
} agent_dsml_marker_detector;

typedef struct {
    agent_token_renderer *renderer;
    agent_dsml_parser *parser;
    agent_tool_visualizer viz;
    bool in_think;
    bool dsml_active;
    bool dsml_ignored;
    bool replay;
    char pending[16];
    size_t pending_len;
    char dsml_start_tail[64];
    size_t dsml_start_len;
    agent_dsml_marker_detector plain_dsml;
    agent_dsml_marker_detector think_dsml;
    bool dsml_in_think;
    bool dsml_in_think_reported;
    bool dsml_origin_in_think;
    bool dsml_origin_rendered;
    bool post_think_gap;
    bool tool_preflight_error;
    char tool_preflight_error_msg[256];
} agent_stream_renderer;

typedef struct {
    bool active;
    bool done;
} agent_edit_upto_forcer;

typedef struct {
    bool active;
    size_t head_len;
    size_t tail_offset;
    size_t tail_len;
} agent_edit_anchor_span;

static volatile sig_atomic_t agent_sigint;
static agent_worker *agent_completion_worker;

static void worker_apply_pending_power(agent_worker *w);
static void agent_trace(agent_worker *w, const char *fmt, ...);
static void agent_trace_text(agent_worker *w, const char *label,
                             const char *text, size_t len);
static void agent_publish_system_status(agent_worker *w, const char *msg);
static int agent_web_confirm(void *privdata, const char *message,
                             char *err, size_t err_len);
static void agent_web_log(void *privdata, const char *message);
static bool agent_preflight_edit_old(agent_worker *w, const agent_tool_call *call,
                                     char *err, size_t err_len);
static char *agent_execute_tool_call(agent_worker *w, const agent_tool_call *call);
static char *agent_execute_tool_calls_ordered(agent_worker *w,
                                              const agent_tool_calls *calls,
                                              bool stop_on_interrupt,
                                              bool *interrupted);
static void agent_string_array_free(char **v, int count);
static bool agent_json_parse_string_array(const char *json,
                                          char ***out_v,
                                          int *out_count);
static int agent_worker_sync_tokens(agent_worker *w, const ds4_tokens *tokens,
                                    bool publish_progress,
                                    char *err, size_t err_len);
static int agent_worker_session_sync(agent_worker *w,
                                     const ds4_tokens *tokens,
                                     agent_worker_state resume_state,
                                     char *err,
                                     size_t err_len);
static int agent_worker_session_eval(agent_worker *w,
                                     int token,
                                     agent_worker_state resume_state,
                                     char *err,
                                     size_t err_len);
static int agent_worker_session_set_power(agent_worker *w, int power);

/* ============================================================================
 * Small Utilities And Command-Line Parsing
 * ============================================================================
 */

static void agent_sigint_handler(int sig) {
    (void)sig;
    agent_sigint = 1;
}

void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (!p) {
        perror("ds4-agent: malloc");
        exit(1);
    }
    return p;
}

char *xstrdup(const char *s) {
    if (!s) s = "";
    size_t n = strlen(s);
    char *p = xmalloc(n + 1);
    memcpy(p, s, n + 1);
    return p;
}

char *xstrndup(const char *s, size_t n) {
    char *p = xmalloc(n + 1);
    memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

void *xrealloc(void *ptr, size_t n) {
    void *p = realloc(ptr, n ? n : 1);
    if (!p) {
        perror("ds4-agent: realloc");
        exit(1);
    }
    return p;
}

void agent_path_list_append(agent_path_list *list, const char *path) {
    if (list->len == list->cap) {
        list->cap = list->cap ? list->cap * 2 : 4;
        list->v = xrealloc(list->v, (size_t)list->cap * sizeof(list->v[0]));
    }
    list->v[list->len++] = xstrdup(path ? path : "");
}

static bool agent_path_list_contains(const agent_path_list *list, const char *path) {
    if (!list || !path) return false;
    for (int i = 0; i < list->len; i++) {
        if (list->v[i] && !strcmp(list->v[i], path)) return true;
    }
    return false;
}

static bool agent_path_list_remove(agent_path_list *list, const char *path,
                                   bool prefix) {
    if (!list || !path) return false;
    bool removed = false;
    size_t plen = prefix ? strlen(path) : 0;
    for (int i = 0; i < list->len; ) {
        bool match = false;
        if (list->v[i]) {
            if (prefix) {
                match = strncmp(list->v[i], path, plen) == 0 &&
                    (list->v[i][plen] == '/' || list->v[i][plen] == '\0');
            } else {
                match = strcmp(list->v[i], path) == 0;
            }
        }
        if (match) {
            free(list->v[i]);
            for (int j = i + 1; j < list->len; j++)
                list->v[j - 1] = list->v[j];
            list->len--;
            if (list->len >= 0) list->v[list->len] = NULL;
            removed = true;
            /* Stay at index i to check the shifted-down entry. */
            continue;
        } 
        i++;
    }
    return removed;
}

void agent_path_list_free(agent_path_list *list) {
    if (!list) return;
    for (int i = 0; i < list->len; i++) free(list->v[i]);
    free(list->v);
    memset(list, 0, sizeof(*list));
}

/* ============================================================================
 * Skill System: registration, parsing, listing, prompt building
 *
 * Skills are lightweight metadata records (name, description, path) stored in a
 * linked list owned by the agent worker.  Bodies are NOT cached — the LLM reads
 * skill content through the normal file tools when it sees [skill:<name>](<path>).
 * ============================================================================ */

enum {
    AGENT_SKILL_ERR_IO = -1,
    AGENT_SKILL_ERR_FRONTMATTER = -2,
    AGENT_SKILL_ERR_UTF8 = -3,
    AGENT_SKILL_ERR_NAME = -4,
    AGENT_SKILL_ERR_WORKSPACE = -5,
    AGENT_SKILL_ERR_DUPLICATE = -6,
    AGENT_SKILL_ERR_LINE_TOO_LONG = -7,
};

static agent_skill_registry *agent_skill_registry_create(void) {
    agent_skill_registry *registry = xmalloc(sizeof(*registry));
    memset(registry, 0, sizeof(*registry));
    pthread_mutex_init(&registry->mu, NULL);
    registry->refs = 1;
    return registry;
}

static void agent_skill_registry_acquire(agent_skill_registry *registry) {
    if (!registry) return;
    pthread_mutex_lock(&registry->mu);
    registry->refs++;
    pthread_mutex_unlock(&registry->mu);
}

static void agent_skill_registry_release(agent_skill_registry *registry) {
    if (!registry) return;
    pthread_mutex_lock(&registry->mu);
    if (--registry->refs != 0) {
        pthread_mutex_unlock(&registry->mu);
        return;
    }
    agent_skill *skill = registry->head;
    registry->head = NULL;
    while (skill) {
        agent_skill *next = skill->next;
        free(skill);
        skill = next;
    }
    pthread_mutex_unlock(&registry->mu);
    pthread_mutex_destroy(&registry->mu);
    free(registry);
}

static bool agent_utf8_valid(const unsigned char *s, size_t len) {
    size_t i = 0;
    while (i < len) {
        unsigned char c = s[i++];
        if (c <= 0x7f) continue;
        if (c >= 0xc2 && c <= 0xdf) {
            if (i >= len || (s[i] & 0xc0) != 0x80) return false;
            i++;
            continue;
        }
        if (c >= 0xe0 && c <= 0xef) {
            if (i + 1 >= len || (s[i] & 0xc0) != 0x80 ||
                (s[i + 1] & 0xc0) != 0x80) return false;
            if (c == 0xe0 && s[i] < 0xa0) return false;
            if (c == 0xed && s[i] >= 0xa0) return false;
            i += 2;
            continue;
        }
        if (c >= 0xf0 && c <= 0xf4) {
            if (i + 2 >= len || (s[i] & 0xc0) != 0x80 ||
                (s[i + 1] & 0xc0) != 0x80 ||
                (s[i + 2] & 0xc0) != 0x80) return false;
            if (c == 0xf0 && s[i] < 0x90) return false;
            if (c == 0xf4 && s[i] > 0x8f) return false;
            i += 3;
            continue;
        }
        return false;
    }
    return true;
}

bool agent_skill_name_valid(const char *name) {
    if (!name || !name[0]) return false;
    for (const char *p = name; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') ||
              (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') ||
              *p == '_' || *p == '-'))
            return false;
    }
    return true;
}

static bool agent_skill_path_allowed(const agent_worker *w,
                                     const char *resolved) {
    if (!w || !resolved) return false;
    for (int i = 0; i < w->working_directories.len; i++) {
        size_t dlen = strlen(w->working_directories.v[i]);
        if (strncmp(resolved, w->working_directories.v[i], dlen) == 0 &&
            (resolved[dlen] == '/' || resolved[dlen] == '\0')) return true;
    }
    return false;
}

int agent_skill_parse_file(const char *path, char *name_out, size_t name_size,
                           char *desc_out, size_t desc_size) {
    if (!path || !path[0] || !name_out || name_size < 2 ||
        !desc_out || desc_size < 2) return AGENT_SKILL_ERR_IO;

    /* Open file, read at most 20 lines */
    FILE *fp = fopen(path, "rb");
    if (!fp) return AGENT_SKILL_ERR_IO;

    char lines[20][512];
    int nlines = 0;
    bool frontmatter_open = false;
    while (nlines < 20) {
        if (!fgets(lines[nlines], (int)sizeof(lines[nlines]), fp)) break;
        size_t len = strlen(lines[nlines]);
        if (len > 0 && lines[nlines][len - 1] != '\n' && !feof(fp)) {
            fclose(fp);
            return AGENT_SKILL_ERR_LINE_TOO_LONG;
        }
        if (!agent_utf8_valid((const unsigned char *)lines[nlines], len)) {
            fclose(fp);
            return AGENT_SKILL_ERR_UTF8;
        }
        bool delimiter = strcmp(lines[nlines], "---\n") == 0 ||
                         strcmp(lines[nlines], "---\r\n") == 0;
        nlines++;
        if (delimiter) {
            if (frontmatter_open) break;
            frontmatter_open = true;
        }
    }
    fclose(fp);

    /* Reject if fewer than 3 lines (need ---, frontmatter, ---) */
    if (nlines < 3) return AGENT_SKILL_ERR_FRONTMATTER;

    /* Check first line is "---\n" or "---\r\n" */
    if (strcmp(lines[0], "---\n") != 0 && strcmp(lines[0], "---\r\n") != 0)
        return AGENT_SKILL_ERR_FRONTMATTER;

    /* Find second --- delimiter within first 20 lines */
    int delim_idx = -1;
    for (int i = 1; i < nlines; i++) {
        if (strcmp(lines[i], "---\n") == 0 ||
            strcmp(lines[i], "---\r\n") == 0) {
            delim_idx = i;
            break;
        }
    }
    if (delim_idx < 0) return AGENT_SKILL_ERR_FRONTMATTER;

    /* Parse frontmatter between first --- and second --- */
    char name[AGENT_SKILL_NAME_MAX];
    char desc[AGENT_SKILL_DESC_MAX];
    name[0] = '\0';
    desc[0] = '\0';

    for (int i = 1; i < delim_idx; i++) {
        const char *line = lines[i];
        /* Trim trailing whitespace/newline */
        size_t llen = strlen(line);
        while (llen > 0 && (line[llen-1] == '\n' || line[llen-1] == '\r' ||
               line[llen-1] == ' ' || line[llen-1] == '\t'))
            llen--;
        if (llen == 0) continue;

        /* Parse key: value */
        const char *colon = strchr(line, ':');
        if (!colon) continue;
        size_t key_len = (size_t)(colon - line);
        if (key_len == 0) continue;

        /* Extract key */
        char key[64];
        if (key_len >= sizeof(key)) key_len = sizeof(key) - 1;
        memcpy(key, line, key_len);
        key[key_len] = '\0';

        /* Trim key */
        while (key_len > 0 && (key[key_len-1] == ' ' || key[key_len-1] == '\t'))
            key[--key_len] = '\0';

        /* Value starts after colon */
        const char *val = colon + 1;
        while (*val == ' ' || *val == '\t') val++;
        size_t val_off = (size_t)(val - line);
        if (val_off >= llen) continue;
        size_t vlen = llen - val_off;
        if (vlen == 0) continue;

        if (strcmp(key, "name") == 0) {
            if (vlen >= sizeof(name) || vlen >= name_size)
                return AGENT_SKILL_ERR_LINE_TOO_LONG;
            size_t cp = vlen < name_size - 1 ? vlen : name_size - 1;
            memcpy(name, val, cp);
            name[cp] = '\0';
        } else if (strcmp(key, "description") == 0) {
            if (vlen >= sizeof(desc) || vlen >= desc_size)
                return AGENT_SKILL_ERR_LINE_TOO_LONG;
            size_t cp = vlen < desc_size - 1 ? vlen : desc_size - 1;
            memcpy(desc, val, cp);
            desc[cp] = '\0';
        }
    }

    if (!name[0] || !desc[0]) return AGENT_SKILL_ERR_FRONTMATTER;

    /* Validate name against [a-zA-Z0-9_-]+ */
    if (!agent_skill_name_valid(name)) return AGENT_SKILL_ERR_NAME;

    /* Copy outputs */
    strncpy(name_out, name, name_size - 1);
    name_out[name_size - 1] = '\0';
    strncpy(desc_out, desc, desc_size - 1);
    desc_out[desc_size - 1] = '\0';

    return 0;
}

int agent_skill_register(agent_worker *w, const char *path) {
    if (!w || !w->skill_registry || !path || !path[0])
        return AGENT_SKILL_ERR_IO;

    /* Resolve to absolute path */
    char resolved[AGENT_SKILL_PATH_MAX];
    if (!realpath(path, resolved)) {
        fprintf(stderr, "ds4-agent: warning: cannot resolve skill %s: %s\n",
                path, strerror(errno));
        return AGENT_SKILL_ERR_IO;
    }

    /* Validate path is within allowed workspace directories */
    if (!agent_skill_path_allowed(w, resolved))
        return AGENT_SKILL_ERR_WORKSPACE;

    /* Parse the file */
    char name[AGENT_SKILL_NAME_MAX];
    char desc[AGENT_SKILL_DESC_MAX];
    int ret = agent_skill_parse_file(resolved, name, sizeof(name),
                                     desc, sizeof(desc));
    if (ret != 0) return ret;

    /* Check for duplicate name */
    agent_skill_registry *registry = w->skill_registry;
    pthread_mutex_lock(&registry->mu);
    for (agent_skill *s = registry->head; s; s = s->next) {
        if (strcmp(s->name, name) == 0) {
            pthread_mutex_unlock(&registry->mu);
            return AGENT_SKILL_ERR_DUPLICATE;
        }
    }

    /* Allocate and link new skill node */
    agent_skill *sk = xmalloc(sizeof(*sk));
    strncpy(sk->name, name, sizeof(sk->name) - 1);
    sk->name[sizeof(sk->name) - 1] = '\0';
    strncpy(sk->description, desc, sizeof(sk->description) - 1);
    sk->description[sizeof(sk->description) - 1] = '\0';
    strncpy(sk->path, resolved, sizeof(sk->path) - 1);
    sk->path[sizeof(sk->path) - 1] = '\0';
    sk->next = registry->head;
    registry->head = sk;
    registry->count++;
    registry->generation++;
    pthread_mutex_unlock(&registry->mu);
    return 0;
}

static const char *agent_skill_error_text(int rc) {
    switch (rc) {
    case AGENT_SKILL_ERR_FRONTMATTER: return "invalid or incomplete frontmatter";
    case AGENT_SKILL_ERR_UTF8: return "invalid UTF-8 metadata";
    case AGENT_SKILL_ERR_NAME: return "invalid skill name";
    case AGENT_SKILL_ERR_WORKSPACE: return "path outside allowed workspace dirs";
    case AGENT_SKILL_ERR_DUPLICATE: return "duplicate skill name";
    case AGENT_SKILL_ERR_LINE_TOO_LONG: return "frontmatter line too long";
    default: return "filesystem error";
    }
}

static bool agent_skill_markdown_path(const char *name) {
    size_t len = name ? strlen(name) : 0;
    return len >= 3 && strcmp(name + len - 3, ".md") == 0;
}

static bool agent_skill_join_path(char *fullpath, size_t fullpath_size,
                                  const char *dir, const char *name) {
    int path_len = snprintf(fullpath, fullpath_size, "%s/%s", dir, name);
    if (path_len < 0 || (size_t)path_len >= fullpath_size) {
        fprintf(stderr, "ds4-agent: warning: skill path too long: %s/%s\n",
                dir, name);
        return false;
    }
    return true;
}

int agent_skill_register_dir(agent_worker *w, const char *dir) {
    if (!w || !dir || !dir[0]) return -1;

    DIR *d = opendir(dir);
    if (!d) {
        fprintf(stderr, "ds4-agent: warning: cannot scan skill dir %s: %s\n",
                dir, strerror(errno));
        return AGENT_SKILL_ERR_IO;
    }

    int count = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue; /* skip hidden entries and . / .. */

        char fullpath[AGENT_SKILL_PATH_MAX];
        if (!agent_skill_join_path(fullpath, sizeof(fullpath), dir, ent->d_name))
            continue;

        struct stat st;
        if (lstat(fullpath, &st) != 0) {
            fprintf(stderr, "ds4-agent: warning: cannot inspect skill path %s: %s\n",
                    fullpath, strerror(errno));
            continue;
        }

        if (S_ISLNK(st.st_mode)) {
            fprintf(stderr, "ds4-agent: warning: skipping symlink while scanning skills: %s\n",
                    fullpath);
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            /* Recurse into subdirectories */
            int sub = agent_skill_register_dir(w, fullpath);
            if (sub > 0) count += sub;
            continue;
        }

        if (!S_ISREG(st.st_mode) || !agent_skill_markdown_path(ent->d_name))
            continue;

        int ret = agent_skill_register(w, fullpath);
        if (ret == 0) count++;
        else fprintf(stderr, "ds4-agent: warning: skipped skill %s: %s\n",
                     fullpath, agent_skill_error_text(ret));
    }
    closedir(d);
    return count;
}

int agent_skills_init(agent_worker *w) {
    if (!w) return -1;
    int count = 0;
    for (int i = 0; i < w->cfg->skill_dirs.len; i++) {
        const char *raw = w->cfg->skill_dirs.v[i];
        if (!raw || !raw[0]) continue;

        /* Resolve to absolute path (like --workspace does) */
        char resolved[PATH_MAX];
        if (!realpath(raw, resolved)) {
            fprintf(stderr, "ds4-agent: warning: cannot resolve skill dir %s: %s\n",
                    raw, strerror(errno));
            continue;
        }
        struct stat st;
        if (stat(resolved, &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "ds4-agent: warning: skill dir not a directory: %s\n",
                    resolved);
            continue;
        }

        /* Ensure directory is in working_directories */
        if (!agent_path_list_contains(&w->working_directories, resolved))
            agent_path_list_append(&w->working_directories, resolved);
        if (!agent_path_list_contains(&w->cfg->working_directories, resolved))
            agent_path_list_append(&w->cfg->working_directories, resolved);

        int ret = agent_skill_register_dir(w, resolved);
        if (ret < 0) {
            fprintf(stderr, "ds4-agent: warning: failed to scan skill dir %s\n", resolved);
        } else {
            count += ret;
        }
    }
    return count;
}

agent_skill *agent_skill_find(agent_worker *w, const char *name) {
    if (!w || !w->skill_registry || !name) return NULL;
    agent_skill_registry *registry = w->skill_registry;
    pthread_mutex_lock(&registry->mu);
    for (agent_skill *s = registry->head; s; s = s->next) {
        if (strcmp(s->name, name) == 0) {
            pthread_mutex_unlock(&registry->mu);
            return s;
        }
    }
    pthread_mutex_unlock(&registry->mu);
    return NULL;
}

void agent_skill_delete(agent_worker *w, const char *name) {
    if (!w || !w->skill_registry || !name) return;
    agent_skill_registry *registry = w->skill_registry;
    pthread_mutex_lock(&registry->mu);
    agent_skill **pp = &registry->head;
    while (*pp) {
        if (strcmp((*pp)->name, name) == 0) {
            agent_skill *next = (*pp)->next;
            free(*pp);
            *pp = next;
            registry->count--;
            registry->generation++;
            pthread_mutex_unlock(&registry->mu);
            return;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&registry->mu);
}

void agent_skill_list_all(agent_worker *w,
                          void (*cb)(const agent_skill *s, void *ctx),
                          void *ctx) {
    if (!w || !w->skill_registry || !cb) return;
    agent_skill_registry *registry = w->skill_registry;
    pthread_mutex_lock(&registry->mu);
    for (agent_skill *s = registry->head; s; s = s->next)
        cb(s, ctx);
    pthread_mutex_unlock(&registry->mu);
}

char *agent_build_skills_prompt(agent_worker *w) {
    if (!w || !w->skill_registry) return xstrdup("");

    agent_buf b = {0};
    agent_skill_registry *registry = w->skill_registry;
    pthread_mutex_lock(&registry->mu);
    if (!registry->head) {
        pthread_mutex_unlock(&registry->mu);
        return xstrdup("");
    }
    agent_buf_puts(&b, "\n## Skills\n\n");
    agent_buf_puts(&b, "Available skills (registered by the user or the system):\n\n");

    for (agent_skill *s = registry->head; s; s = s->next) {
        agent_buf_puts(&b, "- **");
        agent_buf_puts(&b, s->name);
        agent_buf_puts(&b, "**: ");
        agent_buf_puts(&b, s->description);
        agent_buf_puts(&b, "\n");
    }
    pthread_mutex_unlock(&registry->mu);

    agent_buf_puts(&b,
        "\n"
        "### Usage\n\n"
        "When the user includes **@<skill-name>** in their input, the system "
        "replaces it with a `[skill:<skill-name>](<path>)` link before the input "
        "is sent to you. Read that linked file before following the skill.\n\n"
        "Use `\\@<skill-name>` to pass `@skill-name` through literally without invocation.\n\n"
        "To discover available skills, use the `skill_list` DSML tool.\n"
    );

    return agent_buf_take(&b);
}

#define AGENT_TEMP_FILE_IDLE_SECONDS (5 * 60)

static int agent_temp_files_index(const agent_temp_file_list *list,
                                  const char *path) {
    if (!list || !path) return -1;
    for (int i = 0; i < list->len; i++) {
        if (list->v[i].path && !strcmp(list->v[i].path, path)) return i;
    }
    return -1;
}

static void agent_temp_files_append(agent_temp_file_list *list,
                                    const char *path,
                                    uint64_t last_read_at,
                                    uint64_t last_write_at) {
    if (!list || !path || !path[0]) return;
    if (list->len == list->cap) {
        list->cap = list->cap ? list->cap * 2 : 4;
        list->v = xrealloc(list->v, (size_t)list->cap * sizeof(list->v[0]));
    }
    list->v[list->len].path = xstrdup(path);
    list->v[list->len].last_read_at = last_read_at;
    list->v[list->len].last_write_at = last_write_at;
    list->len++;
}

static void agent_temp_files_remove_at(agent_temp_file_list *list, int idx) {
    if (!list || idx < 0 || idx >= list->len) return;
    free(list->v[idx].path);
    for (int j = idx + 1; j < list->len; j++)
        list->v[j - 1] = list->v[j];
    list->len--;
    if (list->len >= 0)
        memset(&list->v[list->len], 0, sizeof(list->v[list->len]));
}

static bool agent_temp_files_remove(agent_temp_file_list *list,
                                    const char *path,
                                    bool prefix) {
    if (!list || !path) return false;
    bool removed = false;
    size_t plen = prefix ? strlen(path) : 0;
    for (int i = 0; i < list->len; ) {
        bool match = false;
        if (list->v[i].path) {
            if (prefix) {
                match = strncmp(list->v[i].path, path, plen) == 0 &&
                    (list->v[i].path[plen] == '/' ||
                     list->v[i].path[plen] == '\0');
            } else {
                match = strcmp(list->v[i].path, path) == 0;
            }
        }
        if (match) {
            agent_temp_files_remove_at(list, i);
            removed = true;
            continue;
        }
        i++;
    }
    return removed;
}

static void agent_temp_files_free(agent_temp_file_list *list) {
    if (!list) return;
    for (int i = 0; i < list->len; i++) free(list->v[i].path);
    free(list->v);
    memset(list, 0, sizeof(*list));
}

static uint64_t agent_now_sec(void) {
    time_t now = time(NULL);
    return now > 0 ? (uint64_t)now : 0;
}

static void agent_temp_files_track(agent_worker *w, const char *path) {
    if (!w || !path || !path[0]) return;
    uint64_t now = agent_now_sec();
    int idx = agent_temp_files_index(&w->temp_files, path);
    if (idx >= 0) {
        w->temp_files.v[idx].last_write_at = now;
    } else {
        agent_temp_files_append(&w->temp_files, path, 0, now);
    }
    if (!agent_path_list_contains(&w->auto_allowed_paths, path))
        agent_path_list_append(&w->auto_allowed_paths, path);
}

static void agent_temp_files_note_read(agent_worker *w, const char *path) {
    if (!w || !path) return;
    int idx = agent_temp_files_index(&w->temp_files, path);
    if (idx >= 0) w->temp_files.v[idx].last_read_at = agent_now_sec();
}

static void agent_temp_files_note_write(agent_worker *w, const char *path) {
    if (!w || !path) return;
    int idx = agent_temp_files_index(&w->temp_files, path);
    if (idx >= 0) w->temp_files.v[idx].last_write_at = agent_now_sec();
}

static uint64_t agent_temp_file_last_access(const agent_temp_file *tf) {
    if (!tf) return 0;
    return tf->last_read_at > tf->last_write_at ?
        tf->last_read_at : tf->last_write_at;
}

static void agent_temp_files_cleanup(agent_worker *w, uint64_t now) {
    if (!w) return;
    if (w->cfg && w->cfg->preserve_agent_files) return;
    for (int i = 0; i < w->temp_files.len; ) {
        agent_temp_file *tf = &w->temp_files.v[i];
        uint64_t last = agent_temp_file_last_access(tf);
        if (last && now < last + AGENT_TEMP_FILE_IDLE_SECONDS) {
            i++;
            continue;
        }
        if (tf->path && tf->path[0]) {
            if (unlink(tf->path) != 0 && errno != ENOENT) {
                i++;
                continue;
            }
            agent_path_list_remove(&w->auto_allowed_paths, tf->path, false);
        }
        agent_temp_files_remove_at(&w->temp_files, i);
    }
}

static void agent_temp_files_cleanup_now(agent_worker *w) {
    agent_temp_files_cleanup(w, agent_now_sec());
}

static void write_all(int fd, const char *p, size_t n) {
    while (n) {
        ssize_t wr = write(fd, p, n);
        if (wr < 0) {
            if (errno == EINTR) continue;
            return;
        }
        p += wr;
        n -= (size_t)wr;
    }
}

typedef struct {
    char *ptr;
    size_t len;
    size_t cap;
} agent_input_buf;

static void agent_input_buf_append(agent_input_buf *b, const char *s, size_t n) {
    if (!n) return;
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 4096;
        while (cap < b->len + n + 1) cap *= 2;
        b->ptr = xrealloc(b->ptr, cap);
        b->cap = cap;
    }
    memcpy(b->ptr + b->len, s, n);
    b->len += n;
    b->ptr[b->len] = '\0';
}

static char *agent_input_buf_take(agent_input_buf *b) {
    if (!b->ptr) return xstrdup("");
    char *p = b->ptr;
    memset(b, 0, sizeof(*b));
    return p;
}

static void agent_input_buf_free(agent_input_buf *b) {
    free(b->ptr);
    memset(b, 0, sizeof(*b));
}

static int parse_int(const char *s, const char *opt) {
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (s[0] == '\0' || *end != '\0' || v <= 0 || v > INT32_MAX) {
        fprintf(stderr, "ds4-agent: invalid value for %s: %s\n", opt, s);
        exit(2);
    }
    return (int)v;
}

static bool parse_power_percent(const char *arg, int *out) {
    char *end = NULL;
    long v = strtol(arg, &end, 10);
    if (!arg[0] || *end != '\0' || v < 1 || v > 100) return false;
    *out = (int)v;
    return true;
}

bool agent_slash_command_with_args(const char *cmd, const char *name) {
    size_t len = strlen(name);
    return !strncmp(cmd, name, len) &&
           (cmd[len] == '\0' || isspace((unsigned char)cmd[len]));
}

static bool agent_slash_command_known(const char *cmd) {
    return !strcmp(cmd, "/help") ||
           !strcmp(cmd, "/save") ||
           !strcmp(cmd, "/compact") ||
           !strcmp(cmd, "/list") ||
           !strcmp(cmd, "/strict_sandbox") ||
           !strcmp(cmd, "/no_strict_sandbox") ||
           agent_slash_command_with_args(cmd, "/command_output") ||
           !strcmp(cmd, "/preserve_agent_files") ||
           !strcmp(cmd, "/no_preserve_agent_files") ||
           !strcmp(cmd, "/docker help") ||
           !strcmp(cmd, "/docker debug") ||
           agent_slash_command_with_args(cmd, "/docker create") ||
           agent_slash_command_with_args(cmd, "/docker describe") ||
           agent_slash_command_with_args(cmd, "/docker destroy") ||
           agent_slash_command_with_args(cmd, "/docker stop") ||
           agent_slash_command_with_args(cmd, "/docker use") ||
           !strcmp(cmd, "/docker list") ||
           !strcmp(cmd, "/quit") ||
           !strcmp(cmd, "/exit") ||
           !strcmp(cmd, "/new") ||
           agent_slash_command_with_args(cmd, "/power") ||
           agent_slash_command_with_args(cmd, "/thinking") ||
           agent_slash_command_with_args(cmd, "/switch") ||
           agent_slash_command_with_args(cmd, "/del") ||
           agent_slash_command_with_args(cmd, "/strip") ||
           agent_slash_command_with_args(cmd, "/history") ||
           agent_slash_command_with_args(cmd, "/workspace") ||
           agent_slash_command_with_args(cmd, "/subagent") ||
           agent_slash_command_with_args(cmd, "/allow") ||
           agent_slash_command_with_args(cmd, "/disallow") ||
           agent_slash_command_with_args(cmd, "/skills") ||
           !strcmp(cmd, "/purge_auto_files");
}

static uint64_t parse_u64(const char *s, const char *opt) {
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (s[0] == '\0' || *end != '\0' || v == 0) {
        fprintf(stderr, "ds4-agent: invalid value for %s: %s\n", opt, s);
        exit(2);
    }
    return (uint64_t)v;
}

static float parse_float_range(const char *s, const char *opt, float min, float max) {
    char *end = NULL;
    float v = strtof(s, &end);
    if (s[0] == '\0' || *end != '\0' || !isfinite(v) || v < min || v > max) {
        fprintf(stderr, "ds4-agent: invalid value for %s: %s\n", opt, s);
        exit(2);
    }
    return v;
}

static ds4_backend parse_backend(const char *s) {
    if (!strcmp(s, "metal")) return DS4_BACKEND_METAL;
    if (!strcmp(s, "cuda")) return DS4_BACKEND_CUDA;
    if (!strcmp(s, "cpu")) return DS4_BACKEND_CPU;
    fprintf(stderr, "ds4-agent: invalid backend: %s\n", s);
    exit(2);
}

static ds4_backend default_backend(void) {
#ifdef DS4_NO_GPU
    return DS4_BACKEND_CPU;
#elif defined(__APPLE__)
    return DS4_BACKEND_METAL;
#else
    return DS4_BACKEND_CUDA;
#endif
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1.0e-9;
}

static void usage(FILE *fp, const char *topic) {
    ds4_help_print(fp, DS4_HELP_AGENT, topic);
}

static const char *need_arg(int *i, int argc, char **argv, const char *opt) {
    if (*i + 1 >= argc) {
        fprintf(stderr, "ds4-agent: missing value for %s\n", opt);
        exit(2);
    }
    return argv[++(*i)];
}

static agent_config parse_options(int argc, char **argv) {
    agent_config c = {
        .engine = {
            .model_path = "ds4flash.gguf",
            .backend = default_backend(),
            .mtp_draft_tokens = 1,
            .mtp_margin = 3.0f,
        },
        .gen = {
            .system = "You are a helpful coding assistant running inside ds4-agent.",
            .n_predict = 50000,
            .ctx_size = 100000,
            .temperature = DS4_DEFAULT_TEMPERATURE,
            .top_p = DS4_DEFAULT_TOP_P,
            .min_p = DS4_DEFAULT_MIN_P,
            .think_mode = DS4_THINK_HIGH,
        },
        .strict_sandbox = true,
        .docker_auto = true,
        .command_output = false,
        .preserve_agent_files = false,
    };
    if (!getcwd(c.launch_working_directory, sizeof(c.launch_working_directory))) {
        fprintf(stderr, "ds4-agent: failed to get current working directory: %s\n",
                strerror(errno));
        exit(2);
    }
    snprintf(c.temp_directory, sizeof(c.temp_directory), "/tmp");

    bool steering_scale_set = false;
    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (!strcmp(arg, "-h") || !strcmp(arg, "--help")) {
            const char *topic = (i + 1 < argc && argv[i + 1][0] != '-') ?
                argv[i + 1] : NULL;
            usage(stdout, topic);
            exit(0);
        }
        char dist_parse_err[256] = {0};
        ds4_dist_cli_parse_result dist_parse =
            ds4_dist_parse_cli_arg(arg,
                                   &i,
                                   argc,
                                   argv,
                                   &c.engine.distributed,
                                   dist_parse_err,
                                   sizeof(dist_parse_err));
        if (dist_parse == DS4_DIST_CLI_ERROR) {
            fprintf(stderr,
                    "ds4-agent: %s\n",
                    dist_parse_err[0] ? dist_parse_err : "invalid distributed option");
            exit(2);
        }
        if (dist_parse == DS4_DIST_CLI_MATCHED) continue;

        if (!strcmp(arg, "-p") || !strcmp(arg, "--prompt")) {
            c.gen.prompt = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--non-interactive")) {
            c.non_interactive = true;
        } else if (!strcmp(arg, "--no-strict-sandbox")) {
            c.strict_sandbox = false;
        } else if (!strcmp(arg, "--no-docker-auto")) {
            c.docker_auto = false;
        } else if (!strcmp(arg, "--preserve-agent-files")) {
            c.preserve_agent_files = true;
        } else if (!strcmp(arg, "-sys") || !strcmp(arg, "--system")) {
            c.gen.system = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--trace")) {
            c.gen.trace_path = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "-m") || !strcmp(arg, "--model")) {
            c.engine.model_path = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--mtp")) {
            c.engine.mtp_path = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--mtp-draft")) {
            c.engine.mtp_draft_tokens = parse_int(need_arg(&i, argc, argv, arg), arg);
        } else if (!strcmp(arg, "--mtp-margin")) {
            c.engine.mtp_margin = parse_float_range(need_arg(&i, argc, argv, arg), arg, 0.0f, 1000.0f);
        } else if (!strcmp(arg, "-c") || !strcmp(arg, "--ctx")) {
            c.gen.ctx_size = parse_int(need_arg(&i, argc, argv, arg), arg);
        } else if (!strcmp(arg, "-n") || !strcmp(arg, "--tokens")) {
            c.gen.n_predict = parse_int(need_arg(&i, argc, argv, arg), arg);
        } else if (!strcmp(arg, "--temp")) {
            c.gen.temperature = parse_float_range(need_arg(&i, argc, argv, arg), arg, 0.0f, 100.0f);
        } else if (!strcmp(arg, "--top-p")) {
            c.gen.top_p = parse_float_range(need_arg(&i, argc, argv, arg), arg, 0.0f, 1.0f);
        } else if (!strcmp(arg, "--min-p")) {
            c.gen.min_p = parse_float_range(need_arg(&i, argc, argv, arg), arg, 0.0f, 1.0f);
        } else if (!strcmp(arg, "--seed")) {
            c.gen.seed = parse_u64(need_arg(&i, argc, argv, arg), arg);
        } else if (!strcmp(arg, "--think")) {
            c.gen.think_mode = DS4_THINK_HIGH;
        } else if (!strcmp(arg, "--think-max")) {
            c.gen.think_mode = DS4_THINK_MAX;
        } else if (!strcmp(arg, "--nothink")) {
            c.gen.think_mode = DS4_THINK_NONE;
        } else if (!strcmp(arg, "--backend")) {
            c.engine.backend = parse_backend(need_arg(&i, argc, argv, arg));
        } else if (!strcmp(arg, "--metal")) {
            c.engine.backend = DS4_BACKEND_METAL;
        } else if (!strcmp(arg, "--cuda")) {
            c.engine.backend = DS4_BACKEND_CUDA;
        } else if (!strcmp(arg, "--cpu")) {
            c.engine.backend = DS4_BACKEND_CPU;
        } else if (!strcmp(arg, "-t") || !strcmp(arg, "--threads")) {
            c.engine.n_threads = parse_int(need_arg(&i, argc, argv, arg), arg);
        } else if (!strcmp(arg, "--chdir")) {
            c.chdir_path = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--docker-command")) {
            c.docker_command = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--docker-container")) {
            c.docker_container = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--workspace")) {
            agent_path_list_append(&c.working_directory_args,
                                   need_arg(&i, argc, argv, arg));
        } else if (!strcmp(arg, "--temp-directory")) {
            snprintf(c.temp_directory, sizeof(c.temp_directory), "%s",
                     need_arg(&i, argc, argv, arg));
        } else if (!strcmp(arg, "--web-cdp-host")) {
            snprintf(c.web_cdp_host, sizeof(c.web_cdp_host), "%s",
                     need_arg(&i, argc, argv, arg));
        } else if (!strcmp(arg, "--skill-dir")) {
            agent_path_list_append(&c.skill_dirs,
                                   need_arg(&i, argc, argv, arg));
        } else if (!strcmp(arg, "--web-cdp-port")) {
            int v = parse_int(need_arg(&i, argc, argv, arg), arg);
            if (v <= 0 || v > 65535) {
                fprintf(stderr, "ds4-agent: --web-cdp-port must be 1..65535\n");
                exit(2);
            }
            c.web_cdp_port = v;
        } else if (!strcmp(arg, "--quality")) {
            c.engine.quality = true;
        } else if (!strcmp(arg, "--ssd-streaming")) {
            c.engine.ssd_streaming = true;
        } else if (!strcmp(arg, "--ssd-streaming-cold")) {
            c.engine.ssd_streaming_cold = true;
        } else if (!strcmp(arg, "--ssd-streaming-cache-experts")) {
            uint32_t experts = 0;
            uint64_t bytes = 0;
            if (!ds4_parse_streaming_cache_experts_arg(
                    need_arg(&i, argc, argv, arg), &experts, &bytes)) {
                fprintf(stderr,
                        "ds4-agent: --ssd-streaming-cache-experts must be a positive count or <number>GB\n");
                exit(2);
            }
            c.engine.ssd_streaming_cache_experts = experts;
            c.engine.ssd_streaming_cache_bytes = bytes;
        } else if (!strcmp(arg, "--ssd-streaming-preload-experts")) {
            int v = parse_int(need_arg(&i, argc, argv, arg), arg);
            if (v <= 0) {
                fprintf(stderr, "ds4-agent: --ssd-streaming-preload-experts must be positive\n");
                exit(2);
            }
            c.engine.ssd_streaming_preload_experts = (uint32_t)v;
        } else if (!strcmp(arg, "--simulate-used-memory")) {
            if (!ds4_parse_gib_arg(need_arg(&i, argc, argv, arg),
                                   &c.engine.simulate_used_memory_bytes)) {
                fprintf(stderr,
                        "ds4-agent: --simulate-used-memory must be a positive GiB value, e.g. 64GB\n");
                exit(2);
            }
        } else if (!strcmp(arg, "--prefill-chunk")) {
            int v = parse_int(need_arg(&i, argc, argv, arg), arg);
            if (v <= 0) {
                fprintf(stderr, "ds4-agent: --prefill-chunk must be positive\n");
                exit(2);
            }
            c.engine.prefill_chunk = (uint32_t)v;
        } else if (!strcmp(arg, "--power")) {
            c.engine.power_percent = parse_int(need_arg(&i, argc, argv, arg), arg);
            if (c.engine.power_percent < 1 || c.engine.power_percent > 100) {
                fprintf(stderr, "ds4-agent: --power must be between 1 and 100\n");
                exit(2);
            }
        } else if (!strcmp(arg, "--warm-weights")) {
            c.engine.warm_weights = true;
        } else if (!strcmp(arg, "--dir-steering-file")) {
            c.engine.directional_steering_file = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--dir-steering-ffn")) {
            c.engine.directional_steering_ffn = parse_float_range(need_arg(&i, argc, argv, arg), arg, -100.0f, 100.0f);
            steering_scale_set = true;
        } else if (!strcmp(arg, "--dir-steering-attn")) {
            c.engine.directional_steering_attn = parse_float_range(need_arg(&i, argc, argv, arg), arg, -100.0f, 100.0f);
            steering_scale_set = true;
        } else if (!strcmp(arg, "--recover")) {
            c.recover_session = need_arg(&i, argc, argv, arg);
        } else if (!strcmp(arg, "--tools")) {
            snprintf(c.default_tools, sizeof(c.default_tools), "%s",
                     need_arg(&i, argc, argv, arg));
        } else {
            fprintf(stderr, "ds4-agent: unknown option: %s\n", arg);
            usage(stderr, NULL);
            exit(2);
        }

    }

    if (c.engine.directional_steering_file && !steering_scale_set)
        c.engine.directional_steering_ffn = 1.0f;
    char dist_err[256];
    if (ds4_dist_prepare_engine_options(&c.engine.distributed,
                                        &c.engine,
                                        dist_err,
                                        sizeof(dist_err)) != 0) {
        fprintf(stderr, "ds4-agent: %s\n", dist_err);
        exit(2);
    }
    if (c.engine.distributed.role == DS4_DISTRIBUTED_WORKER) {
        fprintf(stderr, "ds4-agent: --role worker is a serving mode; start workers with ./ds4\n");
        exit(2);
    }
    /* If no explicit --tools flag, start with all tools enabled. */
    if (!c.default_tools[0]) {
        snprintf(c.default_tools, sizeof(c.default_tools), "all");
    }
    return c;
}

static bool agent_resolve_working_directory_arg(const char *path,
                                                char out[PATH_MAX],
                                                char *err, size_t err_len) {
    if (!path || !path[0]) {
        snprintf(err, err_len, "working directory path is empty");
        return false;
    }
    if (!realpath(path, out)) {
        snprintf(err, err_len, "failed to resolve working directory %s: %s",
                 path, strerror(errno));
        return false;
    }
    struct stat st;
    if (stat(out, &st) != 0) {
        snprintf(err, err_len, "failed to stat working directory %s: %s",
                 out, strerror(errno));
        return false;
    }
    if (!S_ISDIR(st.st_mode)) {
        snprintf(err, err_len, "working directory is not a directory: %s",
                 out);
        return false;
    }
    return true;
}

static bool agent_config_resolve_working_directory(agent_config *cfg,
                                                   char *err, size_t err_len) {
    if (!cfg) return true;
    if (cfg->working_directory_args.len == 0) {
        if (!cfg->launch_working_directory[0]) {
            snprintf(err, err_len,
                     "failed to determine launch working directory");
            return false;
        }
        if (!agent_path_list_contains(&cfg->working_directories,
                                      cfg->launch_working_directory))
            agent_path_list_append(&cfg->working_directories,
                                   cfg->launch_working_directory);
        return true;
    }
    for (int i = 0; i < cfg->working_directory_args.len; i++) {
        const char *arg = cfg->working_directory_args.v[i];
        char resolved[PATH_MAX];
        if (!agent_resolve_working_directory_arg(arg, resolved, err, err_len))
            return false;
        if (!agent_path_list_contains(&cfg->working_directories, resolved))
            agent_path_list_append(&cfg->working_directories, resolved);
    }
    return true;
}

static void log_context_memory(ds4_backend backend,
                               int         ctx_size,
                               uint32_t    prefill_chunk) {
    ds4_context_memory m =
        ds4_context_memory_estimate_with_prefill(backend,
                                                 ctx_size,
                                                 prefill_chunk);
    fprintf(stderr,
            "ds4-agent: context buffers %.2f MiB (ctx=%d, backend=%s, prefill_chunk=%u, raw_kv_rows=%u, compressed_kv_rows=%u)\n",
            (double)m.total_bytes / (1024.0 * 1024.0),
            ctx_size,
            ds4_backend_name(backend),
            m.prefill_cap,
            m.raw_cap,
            m.comp_cap);
}

static ds4_think_mode effective_think_mode(const agent_config *cfg) {
    return ds4_think_mode_for_context(cfg->gen.think_mode, cfg->gen.ctx_size);
}

/* Replaced by shared agent_think_mode_icon() in ds4_agent_internal.h */

static const char *agent_think_mode_confirmation(ds4_think_mode mode) {
    switch (mode) {
    case DS4_THINK_NONE: return "thinking disabled";
    case DS4_THINK_HIGH: return "thinking set to default";
    case DS4_THINK_MAX:  return "thinking set to max";
    default:             return "thinking set";
    }
}

static ds4_think_mode agent_next_think_mode(ds4_think_mode mode) {
    switch (mode) {
    case DS4_THINK_NONE: return DS4_THINK_HIGH;
    case DS4_THINK_HIGH: return DS4_THINK_MAX;
    case DS4_THINK_MAX:  return DS4_THINK_NONE;
    default:             return DS4_THINK_HIGH;
    }
}

/* ============================================================================
 * System Prompt Rendering And Worker Output Queues
 * ============================================================================
 */

static const char agent_tools_prompt_intro[] =
    "You are a coding agent running in a local workspace. Use tools for local file and system work. "
    "Avoid printing large file contents or large code blocks as answers; create or edit files with tools, "
    "then summarize results briefly.\n\n"
    "## Tools\n\n"
    "You have access to native DSML tools. Invoke tools by writing exactly this shape:\n\n"
    "<｜DSML｜tool_calls>\n"
    "<｜DSML｜invoke name=\"$TOOL_NAME\">\n"
    "<｜DSML｜parameter name=\"$PARAMETER_NAME\" string=\"true|false\">$PARAMETER_VALUE</｜DSML｜parameter>\n"
    "</｜DSML｜invoke>\n"
    "</｜DSML｜tool_calls>\n\n"
    "Inside <think></think>, only read, more, list, search, web_browse, web_fetch, and skill_list "
    "may be called. Read and web calls still require permission from the current tool-access policy; "
    "skill_list is always available. Finish thinking before calling any other tool.\n\n"
    "String parameters use raw text and string=\"true\". Numbers and booleans use JSON text and string=\"false\".\n\n"
    "Read defaults to a bounded chunk: path alone returns the first 500 lines, not the whole file. "
    "If read says more lines are available, call more with count=<lines> to read the next chunk; "
    "more defaults to the next 500 lines. "
    "The read result also reports continue_offset=N, which is the next start_line if you need to jump manually. "
    "If the user explicitly asks you to read a complete file into context, call read with whole=true. "
    "A whole-file read may fail if the result would not fit the current context; then explain that and use chunks.\n\n"
    "Local file tools are limited to the configured working directories. "
    "If none were passed on startup, the launch directory is the initial workspace. "
    "Relative paths resolve from the first configured workspace. "
    "Paths outside the current workspaces require user approval before the agent adds a new working directory. "
    "Bash commands start in the first workspace.\n\n";

static const char agent_tools_prompt_edit_line[] =
    "## Editing files\n\n"
    "Use write for new files or deliberate whole-file replacement. Use edit with path, old, and new for changes. "
    "For edit, always put the edited file path as the first parameter. "
    "The old text must match exactly once in the current file; otherwise edit fails for safety.\n"
    "When file content needs the literal text \\n (backslash followed by n), emit it in write.content, edit.old, or edit.new as \\\\n; actual line breaks should remain real newlines.\n"
    "For large replacements, prefer anchored old text: write the first lines, then [upto], then the final lines. "
    "The tool replaces everything from the head through the tail. If the head or tail is ambiguous, the edit fails.\n"
    "When old uses [upto], new may use one [upto] to keep the original omitted middle at that point.\n"
    "After [upto], always write unique final lines before closing old; never close old immediately after [upto].\n"
    "Do not use a generic tail anchor like:\n"
    "- BigNum bignum_add(BigNum *a, BigNum *b) {\n"
    "- [upto]\n"
    "- }\n"
    "because the closing brace may match many functions. Instead include final lines that are unique near that function, "
    "for example its last calculation and return line before the brace.\n"
    "Example anchored edit:\n"
    "<｜DSML｜tool_calls>\n"
    "<｜DSML｜invoke name=\"edit\">\n"
    "<｜DSML｜parameter name=\"path\" string=\"true\">/tmp/example.c</｜DSML｜parameter>\n"
    "<｜DSML｜parameter name=\"old\" string=\"true\">static int parse(void) {\n"
    "    int ok = 0;\n"
    "[upto]\n"
    "    return ok;\n"
    "}</｜DSML｜parameter>\n"
    "<｜DSML｜parameter name=\"new\" string=\"true\">static int parse(void) {\n"
    "    return parse_impl();\n"
    "}</｜DSML｜parameter>\n"
    "</｜DSML｜invoke>\n"
    "</｜DSML｜tool_calls>\n"
    "To insert text, use edit with old set to an exact unique anchor and new set to that anchor plus the added text.\n"
    "Use read raw=true only when you need plain file text without line numbers or read annotations.\n\n";

/* Individual tool JSON schemas for per-tool filtering.
 * Each entry includes guidance text so the model knows when to use it. */

static const char agent_tool_schema_ask_question[] =
    "Use ask_question when a decision requires user clarification, especially during exploratory work, planning, "
    "or before performing an action. If you provide choices, the UI will always also offer Interrupt and "
    "Something else; do not include those fallback choices yourself.\n\n"
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"ask_question\",\n"
    "    \"description\": \"When a decision is required you can ask a question to clarify, especially when planning changes or before performing an action.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"question\": {\"type\": \"string\"},\n"
    "        \"choices\": {\"type\": \"array\", \"items\": {\"type\": \"string\"}}\n"
    "      },\n"
    "      \"required\": [\"question\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_skill_list[] =
    "Use the `skill_list` tool to discover registered skills. "
    "When `has_more` is true, use different query substrings (e.g., narrower terms) "
    "to discover the remaining skills.\n\n"
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"skill_list\",\n"
    "    \"description\": \"List registered skills. Accepts optional `query` (literal substring, case-insensitive) to filter by name. Returns at most 10 skills per call, with total count and has_more flag.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"query\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": []\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_bash[] =
    "For long-running bash commands, pass refresh_sec. If a bash job is still running, use "
    "bash_status to check it early or bash_stop to terminate it.\n\n"
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"bash\",\n"
    "    \"description\": \"Run a shell command.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"command\": {\"type\": \"string\"},\n"
    "        \"timeout_sec\": {\"type\": \"number\"},\n"
    "        \"refresh_sec\": {\"type\": \"number\"}\n"
    "      },\n"
    "      \"required\": [\"command\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_bash_status[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"bash_status\",\n"
    "    \"description\": \"Report current status and new output for a bash job.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"job\": {\"type\": \"number\"},\n"
    "        \"pid\": {\"type\": \"number\"},\n"
    "        \"refresh_sec\": {\"type\": \"number\"}\n"
    "      },\n"
    "      \"required\": [\"job\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_bash_stop[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"bash_stop\",\n"
    "    \"description\": \"Terminate a running bash job and report its final output.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"job\": {\"type\": \"number\"},\n"
    "        \"pid\": {\"type\": \"number\"},\n"
    "        \"refresh_sec\": {\"type\": \"number\"}\n"
    "      },\n"
    "      \"required\": [\"job\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_read[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"read\",\n"
    "    \"description\": \"Read a text file or a range of lines.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"path\": {\"type\": \"string\"},\n"
    "        \"start_line\": {\"type\": \"number\"},\n"
    "        \"max_lines\": {\"type\": \"number\"},\n"
    "        \"whole\": {\"type\": \"boolean\"},\n"
    "        \"raw\": {\"type\": \"boolean\"}\n"
    "      },\n"
    "      \"required\": [\"path\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_more[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"more\",\n"
    "    \"description\": \"Continue the previous read-like output.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"count\": {\"type\": \"number\"}\n"
    "      }\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_write[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"write\",\n"
    "    \"description\": \"Create or overwrite a text file.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"path\": {\"type\": \"string\"},\n"
    "        \"content\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"path\", \"content\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_edit[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"edit\",\n"
    "    \"description\": \"Replace exactly one old text match; old may contain [upto] between unique head and tail anchors; when old is anchored, new may contain one [upto] to keep the omitted original middle.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"path\": {\"type\": \"string\"},\n"
    "        \"old\": {\"type\": \"string\"},\n"
    "        \"new\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"path\", \"old\", \"new\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_search[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"search\",\n"
    "    \"description\": \"Search files and return compact edit-friendly matches.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"query\": {\"type\": \"string\"},\n"
    "        \"path\": {\"type\": \"string\"},\n"
    "        \"mode\": {\"type\": \"string\"},\n"
    "        \"glob\": {\"type\": \"string\"},\n"
    "        \"context\": {\"type\": \"number\"},\n"
    "        \"max_results\": {\"type\": \"number\"},\n"
    "        \"case_sensitive\": {\"type\": \"boolean\"}\n"
    "      },\n"
    "      \"required\": [\"query\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_list[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"list\",\n"
    "    \"description\": \"List one directory compactly.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"path\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"path\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_web_browse[] =
    "Use web_browse to find web pages. Use web_fetch to read a known URL with a visible browser. "
    "The first web call may ask the user for permission to start Chrome.\n\n"
    "### Available Tool Schemas\n\n"
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"web_browse\",\n"
    "    \"description\": \"Search Google in a visible browser and return compact Markdown links.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"query\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"query\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_web_fetch[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"web_fetch\",\n"
    "    \"description\": \"Open a URL in a visible browser and return rendered page Markdown.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"url\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"url\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

static const char agent_tool_schema_mkdir[] =
    "{\n"
    "  \"type\": \"function\",\n"
    "  \"function\": {\n"
    "    \"name\": \"mkdir\",\n"
    "    \"description\": \"Recursively create directories for the given path. Respects workspace folder rules.\",\n"
    "    \"parameters\": {\n"
    "      \"type\": \"object\",\n"
    "      \"properties\": {\n"
    "        \"path\": {\"type\": \"string\"}\n"
    "      },\n"
    "      \"required\": [\"path\"]\n"
    "    }\n"
    "  }\n"
    "}\n\n";

/* Rules section — always included after tool schemas. */
static const char agent_tools_prompt_rules[] =
    "# Rules\n\n"
    "- Always use strict syntax for DSML tool stanzas.\n"
    "- Use ask_question when exploratory work, planning, or a decision point requires clarification from the user; "
    "when enough context exists, proceed without asking unnecessary questions.\n"
    "- This system runs on local inference of a few hundred tokens/s of prefill, "
    "and a few tens of tokens/s decoding speed. Use read/search to get the "
    "anchors you need, then use anchored edit to avoid having to "
    "retype large text.\n"
    "- Write code that is reliable and works well; always have a mental model of "
    "what is going on in complex parts of the code.\n"
    "- Work in a way that preserves the current system configuration integrity, "
    "unless explicitly asked otherwise by the user.\n";

/* Map from concrete tool index to its schema string.  Indices match the
 * agent_tool_registry ordering: read(0), more(1), write(2),
 * list(3), edit(4), search(5), web_browse(6), web_fetch(7), bash(8),
 * bash_status(9), bash_stop(10), mkdir(11). */
static const char *agent_tool_schemas[AGENT_TOOL_COUNT] = {
    [AGENT_TOOL_READ]        = agent_tool_schema_read,
    [AGENT_TOOL_MORE]        = agent_tool_schema_more,
    [AGENT_TOOL_WRITE]       = agent_tool_schema_write,
    [AGENT_TOOL_LIST]        = agent_tool_schema_list,
    [AGENT_TOOL_EDIT]        = agent_tool_schema_edit,
    [AGENT_TOOL_SEARCH]      = agent_tool_schema_search,
    [AGENT_TOOL_WEB_BROWSE]  = agent_tool_schema_web_browse,
    [AGENT_TOOL_WEB_FETCH]   = agent_tool_schema_web_fetch,
    [AGENT_TOOL_BASH]        = agent_tool_schema_bash,
    [AGENT_TOOL_BASH_STATUS] = agent_tool_schema_bash_status,
    [AGENT_TOOL_BASH_STOP]   = agent_tool_schema_bash_stop,
    [AGENT_TOOL_MKDIR]       = agent_tool_schema_mkdir,
    [AGENT_TOOL_SKILL_LIST]  = agent_tool_schema_skill_list,
};

static const char *agent_tool_names[AGENT_TOOL_COUNT] = {
    [AGENT_TOOL_READ]        = "read",
    [AGENT_TOOL_MORE]        = "more",
    [AGENT_TOOL_WRITE]       = "write",
    [AGENT_TOOL_LIST]        = "list",
    [AGENT_TOOL_EDIT]        = "edit",
    [AGENT_TOOL_SEARCH]      = "search",
    [AGENT_TOOL_WEB_BROWSE]  = "web_browse",
    [AGENT_TOOL_WEB_FETCH]   = "web_fetch",
    [AGENT_TOOL_BASH]        = "bash",
    [AGENT_TOOL_BASH_STATUS] = "bash_status",
    [AGENT_TOOL_BASH_STOP]   = "bash_stop",
    [AGENT_TOOL_MKDIR]       = "mkdir",
    [AGENT_TOOL_SKILL_LIST]  = "skill_list",
};

static bool agent_tool_policy_includes_index(const ds4_agent_tool_policy *pol,
                                             int idx) {
    if (idx < 0 || idx >= AGENT_TOOL_COUNT) return false;
    if (!pol || pol->allow_all) return true;
    if (pol->allow_none) return false;
    return pol->allowed[idx];
}

static bool agent_tool_is_policy_exempt(const char *tool_name) {
    return tool_name && (!strcmp(tool_name, "ask_question") ||
                         !strcmp(tool_name, "skill_list"));
}

static void agent_append_available_tools(agent_buf *b,
                                         const ds4_agent_tool_policy *pol) {
    agent_buf_puts(b, "Available tools: ");
    int first = 1;
    agent_buf_puts(b, "ask_question");
    first = 0;
    for (int i = 0; i < AGENT_TOOL_COUNT; i++) {
        const char *tool = agent_tool_names[i];
        if (!tool) continue;
        if (!agent_tool_policy_includes_index(pol, i) &&
            !agent_tool_is_policy_exempt(tool)) continue;
        if (!first) agent_buf_puts(b, ", ");
        first = 0;
        agent_buf_puts(b, tool);
    }
    agent_buf_puts(b, "\n");
}

/* Build a filtered tools prompt that only includes schemas and guidance for
 * concrete tools allowed by the given policy.  If pol is NULL, all tools are
 * included (fallback for legacy callers).  The available-tool list and full
 * schemas are both derived from the active policy. */
static char *agent_build_filtered_tools_prompt(const ds4_agent_tool_policy *pol) {
    agent_buf b = {0};
    /* Always include the intro. */
    agent_buf_puts(&b, agent_tools_prompt_intro);
    agent_buf_puts(&b, "\n");
    agent_append_available_tools(&b, pol);
    agent_buf_puts(&b, "\n");
    agent_buf_puts(&b, agent_tool_schema_ask_question);

    /* Include editing instructions if any WRITE-class tool is allowed
     * (includes READ-class tools since they're a subset of WRITE). */
    if (!pol || pol->allow_all) {
        agent_buf_puts(&b, agent_tools_prompt_edit_line);
    } else if (!pol->allow_none) {
        bool include_edit = false;
        int count = agent_tools_class_indices[AGENT_TOOLS_CLASS_WRITE][0];
        for (int j = 1; j <= count; j++) {
            int idx = agent_tools_class_indices[AGENT_TOOLS_CLASS_WRITE][j];
            if (pol->allowed[idx]) { include_edit = true; break; }
        }
        if (include_edit)
            agent_buf_puts(&b, agent_tools_prompt_edit_line);
    }

    /* Include individual tool schemas for each allowed concrete tool.
     * When pol is NULL, include all tools. */
    for (int i = 0; i < AGENT_TOOL_COUNT; i++) {
        bool include = !pol || pol->allow_all ||
            (pol->allow_none ? false : pol->allowed[i]) ||
            agent_tool_is_policy_exempt(agent_tool_names[i]);
        if (include && agent_tool_schemas[i])
            agent_buf_puts(&b, agent_tool_schemas[i]);
    }

    /* Always include the Rules section (meta-guidance, not tool-specific). */
    agent_buf_puts(&b, agent_tools_prompt_rules);

    /* Append a policy summary line so the model knows its current contract. */
    if (pol) {
        char summary[256];
        ds4_agent_tool_policy_format(pol, summary, sizeof(summary));
        agent_buf_puts(&b, "\n**Current tool-access policy:** ");
        agent_buf_puts(&b, summary);
        agent_buf_puts(&b, "\n");
    }

    return agent_buf_take(&b);
}

static const char agent_dsml_syntax_reminder[] =
    "DSML syntax reminder:\n"
    "<｜DSML｜tool_calls>\n"
    "<｜DSML｜invoke name=\"$TOOL_NAME\">\n"
    "<｜DSML｜parameter name=\"$PARAMETER_NAME\" string=\"true|false\">$PARAMETER_VALUE</｜DSML｜parameter>\n"
    "</｜DSML｜invoke>\n"
    "</｜DSML｜tool_calls>\n";

#define AGENT_SYSTEM_PROMPT_REMINDER_TOKENS 50000

static char *agent_build_system_prompt_reminder(agent_worker *w) {
    char *tools = agent_build_filtered_tools_prompt(&w->tool_policy);
    const char *start = "\n\n[System prompt reminder follows.]\n";
    const char *end = "[End system prompt reminder.]\n\n";
    /* Include skills section */
    char *skills = agent_build_skills_prompt(w);
    size_t len = strlen(start) + strlen(tools) + strlen(end) + strlen(skills) + 1;
    char *out = xmalloc(len);
    out[0] = '\0';
    strcat(out, start);
    strcat(out, tools);
    strcat(out, skills);
    strcat(out, end);
    free(tools);
    free(skills);
    return out;
}

static void agent_append_system_prompt(agent_worker *w, ds4_engine *engine,
                                       ds4_tokens *tokens, const char *extra) {
    char *tools_prompt = agent_build_filtered_tools_prompt(&w->tool_policy);
    // fprintf(stdout, "--- agent prompt ---\n%s\n--- agent prompt ---\n", tools_prompt);
    ds4_tokenize_rendered_chat(engine, tools_prompt, tokens);
    free(tools_prompt);

    /* Append skills section */
    char *skills = agent_build_skills_prompt(w);
    if (skills && skills[0]) {
        ds4_tokenize_rendered_chat(engine, skills, tokens);
    }
    free(skills);

    if (!extra || !extra[0]) return;
    size_t n = strlen(extra);
    char *plain = xmalloc(n + 3);
    memcpy(plain, "\n\n", 2);
    memcpy(plain + 2, extra, n + 1);
    ds4_chat_append_message(engine, tokens, "system", plain);
    free(plain);
}

static const char *agent_project_instruction_files[] = {
    "AGENT.md",
    "AGENTS.md",
    "CLAUDE.md",
    "HERMES.md",
};

static char *agent_read_project_instruction_file(const char *root,
                                                 char *selected,
                                                 size_t selected_len) {
    const char *dir = root && root[0] ? root : ".";
    for (size_t i = 0; i < sizeof(agent_project_instruction_files) /
                           sizeof(agent_project_instruction_files[0]); i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir,
                 agent_project_instruction_files[i]);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if (st.st_size < 0) continue;

        FILE *fp = fopen(path, "rb");
        if (!fp) continue;
        size_t len = (size_t)st.st_size;
        char *data = xmalloc(len + 1);
        size_t got = fread(data, 1, len, fp);
        bool ok = got == len && ferror(fp) == 0;
        fclose(fp);
        if (!ok) {
            free(data);
            continue;
        }
        data[len] = '\0';
        if (selected && selected_len)
            snprintf(selected, selected_len, "%s",
                     agent_project_instruction_files[i]);
        return data;
    }
    return NULL;
}

static void agent_append_project_instruction_prompt(ds4_engine *engine,
                                                    ds4_tokens *tokens,
                                                    const char *root) {
    char selected[64] = {0};
    char *text = agent_read_project_instruction_file(root, selected,
                                                     sizeof(selected));
    if (!text) return;
    if (text[0]) {
        size_t len = strlen(selected) + strlen(text) + 80;
        char *msg = xmalloc(len);
        snprintf(msg, len, "\n\nProject instructions from %s:\n%s",
                 selected, text);
        ds4_chat_append_message(engine, tokens, "system", msg);
        free(msg);
    }
    free(text);
}

static void agent_append_project_instruction_reminder(agent_worker *w,
                                                      const char *root) {
    char selected[64] = {0};
    char *text = agent_read_project_instruction_file(root, selected,
                                                     sizeof(selected));
    if (!text) return;
    if (text[0]) {
        ds4_tokenize_text(w->engine,
            "\nProject instructions reminder from ", &w->transcript);
        ds4_tokenize_text(w->engine, selected, &w->transcript);
        ds4_tokenize_text(w->engine, ":\n", &w->transcript);
        ds4_tokenize_text(w->engine, text, &w->transcript);
        ds4_tokenize_text(w->engine,
            "\n[End project instructions reminder.]\n\n", &w->transcript);
    }
    free(text);
}

static void agent_worker_note_system_prompt_seen(agent_worker *w) {
    w->last_system_prompt_reminder_at = w->transcript.len;
}

static void agent_worker_note_skills_prompt_seen(agent_worker *w) {
    if (!w || !w->skill_registry) return;
    pthread_mutex_lock(&w->skill_registry->mu);
    w->skills_prompt_generation = w->skill_registry->generation;
    pthread_mutex_unlock(&w->skill_registry->mu);
}

static void agent_worker_sync_skills_prompt(agent_worker *w) {
    if (!w || !w->skill_registry) return;
    pthread_mutex_lock(&w->skill_registry->mu);
    bool changed = w->skills_prompt_generation != w->skill_registry->generation;
    pthread_mutex_unlock(&w->skill_registry->mu);
    if (!changed) return;

    char *reminder = agent_build_system_prompt_reminder(w);
    agent_publish_system_status(w, "Skills changed; re-injecting system prompt...");
    ds4_tokenize_rendered_chat(w->engine, reminder, &w->transcript);
    free(reminder);
    agent_worker_note_system_prompt_seen(w);
    agent_worker_note_skills_prompt_seen(w);
}

static void agent_worker_maybe_append_datetime_context(agent_worker *w) {
    if (w->datetime_context_injected) return;

    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);

    char when[128];
    if (strftime(when, sizeof(when), "%Y-%m-%d %H:%M:%S %Z", &tm) == 0)
        snprintf(when, sizeof(when), "%lld", (long long)now);

    char msg[256];
    snprintf(msg, sizeof(msg),
             "Current local date and time at session start: %s. "
             "Use this only when date or time matters.", when);
    ds4_chat_append_message(w->engine, &w->transcript, "system", msg);
    agent_trace_text(w, "datetime-context", msg, strlen(msg));
    w->datetime_context_injected = true;
}

/* The full tool/system reminder is separate from DSML syntax errors: it is a
 * pressure-controlled refresh of the same trusted prompt shape used at startup.
 * The built-in prompt is tokenized as rendered chat so DSML markers stay native
 * control tokens; arbitrary -sys text remains ordinary text. */
static void agent_worker_maybe_append_system_prompt_reminder(agent_worker *w) {
    if (w->last_system_prompt_reminder_at <= 0) {
        agent_worker_note_system_prompt_seen(w);
        return;
    }
    if (w->transcript.len - w->last_system_prompt_reminder_at <
        AGENT_SYSTEM_PROMPT_REMINDER_TOKENS)
    {
        return;
    }

    char *reminder = agent_build_system_prompt_reminder(w);
    agent_publish_system_status(w, "Re-injecting system prompt reminder...");
    agent_trace(w, "system prompt reminder injected at transcript=%d",
                w->transcript.len);
    ds4_tokenize_rendered_chat(w->engine, reminder, &w->transcript);
    free(reminder);

    const agent_path_list *roots = agent_working_directories(w);
    if (roots) {
        char msg[PATH_MAX + 192];
        snprintf(msg, sizeof(msg),
                 "\nConfigured ds4-agent working directory reminder: %s. "
                 "Local file tools check all configured working directories; "
                 "bash starts from the first one.",
                 roots->v[0]);
        ds4_tokenize_text(w->engine, msg, &w->transcript);
    }

    const char *extra = w->cfg->gen.system;
    if (extra && extra[0]) {
        ds4_tokenize_text(w->engine,
            "\nAdditional system instructions reminder:\n", &w->transcript);
        ds4_tokenize_text(w->engine, extra, &w->transcript);
        ds4_tokenize_text(w->engine,
            "\n[End additional system instructions reminder.]\n\n",
            &w->transcript);
    }
    const agent_path_list *reminder_roots = agent_working_directories(w);
    agent_append_project_instruction_reminder(w,
        reminder_roots && reminder_roots->len ? reminder_roots->v[0] : NULL);
    agent_worker_note_system_prompt_seen(w);
}

/* Wake the UI thread after changing worker-visible state.  The byte in
 * wake_fd is level-triggered with wake_pending so bursts of sampled tokens do
 * not flood the pipe. */
static void agent_wake_locked(agent_worker *w) {
    if (w->wake_pending) return;
    w->wake_pending = true;
    char c = 'x';
    ssize_t wr = write(w->wake_fd[1], &c, 1);
    (void)wr;
}

/* Queue rendered output for the UI thread.  The worker never writes directly
 * to the terminal, which keeps linenoise redraws serialized in one place. */
static void agent_publish(agent_worker *w, const char *s, size_t n) {
    if (!n) return;
    pthread_mutex_lock(&w->mu);
    if (w->out_len + n + 1 > w->out_cap) {
        size_t cap = w->out_cap ? w->out_cap * 2 : 4096;
        while (cap < w->out_len + n + 1) cap *= 2;
        char *p = realloc(w->out, cap);
        if (!p) {
            pthread_mutex_unlock(&w->mu);
            return;
        }
        w->out = p;
        w->out_cap = cap;
    }
    memcpy(w->out + w->out_len, s, n);
    w->out_len += n;
    w->out[w->out_len] = '\0';
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void agent_publishf(agent_worker *w, const char *fmt, ...) {
    char stack[1024];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(stack, sizeof(stack), fmt, ap);
    va_end(ap);
    if (n <= 0) return;
    if ((size_t)n < sizeof(stack)) {
        agent_publish(w, stack, (size_t)n);
        return;
    }

    char *heap = xmalloc((size_t)n + 1);
    va_start(ap, fmt);
    vsnprintf(heap, (size_t)n + 1, fmt, ap);
    va_end(ap);
    agent_publish(w, heap, (size_t)n);
    free(heap);
}

static regex_t agent_output_visible_regex;
static pthread_once_t agent_output_visible_regex_once = PTHREAD_ONCE_INIT;
static int agent_output_visible_regex_rc = -1;

static void agent_output_visible_regex_init(void) {
    agent_output_visible_regex_rc =
        regcomp(&agent_output_visible_regex, "[^[:space:]]", REG_EXTENDED);
}

static bool agent_output_has_visible_byte(const char *s, size_t n) {
    if (!s || !n) return false;
    pthread_once(&agent_output_visible_regex_once,
                 agent_output_visible_regex_init);
    if (agent_output_visible_regex_rc != 0) return true;
#ifdef REG_STARTEND
    regmatch_t m = {.rm_so = 0, .rm_eo = (regoff_t)n};
    return regexec(&agent_output_visible_regex, s, 1, &m,
                   REG_STARTEND) == 0;
#else
    char *tmp = xstrndup(s, n);
    bool visible = regexec(&agent_output_visible_regex, tmp, 0, NULL, 0) == 0;
    free(tmp);
    return visible;
#endif
}

static void agent_publish_command_output_chunk(agent_worker *w,
                                               const char *s, size_t n,
                                               bool *seen_visible) {
    if (!w || !w->cfg || !w->cfg->command_output || !s || !n) return;
    bool visible = agent_output_has_visible_byte(s, n);
    if (!visible && seen_visible && !*seen_visible) return;
    if (visible && seen_visible) *seen_visible = true;
    agent_publish(w, "\x1b[90m", 5);
    agent_publish(w, s, n);
    agent_publish(w, "\x1b[0m", 4);
}


static void agent_set_status(agent_worker *w, agent_worker_state state) {
    pthread_mutex_lock(&w->mu);
    w->status.state = state;
    if (state != AGENT_WORKER_PREFILL)
        w->status.prefill_tps = 0.0;
    if (state != AGENT_WORKER_GENERATING)
        w->status.greedy_sampling = false;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void agent_set_error(agent_worker *w, const char *msg) {
    pthread_mutex_lock(&w->mu);
    w->status.state = AGENT_WORKER_ERROR;
    w->status.prefill_tps = 0.0;
    w->status.greedy_sampling = false;
    snprintf(w->status.error, sizeof(w->status.error), "%s", msg ? msg : "unknown error");
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

/* ============================================================================
 * Trace Logging
 * ============================================================================
 */

static void agent_trace_time(FILE *fp) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);
    fprintf(fp, "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000);
}

static void agent_trace(agent_worker *w, const char *fmt, ...) {
    if (!w || !w->trace) return;
    pthread_mutex_lock(&w->mu);
    agent_trace_time(w->trace);
    fputs(" ", w->trace);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(w->trace, fmt, ap);
    va_end(ap);
    fputc('\n', w->trace);
    fflush(w->trace);
    pthread_mutex_unlock(&w->mu);
}

static void agent_trace_escaped(FILE *fp, const char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
        case '\\': fputs("\\\\", fp); break;
        case '\n': fputs("\\n", fp); break;
        case '\r': fputs("\\r", fp); break;
        case '\t': fputs("\\t", fp); break;
        case '"': fputs("\\\"", fp); break;
        default:
            if (c < 32 || c == 127) fprintf(fp, "\\x%02x", c);
            else fputc(c, fp);
            break;
        }
    }
}

static void agent_trace_token(agent_worker *w, int token, const char *text,
                              size_t text_len, int index) {
    if (!w || !w->trace) return;
    pthread_mutex_lock(&w->mu);
    agent_trace_time(w->trace);
    fprintf(w->trace, " token index=%d id=%d bytes=%zu text=\"",
            index, token, text_len);
    agent_trace_escaped(w->trace, text ? text : "", text_len);
    fputs("\" hex=", w->trace);
    for (size_t i = 0; i < text_len; i++)
        fprintf(w->trace, "%02x", (unsigned char)text[i]);
    fputc('\n', w->trace);
    fflush(w->trace);
    pthread_mutex_unlock(&w->mu);
}

static void agent_trace_tokens(agent_worker *w, const char *label,
                               const ds4_tokens *tokens, int start) {
    if (!w || !w->trace || !tokens) return;
    if (start < 0) start = 0;
    if (start > tokens->len) start = tokens->len;
    agent_trace(w, "tokens label=%s start=%d len=%d", label ? label : "",
                start, tokens->len);
    for (int i = start; i < tokens->len; i++) {
        size_t text_len = 0;
        char *text = ds4_token_text(w->engine, tokens->v[i], &text_len);
        agent_trace_token(w, tokens->v[i], text, text_len, i);
        free(text);
    }
}

static void agent_trace_text(agent_worker *w, const char *label,
                             const char *text, size_t len) {
    if (!w || !w->trace) return;
    pthread_mutex_lock(&w->mu);
    agent_trace_time(w->trace);
    fprintf(w->trace, " %s=\"", label ? label : "text");
    agent_trace_escaped(w->trace, text ? text : "", len);
    fputs("\"\n", w->trace);
    fflush(w->trace);
    pthread_mutex_unlock(&w->mu);
}

/* ============================================================================
 * DSML Tool-Call Parser
 * ============================================================================
 *
 * The model streams raw text tokens.  This parser recognizes completed DSML
 * tool stanzas and keeps a copy of the raw stanza for diagnostics.  It is
 * deliberately strict after the opening marker: typo recovery belongs to the
 * streaming detector so the actual tool parser stays small and predictable.
 */

static bool bytes_has_prefix(const char *p, size_t n, const char *prefix) {
    size_t plen = strlen(prefix);
    return n >= plen && memcmp(p, prefix, plen) == 0;
}

static bool bytes_is_partial_prefix(const char *p, size_t n, const char *prefix) {
    size_t plen = strlen(prefix);
    return n < plen && memcmp(prefix, p, n) == 0;
}

static void agent_tool_call_free(agent_tool_call *c) {
    if (!c) return;
    free(c->name);
    for (int i = 0; i < c->argc; i++) {
        free(c->args[i].name);
        free(c->args[i].value);
    }
    free(c->args);
    memset(c, 0, sizeof(*c));
}

static void agent_tool_calls_free(agent_tool_calls *calls) {
    if (!calls) return;
    for (int i = 0; i < calls->len; i++) agent_tool_call_free(&calls->v[i]);
    free(calls->v);
    memset(calls, 0, sizeof(*calls));
}

static void agent_tool_call_add_arg(agent_tool_call *c, const char *name,
                                    const char *value, size_t value_len,
                                    bool is_string) {
    if (c->argc == c->argcap) {
        c->argcap = c->argcap ? c->argcap * 2 : 4;
        c->args = xrealloc(c->args, (size_t)c->argcap * sizeof(c->args[0]));
    }
    c->args[c->argc++] = (agent_tool_arg){
        .name = xstrdup(name),
        .value = xstrndup(value, value_len),
        .is_string = is_string,
    };
}

static void agent_tool_calls_push(agent_tool_calls *calls, agent_tool_call *call) {
    if (!call->name) return;
    if (calls->len == calls->cap) {
        calls->cap = calls->cap ? calls->cap * 2 : 2;
        calls->v = xrealloc(calls->v, (size_t)calls->cap * sizeof(calls->v[0]));
    }
    calls->v[calls->len++] = *call;
    memset(call, 0, sizeof(*call));
}

static const char *agent_tool_arg_value(const agent_tool_call *call, const char *name) {
    for (int i = 0; i < call->argc; i++) {
        if (call->args[i].name && !strcmp(call->args[i].name, name))
            return call->args[i].value ? call->args[i].value : "";
    }
    return NULL;
}

static bool agent_file_tool_param_uses_literal_escape_markup(const char *tool,
                                                            const char *param) {
    if (!tool || !param) return false;
    if (!strcmp(tool, "write") && !strcmp(param, "content")) return true;
    if (!strcmp(tool, "edit") &&
        (!strcmp(param, "old") || !strcmp(param, "new")))
        return true;
    return false;
}

static char *agent_file_tool_decode_literal_escape_markup(const char *value,
                                                          size_t value_len,
                                                          size_t *out_len) {
    char *out = xmalloc(value_len + 1);
    size_t out_pos = 0;
    size_t i = 0;
    while (i < value_len) {
        if (value[i] != '\\') {
            out[out_pos++] = value[i];
            i++;
            continue;
        }

        size_t slash_start = i;
        while (i < value_len && value[i] == '\\') i++;
        size_t slash_count = i - slash_start;
        if (i < value_len && value[i] == 'n') {
            size_t keep = slash_count == 1 ? 1 : (slash_count + 1) / 2;
            for (size_t j = 0; j < keep; j++)
                out[out_pos++] = '\\';
            out[out_pos++] = 'n';
            i++;
            continue;
        }

        memcpy(out + out_pos, value + slash_start, slash_count);
        out_pos += slash_count;
    }
    out[out_pos] = '\0';
    if (out_len) *out_len = out_pos;
    return out;
}

static void agent_dsml_parser_free(agent_dsml_parser *p) {
    if (!p) return;
    free(p->raw);
    agent_tool_call_free(&p->current);
    free(p->param_name);
    agent_tool_calls_free(&p->calls);
    memset(p, 0, sizeof(*p));
}

static void agent_dsml_parser_reset(agent_dsml_parser *p) {
    agent_dsml_parser_free(p);
    p->state = AGENT_DSML_SEARCH;
}

static void agent_dsml_raw_append(agent_dsml_parser *p, const char *s, size_t n) {
    if (!n) return;
    if (p->raw_len + n + 1 > p->raw_cap) {
        size_t cap = p->raw_cap ? p->raw_cap * 2 : 512;
        while (cap < p->raw_len + n + 1) cap *= 2;
        p->raw = xrealloc(p->raw, cap);
        p->raw_cap = cap;
    }
    memcpy(p->raw + p->raw_len, s, n);
    p->raw_len += n;
    p->raw[p->raw_len] = '\0';
}

static char *agent_parse_attr(const char *tag, const char *name) {
    char pat[64];
    snprintf(pat, sizeof(pat), "%s=\"", name);
    const char *p = strstr(tag, pat);
    if (!p) return NULL;
    p += strlen(pat);
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    return xstrndup(p, (size_t)(end - p));
}

static void agent_dsml_set_error(agent_dsml_parser *p, const char *msg) {
    p->state = AGENT_DSML_ERROR;
    snprintf(p->error, sizeof(p->error), "%s", msg);
}

static bool agent_dsml_open_tag_is(const char *tag, const char *name) {
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "<｜DSML｜%s", name);
    size_t prefix_len = strlen(prefix);
    if (strncmp(tag, prefix, prefix_len) != 0) return false;
    char c = tag[prefix_len];
    return c == '>' || c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static bool agent_dsml_close_tag_at(const char *s, const char *name, size_t *tag_len) {
    char prefix[64];
    static const char dsml_bar[] = "｜";
    snprintf(prefix, sizeof(prefix), "</｜DSML｜%s", name);
    size_t prefix_len = strlen(prefix);
    if (strncmp(s, prefix, prefix_len) != 0) return false;
    const char *p = s + prefix_len;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (strncmp(p, dsml_bar, strlen(dsml_bar)) == 0) p += strlen(dsml_bar);
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != '>') return false;
    if (tag_len) *tag_len = (size_t)(p - s) + 1;
    return true;
}

/* Recognize a streamed parameter close tag prefix.  Full close detection is
 * handled by agent_dsml_close_tag_at(); this helper exists for online behavior:
 * terminal rendering must hide partial close tags without waiting for the whole
 * parameter to finish. */
static bool agent_dsml_parameter_close_tail(const char *tail, size_t len,
                                            bool *complete) {
    static const char prefix[] = "</｜DSML｜parameter";
    static const char dsml_bar[] = "｜";
    const size_t prefix_len = sizeof(prefix) - 1;
    const size_t bar_len = sizeof(dsml_bar) - 1;
    *complete = false;
    if (len <= prefix_len) return memcmp(prefix, tail, len) == 0;
    if (memcmp(prefix, tail, prefix_len) != 0) return false;
    size_t i = prefix_len;
    while (i < len && (tail[i] == ' ' || tail[i] == '\t' ||
                       tail[i] == '\r' || tail[i] == '\n')) i++;
    if (i < len && len - i <= bar_len) {
        if (memcmp(dsml_bar, tail + i, len - i) == 0) return true;
    }
    if (i + bar_len <= len && memcmp(tail + i, dsml_bar, bar_len) == 0)
        i += bar_len;
    for (; i < len; i++) {
        if (tail[i] == '>') {
            *complete = i == len - 1;
            return *complete;
        }
        if (tail[i] != ' ' && tail[i] != '\t' && tail[i] != '\r' && tail[i] != '\n')
            return false;
    }
    return true;
}

static void agent_dsml_update_param_close_prefix(agent_dsml_parser *p) {
    p->param_close_prefix = false;
    if (p->state != AGENT_DSML_PARAM_VALUE || p->raw_len <= p->param_value_start)
        return;

    const char *value = p->raw + p->param_value_start;
    const char *end = p->raw + p->raw_len;
    const char *lt = end;
    while (lt > value) {
        lt--;
        if (*lt == '<') break;
    }
    if (lt < value || *lt != '<') return;

    size_t tail_len = (size_t)(end - lt);
    if (tail_len > 64) return;
    bool complete = false;
    static const char dsml_marker[] = "</｜DSML｜";
    p->param_close_prefix =
        tail_len >= sizeof(dsml_marker) - 1 &&
        memcmp(lt, dsml_marker, sizeof(dsml_marker) - 1) == 0 &&
        agent_dsml_parameter_close_tail(lt, tail_len, &complete) &&
        !complete;
}

/* Find a DSML closing tag while accepting the few harmless closing-tag variants
 * the model has been observed to emit.  Opening tags stay strict so accidental
 * prose does not become a tool call. */
static char *agent_dsml_find_close_tag(const char *s, const char *name, size_t *tag_len) {
    const char *p = s;
    while ((p = strstr(p, "</｜DSML｜")) != NULL) {
        if (agent_dsml_close_tag_at(p, name, tag_len)) return (char *)p;
        p++;
    }
    return NULL;
}

/* Parse as much of the accumulated DSML buffer as possible.  The parser can be
 * called after every streamed byte: incomplete input leaves state unchanged
 * until enough bytes arrive, while malformed completed input switches to
 * AGENT_DSML_ERROR so the model gets a retryable tool error. */
static void agent_dsml_parse(agent_dsml_parser *p) {
    while (p->state == AGENT_DSML_STRUCTURAL || p->state == AGENT_DSML_PARAM_VALUE) {
        if (p->state == AGENT_DSML_PARAM_VALUE) {
            size_t end_tag_len = 0;
            char *end = agent_dsml_find_close_tag(p->raw + p->param_value_start,
                                                  "parameter", &end_tag_len);
            if (!end) return;
            const char *value = p->raw + p->param_value_start;
            size_t value_len = (size_t)(end - value);
            char *decoded = NULL;
            if (agent_file_tool_param_uses_literal_escape_markup(
                    p->current.name, p->param_name))
            {
                decoded = agent_file_tool_decode_literal_escape_markup(
                    value, value_len, &value_len);
                value = decoded;
            }
            agent_tool_call_add_arg(&p->current, p->param_name ? p->param_name : "",
                                    value, value_len, p->param_is_string);
            free(decoded);
            p->param_close_prefix = false;
            free(p->param_name);
            p->param_name = NULL;
            p->parse_pos = (size_t)(end - p->raw) + end_tag_len;
            p->state = AGENT_DSML_STRUCTURAL;
            continue;
        }

        while (p->parse_pos < p->raw_len &&
               (p->raw[p->parse_pos] == ' ' || p->raw[p->parse_pos] == '\t' ||
                p->raw[p->parse_pos] == '\r' || p->raw[p->parse_pos] == '\n'))
            p->parse_pos++;
        if (p->parse_pos >= p->raw_len) return;

        size_t close_len = 0;
        if (agent_dsml_close_tag_at(p->raw + p->parse_pos, "tool_calls", &close_len)) {
            agent_tool_calls_push(&p->calls, &p->current);
            p->parse_pos += close_len;
            p->state = AGENT_DSML_DONE;
            return;
        }
        if (agent_dsml_close_tag_at(p->raw + p->parse_pos, "invoke", &close_len)) {
            agent_tool_calls_push(&p->calls, &p->current);
            p->parse_pos += close_len;
            continue;
        }

        char *tag_end = strchr(p->raw + p->parse_pos, '>');
        if (!tag_end) return;
        size_t tag_len = (size_t)(tag_end - (p->raw + p->parse_pos)) + 1;
        char *tag = xstrndup(p->raw + p->parse_pos, tag_len);

        if (agent_dsml_open_tag_is(tag, "invoke")) {
            agent_tool_call_free(&p->current);
            p->current.name = agent_parse_attr(tag, "name");
            if (!p->current.name) {
                free(tag);
                agent_dsml_set_error(p, "tool invoke without name");
                return;
            }
            p->parse_pos += tag_len;
        } else if (agent_dsml_open_tag_is(tag, "parameter")) {
            free(p->param_name);
            p->param_name = agent_parse_attr(tag, "name");
            char *is_string = agent_parse_attr(tag, "string");
            p->param_is_string = is_string && !strcmp(is_string, "true");
            free(is_string);
            if (!p->param_name) {
                free(tag);
                agent_dsml_set_error(p, "tool parameter without name");
                return;
            }
            p->parse_pos += tag_len;
            p->param_value_start = p->parse_pos;
            p->param_close_prefix = false;
            p->state = AGENT_DSML_PARAM_VALUE;
        } else {
            snprintf(p->error, sizeof(p->error), "unexpected DSML tag: %.*s",
                     (int)(tag_len > 80 ? 80 : tag_len), tag);
            free(tag);
            p->state = AGENT_DSML_ERROR;
            return;
        }
        free(tag);
    }
}

static void agent_dsml_start(agent_dsml_parser *p) {
    static const char start[] = "<｜DSML｜tool_calls>";
    p->state = AGENT_DSML_STRUCTURAL;
    p->search_len = 0;
    agent_dsml_raw_append(p, start, strlen(start));
    p->parse_pos = strlen(start);
}

static void agent_dsml_feed(agent_dsml_parser *p, const char *s, size_t n) {
    static const char start[] = "<｜DSML｜tool_calls>";
    const size_t start_len = sizeof(start) - 1;
    if (p->state == AGENT_DSML_DONE || p->state == AGENT_DSML_ERROR) return;

    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (p->state == AGENT_DSML_SEARCH) {
            if (p->search_len == sizeof(p->search_tail)) {
                memmove(p->search_tail, p->search_tail + 1, --p->search_len);
            }
            p->search_tail[p->search_len++] = c;
            if (p->search_len >= start_len &&
                memcmp(p->search_tail + p->search_len - start_len, start, start_len) == 0)
                agent_dsml_start(p);
            continue;
        }

        agent_dsml_raw_append(p, &c, 1);
        agent_dsml_parse(p);
        if (p->state == AGENT_DSML_PARAM_VALUE)
            agent_dsml_update_param_close_prefix(p);
        else
            p->param_close_prefix = false;
    }
}

static bool agent_tool_phase_eligible_during_thinking(const char *name) {
    return name && (!strcmp(name, "read") || !strcmp(name, "more") ||
        !strcmp(name, "list") || !strcmp(name, "search") ||
        !strcmp(name, "web_browse") || !strcmp(name, "web_fetch") ||
        !strcmp(name, "skill_list"));
}

/* This is structural/phase validation only. Handler argument validation and
 * worker policy authorization deliberately remain ordered dispatch concerns. */
static bool agent_thinking_tool_block_validate(const agent_dsml_parser *p,
                                                char *err, size_t err_len) {
    if (!p || p->state == AGENT_DSML_ERROR) {
        snprintf(err, err_len, "%s", p && p->error[0] ? p->error :
                 "malformed DSML tool call");
        return false;
    }
    if (p->state != AGENT_DSML_DONE) {
        snprintf(err, err_len, "incomplete DSML tool call");
        return false;
    }
    if (p->calls.len == 0) {
        snprintf(err, err_len, "empty in-thinking tool call block");
        return false;
    }
    for (int i = 0; i < p->calls.len; i++) {
        const char *name = p->calls.v[i].name;
        if (!agent_tool_phase_eligible_during_thinking(name)) {
            snprintf(err, err_len,
                     "tool %s is not allowed inside <think></think>",
                     name && name[0] ? name : "<missing>");
            return false;
        }
    }
    if (err_len) err[0] = '\0';
    return true;
}

/* ============================================================================
 * Assistant Markdown Rendering
 * ============================================================================
 *
 * This renderer handles only the cheap markdown cues that make terminal output
 * readable: **bold**, *italic*, inline code, and fenced code blocks.  It is a
 * streaming parser, so it buffers only ambiguous marker bytes long enough to
 * decide whether they are formatting or literal text.
 */

static void agent_tail_capture_append(agent_tail_capture *t,
                                      const char *s, size_t n) {
    if (!t || !n) return;
    if (!t->cap) return;
    if (!t->buf) t->buf = xmalloc(t->cap);
    t->total += n;

    if (n >= t->cap) {
        memcpy(t->buf, s + n - t->cap, t->cap);
        t->start = 0;
        t->len = t->cap;
        return;
    }

    if (t->len < t->cap) {
        size_t free_tail = t->cap - t->len;
        size_t first = n < free_tail ? n : free_tail;
        size_t pos = (t->start + t->len) % t->cap;
        size_t right = t->cap - pos;
        size_t chunk = first < right ? first : right;
        memcpy(t->buf + pos, s, chunk);
        if (first > chunk) memcpy(t->buf, s + chunk, first - chunk);
        t->len += first;
        s += first;
        n -= first;
    }

    while (n) {
        size_t pos = (t->start + t->len) % t->cap;
        size_t right = t->cap - pos;
        size_t chunk = n < right ? n : right;
        memcpy(t->buf + pos, s, chunk);
        t->start = (t->start + chunk) % t->cap;
        s += chunk;
        n -= chunk;
    }
}

static char *agent_tail_capture_take(agent_tail_capture *t, size_t *len) {
    size_t n = t ? t->len : 0;
    char *out = xmalloc(n + 1);
    if (n) {
        size_t right = t->cap - t->start;
        size_t first = n < right ? n : right;
        memcpy(out, t->buf + t->start, first);
        if (n > first) memcpy(out + first, t->buf, n - first);
    }
    out[n] = '\0';
    if (len) *len = n;
    free(t->buf);
    memset(t, 0, sizeof(*t));
    return out;
}

static void renderer_write(agent_token_renderer *r, const char *s, size_t n) {
    if (r->capture) agent_tail_capture_append(r->capture, s, n);
    else agent_publish(r->worker, s, n);
}

static void renderer_set_grey(agent_token_renderer *r) {
    if (r->use_color) renderer_write(r, "\x1b[38;5;245m", 11);
}

static void renderer_reset_color(agent_token_renderer *r) {
    if (r->use_color) renderer_write(r, "\x1b[0m", 4);
    r->color_open = false;
}

static size_t renderer_utf8_need(unsigned char c) {
    if (c < 0x80) return 1;
    if (c >= 0xc2 && c <= 0xdf) return 2;
    if (c >= 0xe0 && c <= 0xef) return 3;
    if (c >= 0xf0 && c <= 0xf4) return 4;
    return 1;
}

static bool renderer_has_text_attrs(agent_token_renderer *r) {
    return r->in_think || r->md_bold || r->md_italic ||
           r->md_inline_code || r->md_code_block;
}

static void renderer_set_text_attrs(agent_token_renderer *r) {
    if (!r->use_color) return;
    if (r->in_think) {
        renderer_set_grey(r);
        return;
    }
    if (r->md_code_block) {
        renderer_write(r, "\x1b[38;5;75m", 10);
        return;
    } else if (r->md_inline_code) {
        renderer_write(r, "\x1b[36m", 5);
    }
    if (r->md_bold) renderer_write(r, "\x1b[1m", 4);
    if (r->md_italic) renderer_write(r, "\x1b[3m", 4);
}

static void renderer_restore_text_attrs(agent_token_renderer *r) {
    if (!r->use_color || !r->color_open || !renderer_has_text_attrs(r)) return;
    renderer_set_text_attrs(r);
}

static void renderer_write_complete_char_raw(agent_token_renderer *r, const char *s, size_t n) {
    bool styled = r->use_color && renderer_has_text_attrs(r);
    if (styled && !r->color_open) {
        renderer_set_text_attrs(r);
        r->color_open = true;
    } else if (!styled && r->color_open) {
        renderer_reset_color(r);
    }
    renderer_write(r, s, n);
    if (n) r->wrote_visible_output = true;
    r->last_output_newline = n == 1 && s[0] == '\n';
}

static void renderer_flush_utf8(agent_token_renderer *r) {
    if (!r->utf8_pending_len) return;
    renderer_write_complete_char_raw(r, r->utf8_pending, r->utf8_pending_len);
    r->utf8_pending_len = 0;
    r->utf8_pending_need = 0;
}

static void renderer_write_char_raw(agent_token_renderer *r, char c) {
    unsigned char uc = (unsigned char)c;

    if (r->utf8_pending_len) {
        if ((uc & 0xc0) == 0x80 && r->utf8_pending_len < sizeof(r->utf8_pending)) {
            r->utf8_pending[r->utf8_pending_len++] = c;
            if (r->utf8_pending_len == r->utf8_pending_need) renderer_flush_utf8(r);
            return;
        }
        renderer_flush_utf8(r);
    }

    size_t need = renderer_utf8_need(uc);
    if (need == 1) {
        renderer_write_complete_char_raw(r, &c, 1);
        return;
    }
    r->utf8_pending[0] = c;
    r->utf8_pending_len = 1;
    r->utf8_pending_need = need;
}

static void renderer_write_plain_byte(agent_token_renderer *r, char c) {
    bool old_bold = r->md_bold;
    bool old_italic = r->md_italic;
    bool old_inline_code = r->md_inline_code;
    bool old_code_block = r->md_code_block;

    /* Code blocks are streamed immediately in plain text, then repainted with
     * syntax colors when a complete terminal-safe line is available.  Disable
     * markdown attributes only for this byte; renderer_write_char_raw() will
     * reset any tracked manual color once if needed. */
    r->md_bold = false;
    r->md_italic = false;
    r->md_inline_code = false;
    r->md_code_block = false;
    renderer_write_char_raw(r, c);
    r->md_bold = old_bold;
    r->md_italic = old_italic;
    r->md_inline_code = old_inline_code;
    r->md_code_block = old_code_block;
}

/* Poor man's code highlighter inspired by antirez/kilo: a tiny language table
 * plus one line-oriented tokenizer for comments, strings, numbers, and
 * separator-bounded keywords.  This is deliberately not a full parser; it is
 * only for making fenced Markdown code readable in the terminal. */
#define AGENT_HL_NORMAL 0
#define AGENT_HL_COMMENT 1
#define AGENT_HL_KEYWORD1 2
#define AGENT_HL_KEYWORD2 3
#define AGENT_HL_STRING 4
#define AGENT_HL_NUMBER 5

#define AGENT_SYNTAX_NUMBERS (1u<<0)
#define AGENT_SYNTAX_STRINGS (1u<<1)
#define AGENT_SYNTAX_BACKTICK_STRINGS (1u<<2)
#define AGENT_SYNTAX_CASE_INSENSITIVE (1u<<3)

struct agent_syntax {
    const char *name;
    const char *aliases;
    const char **keywords;
    const char *singleline_comments[3];
    const char *multiline_start;
    const char *multiline_end;
    unsigned flags;
};

static const char *agent_kw_generic[] = {
    "if","else","for","while","do","switch","case","default","break",
    "continue","return","try","catch","finally","throw","throws","class",
    "struct","enum","interface","trait","impl","fn","func","function",
    "def","lambda","let","var","const","static","public","private",
    "protected","import","include","from","export","package","module",
    "namespace","new","delete","async","await","yield","match","type",
    "true|","false|","null|","nil|","none|","None|","NULL|","void|",
    "int|","long|","float|","double|","char|","bool|","string|",
    "String|","usize|","isize|","u8|","u16|","u32|","u64|","i8|",
    "i16|","i32|","i64|",NULL
};

static const char *agent_kw_c[] = {
    "auto","break","case","continue","default","do","else","enum",
    "extern","for","goto","if","register","return","sizeof","static",
    "struct","switch","typedef","union","volatile","while",
    "alignas","alignof","and","and_eq","asm","bitand","bitor","class",
    "compl","constexpr","const_cast","decltype","delete","dynamic_cast",
    "explicit","export","false","friend","inline","mutable","namespace",
    "new","noexcept","not","not_eq","nullptr","operator","or","or_eq",
    "private","protected","public","reinterpret_cast","static_assert",
    "static_cast","template","this","thread_local","throw","true","try",
    "typeid","typename","virtual","xor","xor_eq",
    "NULL|","bool|","char|","const|","double|","float|","int|","long|",
    "short|","signed|","size_t|","ssize_t|","uint8_t|","uint16_t|",
    "uint32_t|","uint64_t|","unsigned|","void|",NULL
};

static const char *agent_kw_python[] = {
    "and","as","assert","async","await","break","case","class","continue",
    "def","del","elif","else","except","finally","for","from","global",
    "if","import","in","is","lambda","match","nonlocal","not","or","pass",
    "raise","return","try","while","with","yield",
    "False|","None|","True|","bool|","bytes|","dict|","float|","int|",
    "list|","object|","set|","str|","tuple|",NULL
};

static const char *agent_kw_js[] = {
    "async","await","break","case","catch","class","const","continue",
    "debugger","default","delete","do","else","export","extends",
    "finally","for","from","function","get","if","import","in",
    "instanceof","let","new","of","return","set","static","super",
    "switch","this","throw","try","typeof","var","void","while","with",
    "yield","abstract","as","declare","enum","implements","interface",
    "keyof","namespace","private","protected","public","readonly","type",
    "any|","boolean|","false|","never|","null|","number|","string|",
    "symbol|","true|","undefined|","unknown|","void|",NULL
};

static const char *agent_kw_java[] = {
    "abstract","assert","break","case","catch","class","const","continue",
    "default","do","else","enum","extends","final","finally","for","goto",
    "if","implements","import","instanceof","interface","native","new",
    "package","private","protected","public","return","static","strictfp",
    "super","switch","synchronized","this","throw","throws","transient",
    "try","volatile","while",
    "boolean|","byte|","char|","double|","false|","float|","int|","long|",
    "null|","short|","true|","void|",NULL
};

static const char *agent_kw_csharp[] = {
    "abstract","as","base","break","case","catch","checked","class","const",
    "continue","default","delegate","do","else","enum","event","explicit",
    "extern","finally","fixed","for","foreach","goto","if","implicit","in",
    "interface","internal","is","lock","namespace","new","operator","out",
    "override","params","private","protected","public","readonly","ref",
    "return","sealed","sizeof","stackalloc","static","struct","switch",
    "this","throw","try","typeof","unchecked","unsafe","using","virtual",
    "volatile","while","async","await","get","init","record","set","var",
    "bool|","byte|","char|","decimal|","double|","false|","float|","int|",
    "long|","null|","object|","sbyte|","short|","string|","true|","uint|",
    "ulong|","ushort|","void|",NULL
};

static const char *agent_kw_go[] = {
    "break","case","chan","const","continue","default","defer","else",
    "fallthrough","for","func","go","goto","if","import","interface",
    "map","package","range","return","select","struct","switch","type",
    "var","bool|","byte|","complex64|","complex128|","error|","false|",
    "float32|","float64|","int|","int8|","int16|","int32|","int64|",
    "nil|","rune|","string|","true|","uint|","uint8|","uint16|",
    "uint32|","uint64|","uintptr|",NULL
};

static const char *agent_kw_rust[] = {
    "as","async","await","break","const","continue","crate","dyn","else",
    "enum","extern","fn","for","if","impl","in","let","loop","match",
    "mod","move","mut","pub","ref","return","self","Self","static",
    "struct","super","trait","type","unsafe","use","where","while",
    "bool|","char|","false|","f32|","f64|","i8|","i16|","i32|","i64|",
    "i128|","isize|","str|","String|","true|","u8|","u16|","u32|",
    "u64|","u128|","usize|",NULL
};

static const char *agent_kw_shell[] = {
    "case","do","done","elif","else","esac","fi","for","function","if",
    "in","select","then","time","until","while","break","continue",
    "return","export","local","readonly","source","test","true|","false|",
    "echo|","printf|","cd|","pwd|","read|","set|","unset|","shift|",NULL
};

static const char *agent_kw_sql[] = {
    "add","alter","and","as","asc","between","by","case","check","column",
    "constraint","create","delete","desc","distinct","drop","else","end",
    "exists","foreign","from","group","having","in","index","insert",
    "into","is","join","key","left","like","limit","not","null","on",
    "or","order","outer","primary","references","right","select","set",
    "table","then","union","unique","update","values","view","where",
    "bigint|","boolean|","date|","decimal|","false|","int|","integer|",
    "numeric|","real|","text|","timestamp|","true|","varchar|",NULL
};

static const char *agent_kw_ruby[] = {
    "BEGIN","END","alias","and","begin","break","case","class","def",
    "defined?","do","else","elsif","end","ensure","for","if","in",
    "module","next","not","or","redo","rescue","retry","return","self",
    "super","then","undef","unless","until","when","while","yield",
    "false|","nil|","true|",NULL
};

static const char *agent_kw_php[] = {
    "abstract","and","array","as","break","callable","case","catch","class",
    "clone","const","continue","declare","default","die","do","echo","else",
    "elseif","empty","enddeclare","endfor","endforeach","endif","endswitch",
    "endwhile","eval","exit","extends","final","finally","fn","for",
    "foreach","function","global","goto","if","implements","include",
    "include_once","instanceof","insteadof","interface","isset","list",
    "match","namespace","new","or","print","private","protected","public",
    "readonly","require","require_once","return","static","switch","throw",
    "trait","try","unset","use","var","while","xor","bool|","false|",
    "float|","int|","null|","string|","true|","void|",NULL
};

static const char *agent_kw_swift[] = {
    "actor","as","associatedtype","async","await","break","case","catch",
    "class","continue","default","defer","do","else","enum","extension",
    "fallthrough","for","func","guard","if","import","in","init","inout",
    "is","let","nonisolated","operator","private","protocol","public",
    "repeat","return","self","Self","static","struct","subscript","super",
    "switch","throw","throws","try","typealias","var","where","while",
    "Any|","Bool|","Double|","false|","Float|","Int|","nil|","String|",
    "true|","Void|",NULL
};

static const char *agent_kw_kotlin[] = {
    "as","break","class","continue","do","else","false","for","fun","if",
    "in","interface","is","null","object","package","return","super",
    "this","throw","true","try","typealias","typeof","val","var","when",
    "while","actual","annotation","by","catch","companion","const",
    "constructor","crossinline","data","enum","expect","external","final",
    "finally","import","infix","init","inline","inner","internal","lateinit",
    "noinline","open","operator","out","override","private","protected",
    "public","reified","sealed","suspend","tailrec","vararg",
    "Any|","Boolean|","Byte|","Char|","Double|","Float|","Int|","Long|",
    "Short|","String|","Unit|",NULL
};

static const char *agent_kw_zig[] = {
    "addrspace","align","allowzero","and","anyframe","anytype","asm",
    "async","await","break","callconv","catch","comptime","const",
    "continue","defer","else","enum","errdefer","error","export","extern",
    "fn","for","if","inline","linksection","noalias","noinline","nosuspend",
    "opaque","or","orelse","packed","pub","resume","return","struct",
    "suspend","switch","test","threadlocal","try","union","unreachable",
    "usingnamespace","var","volatile","while",
    "bool|","false|","f32|","f64|","i32|","i64|","null|","true|","u8|",
    "u16|","u32|","u64|","usize|","void|",NULL
};

static const char *agent_kw_lua[] = {
    "and","break","do","else","elseif","end","false","for","function",
    "goto","if","in","local","nil","not","or","repeat","return","then",
    "true","until","while",NULL
};

static const char *agent_kw_html[] = {
    "a","body","button","div","doctype","form","h1","h2","h3","head",
    "html","input","label","li","link","main","meta","ol","option","p",
    "script","section","select","span","style","table","tbody","td","th",
    "thead","title","tr","ul","class|","href|","id|","name|","rel|",
    "src|","type|","value|",NULL
};

static const char *agent_kw_css[] = {
    "align-items","background","border","bottom","color","display","flex",
    "font","font-size","gap","grid","height","justify-content","left",
    "margin","max-width","min-width","padding","position","right","top",
    "transform","width","z-index","absolute|","auto|","block|","flex|",
    "grid|","hidden|","inline|","none|","relative|","solid|",NULL
};

static const agent_syntax agent_syntaxes[] = {
    {"generic", " text txt", agent_kw_generic, {"//","#",NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS | AGENT_SYNTAX_BACKTICK_STRINGS},
    {"c", " c h cpp c++ cc cxx hpp hxx objc objective-c", agent_kw_c, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"python", " py python py3", agent_kw_python, {"#",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"javascript", " js jsx javascript typescript ts tsx node mjs cjs", agent_kw_js, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS | AGENT_SYNTAX_BACKTICK_STRINGS},
    {"java", " java", agent_kw_java, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"csharp", " cs c# csharp dotnet", agent_kw_csharp, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"go", " go golang", agent_kw_go, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS | AGENT_SYNTAX_BACKTICK_STRINGS},
    {"rust", " rs rust", agent_kw_rust, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"shell", " sh bash zsh shell fish ksh", agent_kw_shell, {"#",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS | AGENT_SYNTAX_BACKTICK_STRINGS},
    {"sql", " sql postgres mysql sqlite", agent_kw_sql, {"--",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS | AGENT_SYNTAX_CASE_INSENSITIVE},
    {"ruby", " rb ruby", agent_kw_ruby, {"#",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"php", " php", agent_kw_php, {"//","#",NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"swift", " swift", agent_kw_swift, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"kotlin", " kt kts kotlin", agent_kw_kotlin, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"zig", " zig", agent_kw_zig, {"//",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"lua", " lua", agent_kw_lua, {"--",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"html", " html htm xml svg", agent_kw_html, {NULL,NULL,NULL}, "<!--", "-->",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"css", " css scss sass", agent_kw_css, {NULL,NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"json", " json jsonc", NULL, {"//",NULL,NULL}, "/*", "*/",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"yaml", " yaml yml toml ini", NULL, {"#",NULL,NULL}, NULL, NULL,
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {"markdown", " md markdown", agent_kw_generic, {NULL,NULL,NULL}, "<!--", "-->",
        AGENT_SYNTAX_NUMBERS | AGENT_SYNTAX_STRINGS},
    {NULL, NULL, NULL, {NULL,NULL,NULL}, NULL, NULL, 0}
};

static bool agent_syntax_alias_match(const char *aliases, const char *lang) {
    if (!aliases || !lang || !lang[0]) return false;
    size_t llen = strlen(lang);
    const char *p = aliases;
    while (*p) {
        while (*p == ' ') p++;
        const char *start = p;
        while (*p && *p != ' ') p++;
        if ((size_t)(p - start) == llen && !strncasecmp(start, lang, llen))
            return true;
    }
    return false;
}

static const agent_syntax *agent_syntax_for_lang(const char *lang) {
    if (lang && lang[0]) {
        for (const agent_syntax *s = agent_syntaxes; s->name; s++) {
            if (!strcasecmp(s->name, lang) ||
                agent_syntax_alias_match(s->aliases, lang))
                return s;
        }
    }
    return &agent_syntaxes[0];
}

static const agent_syntax *agent_syntax_for_path(const char *path) {
    if (!path || !path[0]) return agent_syntax_for_lang(NULL);
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    if (!strcasecmp(base, "Dockerfile")) return agent_syntax_for_lang("sh");
    if (!strcasecmp(base, "Makefile") || !strcasecmp(base, "makefile"))
        return agent_syntax_for_lang("sh");
    const char *dot = strrchr(base, '.');
    if (!dot || !dot[1]) return agent_syntax_for_lang(NULL);
    return agent_syntax_for_lang(dot + 1);
}

static bool agent_syntax_separator(char c) {
    unsigned char uc = (unsigned char)c;
    return c == '\0' || isspace(uc) || strchr(",.()+-/*=~%[]{}<>:;!&|^?", c) != NULL;
}

static const char *agent_syntax_line_comment(const agent_syntax *syn,
                                             const char *p) {
    if (!syn) return NULL;
    for (int i = 0; i < 3 && syn->singleline_comments[i]; i++) {
        const char *m = syn->singleline_comments[i];
        size_t mlen = strlen(m);
        if (mlen && !strncmp(p, m, mlen)) return m;
    }
    return NULL;
}

static int agent_syntax_color(int hl) {
    switch (hl) {
    case AGENT_HL_COMMENT: return 244;
    case AGENT_HL_KEYWORD1: return 214;
    case AGENT_HL_KEYWORD2: return 81;
    case AGENT_HL_STRING: return 150;
    case AGENT_HL_NUMBER: return 203;
    default: return 252;
    }
}

static void renderer_syntax_write(agent_token_renderer *r, int hl,
                                  const char *s, size_t n) {
    if (!n) return;
    if (hl != AGENT_HL_NORMAL) r->md_syntax_has_highlight = true;
    if (r->md_syntax_silent) return;
    if (r->use_color && hl != AGENT_HL_NORMAL) {
        char seq[32];
        snprintf(seq, sizeof(seq), "\x1b[38;5;%dm", agent_syntax_color(hl));
        renderer_write(r, seq, strlen(seq));
    }
    renderer_write(r, s, n);
    if (r->use_color && hl != AGENT_HL_NORMAL) renderer_write(r, "\x1b[0m", 4);
    r->wrote_visible_output = true;
    r->last_output_newline = false;
}

static void renderer_syntax_write_upto_marker(agent_token_renderer *r) {
    static const char marker[] = "[upto]";
    r->md_syntax_has_highlight = true;
    if (r->md_syntax_silent) return;
    if (r->use_color) {
        renderer_write(r, "\x1b[38;5;244m[", strlen("\x1b[38;5;244m["));
        renderer_write(r, "\x1b[1;38;5;177mupto",
                       strlen("\x1b[1;38;5;177mupto"));
        renderer_write(r, "\x1b[38;5;244m]\x1b[0m",
                       strlen("\x1b[38;5;244m]\x1b[0m"));
    } else {
        renderer_write(r, marker, sizeof(marker) - 1);
    }
    r->wrote_visible_output = true;
    r->last_output_newline = false;
}

static size_t agent_syntax_keyword_len(const char *kw, bool *secondary) {
    size_t len = strlen(kw);
    *secondary = len && kw[len - 1] == '|';
    return *secondary ? len - 1 : len;
}

static bool agent_syntax_match_keyword(const agent_syntax *syn,
                                       const char *p,
                                       const char *line_end,
                                       size_t *out_len,
                                       int *out_hl) {
    if (!syn || !syn->keywords) return false;
    for (int i = 0; syn->keywords[i]; i++) {
        bool secondary = false;
        size_t klen = agent_syntax_keyword_len(syn->keywords[i], &secondary);
        if ((size_t)(line_end - p) < klen) continue;
        bool match = (syn->flags & AGENT_SYNTAX_CASE_INSENSITIVE) ?
            !strncasecmp(p, syn->keywords[i], klen) :
            !strncmp(p, syn->keywords[i], klen);
        if (!match) continue;
        if (!agent_syntax_separator(p[klen])) continue;
        *out_len = klen;
        *out_hl = secondary ? AGENT_HL_KEYWORD2 : AGENT_HL_KEYWORD1;
        return true;
    }
    return false;
}

static bool agent_syntax_number_start(const char *p, const char *line,
                                      bool prev_sep, int prev_hl) {
    unsigned char c = (unsigned char)*p;
    if (isdigit(c) && (prev_sep || prev_hl == AGENT_HL_NUMBER)) return true;
    if (*p == '.' && p > line && prev_hl == AGENT_HL_NUMBER) return true;
    return false;
}

static size_t agent_syntax_number_len(const char *p, const char *line_end) {
    const char *q = p;
    while (q < line_end) {
        unsigned char c = (unsigned char)*q;
        if (isalnum(c) || *q == '_' || *q == '.' || *q == '+' || *q == '-') q++;
        else break;
    }
    return (size_t)(q - p);
}

static void renderer_syntax_emit_line(agent_token_renderer *r,
                                      const char *line, size_t len) {
    const agent_syntax *syn = r->md_syntax ? r->md_syntax : agent_syntax_for_lang(NULL);
    const char *p = line;
    const char *end = line + len;
    bool prev_sep = true;
    int prev_hl = AGENT_HL_NORMAL;
    int in_string = 0;

    while (p < end) {
        if (r->md_code_highlight_upto &&
            (size_t)(end - p) >= strlen("[upto]") &&
            !strncmp(p, "[upto]", strlen("[upto]")))
        {
            renderer_syntax_write_upto_marker(r);
            p += strlen("[upto]");
            prev_sep = true;
            prev_hl = AGENT_HL_NORMAL;
            continue;
        }

        if (r->md_code_in_ml_comment) {
            const char *mce = syn->multiline_end;
            if (mce && *mce) {
                size_t mlen = strlen(mce);
                const char *q = p;
                while (q < end && ((size_t)(end - q) < mlen ||
                       strncmp(q, mce, mlen))) q++;
                if (q < end) {
                    q += mlen;
                    renderer_syntax_write(r, AGENT_HL_COMMENT, p, (size_t)(q - p));
                    p = q;
                    r->md_code_in_ml_comment = false;
                    prev_sep = true;
                    prev_hl = AGENT_HL_COMMENT;
                    continue;
                }
            }
            renderer_syntax_write(r, AGENT_HL_COMMENT, p, (size_t)(end - p));
            return;
        }

        const char *scs = agent_syntax_line_comment(syn, p);
        if (!in_string && scs) {
            renderer_syntax_write(r, AGENT_HL_COMMENT, p, (size_t)(end - p));
            return;
        }

        if (!in_string && syn->multiline_start && syn->multiline_end &&
            !strncmp(p, syn->multiline_start, strlen(syn->multiline_start))) {
            size_t mlen = strlen(syn->multiline_start);
            const char *q = p + mlen;
            size_t elen = strlen(syn->multiline_end);
            while (q < end && ((size_t)(end - q) < elen ||
                   strncmp(q, syn->multiline_end, elen))) q++;
            if (q < end) q += elen;
            else r->md_code_in_ml_comment = true;
            renderer_syntax_write(r, AGENT_HL_COMMENT, p, (size_t)(q - p));
            p = q;
            prev_sep = false;
            prev_hl = AGENT_HL_COMMENT;
            continue;
        }

        if ((syn->flags & AGENT_SYNTAX_STRINGS) && in_string) {
            const char *q = p;
            while (q < end) {
                if (*q == '\\' && q + 1 < end) {
                    q += 2;
                    continue;
                }
                q++;
                if (q[-1] == in_string) {
                    in_string = 0;
                    break;
                }
            }
            renderer_syntax_write(r, AGENT_HL_STRING, p, (size_t)(q - p));
            p = q;
            prev_sep = false;
            prev_hl = AGENT_HL_STRING;
            continue;
        }

        if ((syn->flags & AGENT_SYNTAX_STRINGS) &&
            (*p == '"' || *p == '\'' ||
             ((syn->flags & AGENT_SYNTAX_BACKTICK_STRINGS) && *p == '`'))) {
            int quote = *p;
            const char *q = p + 1;
            while (q < end) {
                if (*q == '\\' && q + 1 < end) {
                    q += 2;
                    continue;
                }
                q++;
                if (q[-1] == quote) {
                    break;
                }
            }
            renderer_syntax_write(r, AGENT_HL_STRING, p, (size_t)(q - p));
            p = q;
            prev_sep = false;
            prev_hl = AGENT_HL_STRING;
            continue;
        }

        if ((syn->flags & AGENT_SYNTAX_NUMBERS) &&
            agent_syntax_number_start(p, line, prev_sep, prev_hl)) {
            size_t nlen = agent_syntax_number_len(p, end);
            renderer_syntax_write(r, AGENT_HL_NUMBER, p, nlen);
            p += nlen;
            prev_sep = false;
            prev_hl = AGENT_HL_NUMBER;
            continue;
        }

        if (prev_sep) {
            size_t klen = 0;
            int khl = AGENT_HL_NORMAL;
            if (agent_syntax_match_keyword(syn, p, end, &klen, &khl)) {
                renderer_syntax_write(r, khl, p, klen);
                p += klen;
                prev_sep = false;
                prev_hl = khl;
                continue;
            }
        }

        renderer_syntax_write(r, AGENT_HL_NORMAL, p, 1);
        prev_sep = agent_syntax_separator(*p);
        prev_hl = AGENT_HL_NORMAL;
        p++;
    }
}

static void renderer_code_line_append(agent_token_renderer *r,
                                      const char *s, size_t n) {
    if (!n) return;
    if (r->md_code_line_len + n + 1 > r->md_code_line_cap) {
        size_t cap = r->md_code_line_cap ? r->md_code_line_cap * 2 : 256;
        while (cap < r->md_code_line_len + n + 1) cap *= 2;
        r->md_code_line = xrealloc(r->md_code_line, cap);
        r->md_code_line_cap = cap;
    }
    memcpy(r->md_code_line + r->md_code_line_len, s, n);
    r->md_code_line_len += n;
    r->md_code_line[r->md_code_line_len] = '\0';
}

static int renderer_terminal_cols(void) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
        return ws.ws_col;
    return 80;
}

static bool renderer_code_line_can_repaint(agent_token_renderer *r) {
    if (!r->use_color || r->capture || r->md_code_line_len == 0) return false;
    int cols = renderer_terminal_cols();
    size_t prefix_len = r->md_code_line_prefix ?
                        strlen(r->md_code_line_prefix) : 0;
    if (cols <= 1 || prefix_len + r->md_code_line_len >= (size_t)cols)
        return false;
    for (size_t i = 0; i < r->md_code_line_len; i++) {
        unsigned char c = (unsigned char)r->md_code_line[i];
        if (c == '\t' || c == 0x1b || c >= 0x80 || (c < 0x20 && c != '\r'))
            return false;
    }
    return true;
}

static void renderer_code_write_line_prefix(agent_token_renderer *r) {
    if (!r->md_code_line_prefix) return;
    if (r->use_color && r->md_code_line_prefix_color)
        renderer_write(r, r->md_code_line_prefix_color,
                       strlen(r->md_code_line_prefix_color));
    renderer_write(r, r->md_code_line_prefix,
                   strlen(r->md_code_line_prefix));
    if (r->use_color && r->md_code_line_prefix_color)
        renderer_write(r, "\x1b[0m", 4);
    r->color_open = false;
}

/* Run the syntax highlighter in silent mode to learn whether the already
 * streamed line would change if repainted, while preserving the multiline
 * comment state until the caller decides whether repaint is safe. */
static bool renderer_code_scan_line(agent_token_renderer *r,
                                    bool *final_ml_comment) {
    bool old_silent = r->md_syntax_silent;
    bool old_highlight = r->md_syntax_has_highlight;
    bool old_ml_comment = r->md_code_in_ml_comment;

    r->md_syntax_silent = true;
    r->md_syntax_has_highlight = false;
    renderer_syntax_emit_line(r, r->md_code_line, r->md_code_line_len);
    bool changed = r->md_syntax_has_highlight;
    *final_ml_comment = r->md_code_in_ml_comment;

    r->md_code_in_ml_comment = old_ml_comment;
    r->md_syntax_silent = old_silent;
    r->md_syntax_has_highlight = old_highlight;
    return changed;
}

/* Code is shown as soon as bytes arrive.  At end-of-line we can cheaply
 * replace only that terminal row with syntax-highlighted text, but only for
 * simple one-row ASCII lines; long, tabbed, escaped, or UTF-8 lines are left
 * as streamed and only advance the highlighter state. */
static void renderer_code_emit_buffered_line(agent_token_renderer *r,
                                             bool with_newline) {
    bool final_ml_comment = r->md_code_in_ml_comment;
    bool changed = renderer_code_scan_line(r, &final_ml_comment);
    bool repaint = changed && renderer_code_line_can_repaint(r);
    if (repaint) {
        renderer_reset_color(r);
        renderer_write(r, "\r\x1b[0K", 5);
        renderer_code_write_line_prefix(r);
        renderer_syntax_emit_line(r, r->md_code_line, r->md_code_line_len);
    } else {
        r->md_code_in_ml_comment = final_ml_comment;
    }
    r->md_code_line_len = 0;
    if (with_newline) {
        renderer_write_plain_byte(r, '\n');
        r->wrote_visible_output = true;
        r->last_output_newline = true;
        r->md_code_line_start = true;
    }
}

static void renderer_code_byte(agent_token_renderer *r, char c) {
    if (c == '\n') {
        renderer_code_emit_buffered_line(r, true);
        return;
    }
    renderer_code_line_append(r, &c, 1);
    renderer_write_plain_byte(r, c);
    if (c != ' ' && c != '\t' && c != '\r') r->md_code_line_start = false;
}

static void renderer_code_emit_backtick_literals(agent_token_renderer *r,
                                                 size_t count) {
    for (size_t i = 0; i < count; i++) renderer_code_byte(r, '`');
}

static void renderer_code_begin(agent_token_renderer *r) {
    renderer_reset_color(r);
    r->md_code_block = true;
    r->md_inline_code = false;
    r->md_fence_info = true;
    r->md_code_line_start = true;
    r->md_code_in_ml_comment = false;
    r->md_syntax = agent_syntax_for_lang(NULL);
    r->md_fence_lang_len = 0;
    r->md_fence_lang[0] = '\0';
    r->md_code_line_prefix = NULL;
    r->md_code_line_prefix_color = NULL;
    r->md_code_highlight_upto = false;
    r->md_code_line_len = 0;
}

static void renderer_code_stream_begin(agent_token_renderer *r,
                                       const agent_syntax *syntax) {
    renderer_reset_color(r);
    r->md_code_block = true;
    r->md_inline_code = false;
    r->md_fence_info = false;
    r->md_code_line_start = true;
    r->md_code_in_ml_comment = false;
    r->md_syntax = syntax ? syntax : agent_syntax_for_lang(NULL);
    r->md_fence_lang_len = 0;
    r->md_fence_lang[0] = '\0';
    r->md_code_line_prefix = NULL;
    r->md_code_line_prefix_color = NULL;
    r->md_code_highlight_upto = false;
    r->md_code_line_len = 0;
}

static void renderer_code_stream_set_prefix(agent_token_renderer *r,
                                            const char *prefix,
                                            const char *color) {
    r->md_code_line_prefix = prefix;
    r->md_code_line_prefix_color = color;
}

static void renderer_code_stream_set_upto_marker(agent_token_renderer *r,
                                                 bool enabled) {
    r->md_code_highlight_upto = enabled;
}

static void renderer_code_end(agent_token_renderer *r) {
    bool only_space = true;
    for (size_t i = 0; i < r->md_code_line_len; i++) {
        if (r->md_code_line[i] != ' ' && r->md_code_line[i] != '\t' &&
            r->md_code_line[i] != '\r') {
            only_space = false;
            break;
        }
    }
    if (r->md_code_line_len && !only_space)
        renderer_code_emit_buffered_line(r, false);
    else
        r->md_code_line_len = 0;
    r->md_code_block = false;
    r->md_inline_code = false;
    r->md_fence_info = false;
    r->md_code_line_start = true;
    r->md_code_in_ml_comment = false;
    r->md_syntax = NULL;
    r->md_fence_lang_len = 0;
    r->md_fence_lang[0] = '\0';
    r->md_code_line_prefix = NULL;
    r->md_code_line_prefix_color = NULL;
}

/* Tiny streaming Markdown highlighter for assistant prose.  It deliberately
 * recognizes only delimiters that the model commonly emits in short answers:
 * **bold**, *italic*, `inline code`, ``inline code`` and fenced code blocks.
 * The state machine holds only possible delimiter bytes; once a byte is known
 * to be ordinary text it is sent to the raw UTF-8 writer above.  Tool
 * visualization and redirected output bypass this layer. */
static void renderer_markdown_clear_pending(agent_token_renderer *r) {
    r->md_pending = AGENT_MD_PENDING_NONE;
    r->md_pending_len = 0;
}

static void renderer_markdown_emit_pending_literals(agent_token_renderer *r) {
    char c;
    if (r->md_pending == AGENT_MD_PENDING_STAR) {
        c = '*';
    } else if (r->md_pending == AGENT_MD_PENDING_BACKTICK) {
        c = '`';
    } else {
        return;
    }
    size_t count = r->md_pending_len;
    renderer_markdown_clear_pending(r);
    if (r->md_code_block) {
        if (c == '`') renderer_code_emit_backtick_literals(r, count);
        else for (size_t i = 0; i < count; i++) renderer_code_byte(r, c);
        return;
    }
    for (size_t i = 0; i < count; i++) renderer_write_char_raw(r, c);
}

static void renderer_markdown_commit_backticks(agent_token_renderer *r) {
    size_t count = r->md_pending_len;
    renderer_markdown_clear_pending(r);
    if (count >= 3) {
        for (size_t i = 0; i < count; i++) renderer_write_plain_byte(r, '`');
        if (r->md_code_block) renderer_code_end(r);
        else renderer_code_begin(r);
        return;
    }
    if (r->md_code_block) {
        renderer_code_emit_backtick_literals(r, count);
        return;
    }
    /* Support both `code` and ``code``.  The latter is uncommon in model
     * replies, but accepting it costs nothing and avoids leaking delimiters. */
    r->md_inline_code = !r->md_inline_code;
}

static bool renderer_space_byte(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

/* Consume one byte of markdown-aware assistant output.  Backticks and stars
 * are held in r->pending until the parser knows whether they form a marker;
 * all ordinary text is emitted with the current terminal attributes. */
static void renderer_markdown_feed(agent_token_renderer *r, char c) {
    if (r->md_fence_info) {
        if (c == '\n') {
            if (r->md_code_block) {
                r->md_fence_lang[r->md_fence_lang_len] = '\0';
                r->md_syntax = agent_syntax_for_lang(r->md_fence_lang);
            }
            renderer_write_plain_byte(r, '\n');
            r->md_fence_info = false;
        } else if (r->md_code_block) {
            unsigned char uc = (unsigned char)c;
            if (r->md_fence_lang_len + 1 < sizeof(r->md_fence_lang) &&
                (isalnum(uc) || c == '_' || c == '-' || c == '+' || c == '#'))
            {
                r->md_fence_lang[r->md_fence_lang_len++] = c;
            }
            renderer_write_plain_byte(r, c);
        }
        return;
    }

    if (r->md_pending == AGENT_MD_PENDING_BACKTICK) {
        if (c == '`') {
            r->md_pending_len++;
            return;
        }
        renderer_markdown_commit_backticks(r);
        renderer_markdown_feed(r, c);
        return;
    }

    if (r->md_pending == AGENT_MD_PENDING_STAR) {
        renderer_markdown_clear_pending(r);
        if (!r->md_inline_code && !r->md_code_block && c == '*') {
            r->md_bold = !r->md_bold;
            return;
        }
        if (!r->md_inline_code && !r->md_code_block &&
            (r->md_italic || !renderer_space_byte(c)))
        {
            r->md_italic = !r->md_italic;
            renderer_markdown_feed(r, c);
            return;
        }
        renderer_write_char_raw(r, '*');
        renderer_markdown_feed(r, c);
        return;
    }

    if (c == '`' && (!r->md_code_block || r->md_code_line_start)) {
        r->md_pending = AGENT_MD_PENDING_BACKTICK;
        r->md_pending_len = 1;
        return;
    }
    if (r->md_code_block) {
        renderer_code_byte(r, c);
        return;
    }
    if (!r->md_inline_code && !r->md_code_block && c == '*') {
        r->md_pending = AGENT_MD_PENDING_STAR;
        r->md_pending_len = 1;
        return;
    }
    renderer_write_char_raw(r, c);
}

static void renderer_markdown_finish(agent_token_renderer *r) {
    /* A closing code fence can be the final bytes of the assistant reply.  In
     * that case no following character arrives to force the pending backticks
     * through the normal streaming path, so commit a full fence here instead of
     * leaking the literal ``` marker to the terminal. */
    if (r->md_pending == AGENT_MD_PENDING_BACKTICK && r->md_pending_len >= 3)
        renderer_markdown_commit_backticks(r);
    else
        renderer_markdown_emit_pending_literals(r);
    if (r->md_code_block && r->md_code_line_len)
        renderer_code_emit_buffered_line(r, false);
    r->md_bold = false;
    r->md_italic = false;
    r->md_inline_code = false;
    r->md_code_block = false;
    r->md_fence_info = false;
    r->md_code_line_start = false;
    r->md_code_in_ml_comment = false;
    r->md_syntax = NULL;
    r->md_fence_lang_len = 0;
    r->md_fence_lang[0] = '\0';
    r->md_code_line_prefix = NULL;
    r->md_code_line_prefix_color = NULL;
    r->md_code_highlight_upto = false;
    free(r->md_code_line);
    r->md_code_line = NULL;
    r->md_code_line_len = 0;
    r->md_code_line_cap = 0;
}

static void renderer_write_char(agent_token_renderer *r, char c) {
    if (!r->format_markdown || r->in_think) {
        renderer_markdown_emit_pending_literals(r);
        renderer_write_char_raw(r, c);
        return;
    }
    renderer_markdown_feed(r, c);
}

/* Render assistant text while hiding <think> tags and dimming thinking text.
 * The function is also responsible for not prematurely emitting a partial
 * control tag split across model tokens. */
static void renderer_process(agent_token_renderer *r, const char *text, size_t len, bool finish) {
    const char *think_open = "<think>";
    const char *think_close = "</think>";
    size_t total = r->pending_len + len;
    char *buf = xmalloc(total ? total : 1);
    if (r->pending_len) memcpy(buf, r->pending, r->pending_len);
    if (len) memcpy(buf + r->pending_len, text, len);
    r->pending_len = 0;

    size_t i = 0;
    while (i < total) {
        const char *cur = buf + i;
        size_t rem = total - i;
        if (bytes_has_prefix(cur, rem, think_open)) {
            r->in_think = true;
            i += strlen(think_open);
            continue;
        }
        if (bytes_has_prefix(cur, rem, think_close)) {
            r->in_think = false;
            renderer_reset_color(r);
            if (!r->last_output_newline) renderer_write(r, "\n", 1);
            renderer_write(r, "\n", 1);
            r->last_output_newline = true;
            i += strlen(think_close);
            continue;
        }
        if (!finish && cur[0] == '<' &&
            (bytes_is_partial_prefix(cur, rem, think_open) ||
             bytes_is_partial_prefix(cur, rem, think_close)))
        {
            if (rem < sizeof(r->pending)) {
                memcpy(r->pending, cur, rem);
                r->pending_len = rem;
            }
            break;
        }
        renderer_write_char(r, cur[0]);
        i++;
    }
    free(buf);
}

static void renderer_finish(agent_token_renderer *r) {
    if (r->format_thinking) {
        renderer_process(r, NULL, 0, true);
    }
    renderer_markdown_finish(r);
    renderer_flush_utf8(r);
    renderer_reset_color(r);
    if (r->wrote_visible_output) {
        if (!r->last_output_newline) renderer_write(r, "\n", 1);
        renderer_write(r, "\n", 1);
        r->last_output_newline = true;
    }
}

static void renderer_color(agent_token_renderer *r, const char *seq) {
    renderer_markdown_emit_pending_literals(r);
    renderer_flush_utf8(r);
    bool reset = !seq || !seq[0] || !strcmp(seq, "\x1b[0m");
    if (r->use_color && seq && seq[0]) renderer_write(r, seq, strlen(seq));
    r->color_open = r->use_color && !reset;
}

static void renderer_plain(agent_token_renderer *r, const char *s, size_t n) {
    renderer_markdown_emit_pending_literals(r);
    renderer_flush_utf8(r);
    renderer_write(r, s, n);
    if (n) r->wrote_visible_output = true;
    if (n) r->last_output_newline = s[n - 1] == '\n';
}

/* ============================================================================
 * Streaming Tool Visualization
 * ============================================================================
 *
 * Tool calls are parsed for execution later, but they are also visualized while
 * the model is still sampling.  This state machine suppresses raw DSML and
 * prints compact, tool-specific progress such as "$ command" or
 * "Reading file 1:500...".
 */

static bool streq_any(const char *s, const char *a, const char *b,
                      const char *c, const char *d) {
    return (a && !strcmp(s, a)) || (b && !strcmp(s, b)) ||
           (c && !strcmp(s, c)) || (d && !strcmp(s, d));
}

static agent_tool_param_kind agent_tool_param_kind_for(const char *tool, const char *param) {
    if (!tool) tool = "";
    if (!param) param = "";
    if (!strcmp(tool, "bash") && !strcmp(param, "command"))
        return AGENT_TOOL_PARAM_BASH_COMMAND;
    if (!strcmp(tool, "edit") && !strcmp(param, "old"))
        return AGENT_TOOL_PARAM_DIFF_OLD;
    if (!strcmp(tool, "edit") && !strcmp(param, "new"))
        return AGENT_TOOL_PARAM_DIFF_NEW;
    if (streq_any(param, "path", "file", "filename", NULL))
        return AGENT_TOOL_PARAM_PATH;
    if (streq_any(param, "line", "start_line", "end_line", "offset") ||
        streq_any(param, "start", "end", "count", "max_lines") ||
        streq_any(param, "timeout_sec", "refresh_sec", NULL, NULL))
        return AGENT_TOOL_PARAM_OFFSET;
    if (streq_any(param, "content", "text", NULL, NULL))
        return AGENT_TOOL_PARAM_CONTENT;
    return AGENT_TOOL_PARAM_NORMAL;
}

static const char *agent_tool_param_color(agent_tool_param_kind kind) {
    switch (kind) {
    case AGENT_TOOL_PARAM_PATH: return "\x1b[32m";
    case AGENT_TOOL_PARAM_OFFSET: return "\x1b[33m";
    case AGENT_TOOL_PARAM_CONTENT: return "\x1b[34m";
    case AGENT_TOOL_PARAM_DIFF_OLD: return "\x1b[31m";
    case AGENT_TOOL_PARAM_DIFF_NEW: return "\x1b[32m";
    case AGENT_TOOL_PARAM_BASH_COMMAND: return "\x1b[1;36m";
    default: return "\x1b[37m";
    }
}

static void agent_tool_viz_write(agent_stream_renderer *sr, const char *s, size_t n) {
    renderer_plain(sr->renderer, s, n);
    for (size_t i = 0; i < n; i++) sr->viz.last_output_newline = s[i] == '\n';
}

static void agent_tool_viz_puts(agent_stream_renderer *sr, const char *s) {
    agent_tool_viz_write(sr, s, strlen(s));
}

static void agent_tool_viz_start(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    bool line_open = !sr->renderer->last_output_newline;
    memset(v, 0, sizeof(*v));
    v->active = true;
    v->at_line_start = true;
    v->last_output_newline = true;
    if (sr->replay) {
        if (line_open) agent_tool_viz_puts(sr, "\n");
    } else if (sr->renderer->use_color) {
        /* The raw DSML start marker may arrive after ordinary text on the
         * current row.  Clear that row only for the live terminal UI; plain
         * stdout mode must never leak cursor-control escapes into pipes. */
        agent_tool_viz_puts(sr, "\r\x1b[2K");
    } else if (line_open) {
        agent_tool_viz_puts(sr, "\n");
    }
    v->last_output_newline = true;
}

static void agent_tool_viz_line_prefix(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
    agent_tool_viz_puts(sr, "🛠️ ");
    v->at_line_start = false;
}

static const char *agent_tool_viz_prefix(const char *name) {
    if (!strcmp(name, "bash")) return "$ ";
    if (!strcmp(name, "read")) return "read ";
    if (!strcmp(name, "write")) return "write ";
    if (!strcmp(name, "edit")) return "edit ";
    if (!strcmp(name, "search")) return "search ";
    if (!strcmp(name, "web_browse")) return "web ";
    if (!strcmp(name, "web_fetch")) return "fetch ";
    return NULL;
}

static void agent_tool_viz_tool(agent_stream_renderer *sr, const char *name) {
    agent_tool_visualizer *v = &sr->viz;
    if (v->tool_announced && !strcmp(v->tool_name, name)) return;
    if (v->tool_announced && !v->last_output_newline) agent_tool_viz_puts(sr, "\n");
    snprintf(v->tool_name, sizeof(v->tool_name), "%s", name ? name : "tool");
    v->tool_announced = true;
    v->read_style = !strcmp(v->tool_name, "read");
    v->ask_question_style = !strcmp(v->tool_name, "ask_question");
    agent_tool_viz_line_prefix(sr);
    if (v->read_style) {
        renderer_color(sr->renderer, "\x1b[1;37m");
        agent_tool_viz_puts(sr, "Reading ");
        renderer_color(sr->renderer, "\x1b[32m");
        v->read_prefix_rendered = true;
        return;
    }
    renderer_color(sr->renderer, !strcmp(v->tool_name, "bash") ?
                                "\x1b[1;36m" : "\x1b[1;37m");
    const char *prefix = agent_tool_viz_prefix(v->tool_name);
    if (prefix) {
        agent_tool_viz_puts(sr, prefix);
    } else {
        agent_tool_viz_puts(sr, v->tool_name);
        agent_tool_viz_puts(sr, " ");
    }
    renderer_color(sr->renderer, "\x1b[0m");
}

static void agent_tool_viz_append(char *dst, size_t cap, char c) {
    size_t len = strlen(dst);
    if (len + 1 >= cap) return;
    dst[len] = c;
    dst[len + 1] = '\0';
}

static void agent_tool_viz_question_preview_byte(agent_tool_visualizer *v,
                                                 char c) {
    if (v->ask_question_preview_done) return;
    unsigned char uc = (unsigned char)c;
    if (c == '\r' || c == '\n') {
        if (v->ask_question_preview_started)
            v->ask_question_preview_done = true;
        return;
    }
    if (!v->ask_question_preview_started) {
        if (uc <= ' ' || uc >= 0x80) return;
        v->ask_question_preview_started = true;
    }
    if (uc < ' ' && c != '\t') return;
    if (c == '\t') c = ' ';
    agent_tool_viz_append(v->ask_question_preview,
                          sizeof(v->ask_question_preview), c);
}

static void agent_tool_viz_read_value_byte(agent_stream_renderer *sr, char c) {
    agent_tool_visualizer *v = &sr->viz;
    if (!strcmp(v->param_name, "path")) {
        agent_tool_viz_append(v->read_path, sizeof(v->read_path), c);
        if (v->read_prefix_rendered) agent_tool_viz_write(sr, &c, 1);
    } else if (!strcmp(v->param_name, "start_line")) {
        agent_tool_viz_append(v->read_start, sizeof(v->read_start), c);
    } else if (!strcmp(v->param_name, "max_lines")) {
        agent_tool_viz_append(v->read_max, sizeof(v->read_max), c);
    } else if (!strcmp(v->param_name, "whole")) {
        agent_tool_viz_append(v->read_whole, sizeof(v->read_whole), c);
    }
}

static void agent_tool_viz_render_read(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->read_style || v->read_line_rendered) return;

    if (!v->read_prefix_rendered) {
        agent_tool_viz_line_prefix(sr);
        renderer_color(sr->renderer, "\x1b[1;37m");
        agent_tool_viz_puts(sr, "Reading ");
        renderer_color(sr->renderer, "\x1b[32m");
        agent_tool_viz_puts(sr, v->read_path[0] ? v->read_path : "<unknown>");
    } else if (!v->read_path[0]) {
        renderer_color(sr->renderer, "\x1b[32m");
        agent_tool_viz_puts(sr, "<unknown>");
    }
    renderer_color(sr->renderer, "\x1b[33m");
    bool whole = agent_parse_bool_default(v->read_whole, false);
    if (whole && (!v->read_start[0] || !strcmp(v->read_start, "1"))) {
        agent_tool_viz_puts(sr, " (whole file)");
    } else if (whole) {
        agent_tool_viz_puts(sr, " ");
        agent_tool_viz_puts(sr, v->read_start);
        agent_tool_viz_puts(sr, ":EOF");
    } else {
        agent_tool_viz_puts(sr, " ");
        agent_tool_viz_puts(sr, v->read_start[0] ? v->read_start : "1");
        agent_tool_viz_puts(sr, ":");
        agent_tool_viz_puts(sr, v->read_max[0] ? v->read_max : "500");
    }
    renderer_color(sr->renderer, "\x1b[1;37m");
    agent_tool_viz_puts(sr, "...");
    renderer_color(sr->renderer, "\x1b[0m");
    agent_tool_viz_puts(sr, "\n");
    v->read_line_rendered = true;
}

static bool agent_tool_viz_param_is_code_body(agent_tool_visualizer *v) {
    if (!strcmp(v->tool_name, "write") &&
        v->param_kind == AGENT_TOOL_PARAM_CONTENT)
        return true;
    if (!strcmp(v->tool_name, "edit") &&
        (v->param_kind == AGENT_TOOL_PARAM_DIFF_OLD ||
         v->param_kind == AGENT_TOOL_PARAM_DIFF_NEW ||
         v->param_kind == AGENT_TOOL_PARAM_CONTENT))
        return true;
    return false;
}

static const char *agent_tool_viz_diff_prefix(agent_tool_param_kind kind,
                                              const char **color) {
    if (color) *color = NULL;
    const char *prefix = NULL;
    if (kind == AGENT_TOOL_PARAM_DIFF_OLD) {
        prefix = "- ";
        if (color) *color = "\x1b[31m";
    } else if (kind == AGENT_TOOL_PARAM_DIFF_NEW) {
        prefix = "+ ";
        if (color) *color = "\x1b[32m";
    }
    return prefix;
}

static void agent_tool_viz_code_prefix(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->at_line_start) return;
    const char *color = NULL;
    const char *prefix = agent_tool_viz_diff_prefix(v->param_kind, &color);
    if (!prefix) return;
    renderer_color(sr->renderer, color);
    renderer_write(sr->renderer, prefix, strlen(prefix));
    renderer_color(sr->renderer, "\x1b[0m");
    sr->renderer->wrote_visible_output = true;
    sr->renderer->last_output_newline = false;
    v->last_output_newline = false;
    v->at_line_start = false;
}

static void agent_tool_viz_code_begin(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    const agent_syntax *syntax = agent_syntax_for_path(v->tool_path);
    renderer_code_stream_begin(sr->renderer, syntax);
    renderer_code_stream_set_upto_marker(sr->renderer,
        !strcmp(v->tool_name, "edit") &&
        v->param_kind == AGENT_TOOL_PARAM_DIFF_OLD);
    v->code_param_active = true;
    if (v->param_kind == AGENT_TOOL_PARAM_DIFF_OLD ||
        v->param_kind == AGENT_TOOL_PARAM_DIFF_NEW)
    {
        const char *color = NULL;
        const char *prefix = agent_tool_viz_diff_prefix(v->param_kind, &color);
        /* Diff prefixes are terminal UI, not code.  Keep them outside the
         * syntax buffer so a later row repaint preserves their red/green color
         * while highlighting only the actual edited line. */
        renderer_code_stream_set_prefix(sr->renderer, prefix, color);
        agent_tool_viz_code_prefix(sr);
    }
}

static void agent_tool_viz_code_end(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->code_param_active) return;
    renderer_code_end(sr->renderer);
    v->code_param_active = false;
    v->at_line_start = true;
    v->last_output_newline = sr->renderer->last_output_newline;
}

static void agent_tool_viz_code_byte(agent_stream_renderer *sr, char c) {
    agent_tool_visualizer *v = &sr->viz;
    agent_tool_viz_code_prefix(sr);
    renderer_code_byte(sr->renderer, c);
    v->last_output_newline = c == '\n';
    v->at_line_start = c == '\n';
}

static void agent_tool_viz_param_begin(agent_stream_renderer *sr, const char *name) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->tool_announced && sr->parser->current.name)
        agent_tool_viz_tool(sr, sr->parser->current.name);
    snprintf(v->param_name, sizeof(v->param_name), "%s", name ? name : "");
    v->param_kind = agent_tool_param_kind_for(v->tool_name, v->param_name);
    v->param_active = true;
    v->param_end_len = 0;
    v->ask_question_question_active = false;

    if (v->read_style) return;

    if (v->ask_question_style) {
        if (!strcmp(v->param_name, "question")) {
            if (!v->at_line_start) agent_tool_viz_puts(sr, " ");
            renderer_color(sr->renderer, "\x1b[1;37m");
            agent_tool_viz_puts(sr, "question=");
            renderer_color(sr->renderer, agent_tool_param_color(v->param_kind));
            v->ask_question_question_active = true;
            v->ask_question_preview_started = false;
            v->ask_question_preview_done = false;
            v->ask_question_preview[0] = '\0';
        }
        return;
    }

    if (v->param_kind == AGENT_TOOL_PARAM_DIFF_OLD ||
        v->param_kind == AGENT_TOOL_PARAM_DIFF_NEW)
    {
        if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
        v->at_line_start = true;
        agent_tool_viz_code_begin(sr);
        return;
    }

    if (v->param_kind == AGENT_TOOL_PARAM_CONTENT) {
        if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
        if (strcmp(v->tool_name, "write")) {
            renderer_color(sr->renderer, "\x1b[1;37m");
            agent_tool_viz_puts(sr, v->param_name);
            agent_tool_viz_puts(sr, ":\n");
        }
        v->at_line_start = true;
        if (agent_tool_viz_param_is_code_body(v)) {
            agent_tool_viz_code_begin(sr);
        } else {
            renderer_color(sr->renderer, "\x1b[34m");
        }
        return;
    }

    if (v->param_kind != AGENT_TOOL_PARAM_BASH_COMMAND) {
        if (!v->at_line_start) agent_tool_viz_puts(sr, " ");
        renderer_color(sr->renderer, "\x1b[1;37m");
        agent_tool_viz_puts(sr, v->param_name);
        agent_tool_viz_puts(sr, "=");
    } else {
        renderer_color(sr->renderer, agent_tool_param_color(AGENT_TOOL_PARAM_BASH_COMMAND));
        return;
    }
    renderer_color(sr->renderer, agent_tool_param_color(v->param_kind));
}

static void agent_tool_viz_param_end(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    v->param_end_len = 0;
    if (v->code_param_active) agent_tool_viz_code_end(sr);
    if (v->ask_question_question_active) {
        agent_tool_viz_puts(sr, v->ask_question_preview[0] ?
                            v->ask_question_preview : "<question>");
        v->at_line_start = false;
    }
    if (!v->read_style) renderer_color(sr->renderer, "\x1b[0m");
    v->ask_question_question_active = false;
    v->param_active = false;
    v->param_name[0] = '\0';
}

static void agent_tool_viz_param_raw_byte(agent_stream_renderer *sr, char c) {
    agent_tool_visualizer *v = &sr->viz;
    if (v->read_style) {
        agent_tool_viz_read_value_byte(sr, c);
        return;
    }
    if (v->ask_question_style) {
        if (v->ask_question_question_active)
            agent_tool_viz_question_preview_byte(v, c);
        return;
    }
    if (v->param_kind == AGENT_TOOL_PARAM_PATH) {
        agent_tool_viz_append(v->tool_path, sizeof(v->tool_path), c);
    }
    if (v->code_param_active) {
        agent_tool_viz_code_byte(sr, c);
        return;
    }
    if (v->param_kind == AGENT_TOOL_PARAM_BASH_COMMAND) {
        agent_tool_viz_write(sr, &c, 1);
        v->at_line_start = c == '\n';
        return;
    }
    if (v->param_kind == AGENT_TOOL_PARAM_DIFF_OLD ||
        v->param_kind == AGENT_TOOL_PARAM_DIFF_NEW)
    {
        agent_tool_viz_code_begin(sr);
        agent_tool_viz_code_byte(sr, c);
        return;
    }
    agent_tool_viz_write(sr, &c, 1);
    v->at_line_start = c == '\n';
}

static void agent_tool_viz_restore_param_color(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->active || !v->param_active || v->read_style) return;
    renderer_color(sr->renderer, agent_tool_param_color(v->param_kind));
}

/* Stream one DSML parameter byte into the visualizer.  The visualizer must not
 * wait for the whole parameter: large write/edit contents should show progress
 * as the model emits them, while still detecting the closing parameter tag. */
static void agent_tool_viz_param_value_byte(agent_stream_renderer *sr, char c) {
    agent_tool_visualizer *v = &sr->viz;

    if (v->param_end_len || c == '<') {
        if (v->param_end_len == sizeof(v->param_end_tail)) {
            size_t keep = v->param_end_len;
            v->param_end_len = 0;
            for (size_t i = 0; i < keep; i++)
                agent_tool_viz_param_raw_byte(sr, v->param_end_tail[i]);
            if (c != '<') {
                agent_tool_viz_param_raw_byte(sr, c);
                return;
            }
        }
        if (v->param_end_len < sizeof(v->param_end_tail))
            v->param_end_tail[v->param_end_len++] = c;
        bool complete = false;
        if (agent_dsml_parameter_close_tail(v->param_end_tail, v->param_end_len, &complete)) {
            if (complete) agent_tool_viz_param_end(sr);
            return;
        }
        size_t keep = v->param_end_len;
        v->param_end_len = 0;
        for (size_t i = 0; i < keep; i++)
            agent_tool_viz_param_raw_byte(sr, v->param_end_tail[i]);
        return;
    }
    agent_tool_viz_param_raw_byte(sr, c);
}

static void agent_tool_viz_finish(agent_stream_renderer *sr, const char *status) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->active) return;
    if (v->param_active) agent_tool_viz_param_end(sr);
    if (!status || !status[0]) agent_tool_viz_render_read(sr);
    if (status && status[0]) {
        if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
        renderer_color(sr->renderer, "\x1b[90m");
        agent_tool_viz_puts(sr, status);
        renderer_color(sr->renderer, "\x1b[0m");
    }
    if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
    v->active = false;
}

static void agent_stream_render_rejected_thinking_block(
    agent_stream_renderer *sr, const char *detail) {
    if (!sr || sr->dsml_origin_rendered) return;
    sr->dsml_origin_rendered = true;
    if (!sr->renderer->last_output_newline)
        renderer_plain(sr->renderer, "\n", 1);
    renderer_color(sr->renderer, "\x1b[1;31m");
    renderer_plain(sr->renderer, "[tool call rejected: ", 21);
    renderer_plain(sr->renderer, detail && detail[0] ? detail :
                   "invalid in-thinking tool block",
                   strlen(detail && detail[0] ? detail :
                          "invalid in-thinking tool block"));
    renderer_plain(sr->renderer, "]\n", 2);
    renderer_color(sr->renderer, "\x1b[0m");
}

/* In-thinking calls stay invisible until the complete block passes phase
 * validation. Render from parsed calls afterward; never replay raw DSML. */
static void agent_stream_render_validated_thinking_block(
    agent_stream_renderer *sr) {
    if (!sr || sr->dsml_origin_rendered) return;
    sr->dsml_origin_rendered = true;
    for (int i = 0; i < sr->parser->calls.len; i++) {
        const agent_tool_call *call = &sr->parser->calls.v[i];
        agent_tool_viz_start(sr);
        agent_tool_viz_tool(sr, call->name ? call->name : "tool");
        for (int j = 0; j < call->argc; j++) {
            const agent_tool_arg *arg = &call->args[j];
            agent_tool_viz_param_begin(sr, arg->name);
            if (arg->value)
                for (const char *p = arg->value; *p; p++)
                    agent_tool_viz_param_raw_byte(sr, *p);
            agent_tool_viz_param_end(sr);
        }
        agent_tool_viz_finish(sr, NULL);
    }
}

static void agent_tool_viz_dump_invalid_dsml(agent_stream_renderer *sr) {
    agent_tool_visualizer *v = &sr->viz;
    if (!v->active) return;

    /* The normal path hides DSML and paints a friendly semantic projection.  If
     * parsing fails, show the exact bytes we rejected so the next fix is based
     * on evidence instead of guessing from the projection. */
    if (v->param_active) {
        v->param_active = false;
        v->param_end_len = 0;
        v->param_name[0] = '\0';
    }
    if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
    renderer_color(sr->renderer, "\x1b[1;31m");
    if (sr->parser->raw && sr->parser->raw_len) {
        agent_tool_viz_write(sr, sr->parser->raw, sr->parser->raw_len);
    } else {
        agent_tool_viz_puts(sr, "<empty DSML>");
    }
    renderer_color(sr->renderer, "\x1b[0m");
    if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
}

static void agent_stream_finish_ignored_dsml(agent_stream_renderer *sr, const char *detail) {
    const char *msg =
        detail && detail[0] ? detail :
        "malformed DSML tool call inside <think></think>";
    sr->dsml_in_think = true;
    sr->dsml_in_think_reported = true;
    agent_trace(sr->renderer->worker, "dsml ignored inside thinking: %s", msg);
    if (!sr->renderer->last_output_newline)
        renderer_plain(sr->renderer, "\n", 1);
    renderer_color(sr->renderer, "\x1b[1;31m");
    renderer_plain(sr->renderer, "[tool call ignored: ", 20);
    renderer_plain(sr->renderer, msg, strlen(msg));
    renderer_plain(sr->renderer, "]\n", 2);
    renderer_color(sr->renderer, "\x1b[0m");
    agent_dsml_parser_reset(sr->parser);
    sr->dsml_active = false;
    sr->dsml_ignored = false;
}

static void agent_stream_malformed_dsml(agent_stream_renderer *sr,
                                        const char *detail) {
    const char *msg = detail && detail[0] ? detail :
        "DSML markup outside a valid tool_calls block";
    if (sr->parser->state == AGENT_DSML_ERROR) return;
    agent_dsml_set_error(sr->parser, msg);
    agent_trace(sr->renderer->worker, "malformed dsml in assistant output: %s", msg);
    if (!sr->renderer->last_output_newline)
        renderer_plain(sr->renderer, "\n", 1);
    renderer_color(sr->renderer, "\x1b[1;31m");
    renderer_plain(sr->renderer, "[invalid tool call: ", 20);
    renderer_plain(sr->renderer, msg, strlen(msg));
    renderer_plain(sr->renderer, "]\n", 2);
    renderer_color(sr->renderer, "\x1b[0m");
}

/* Mirror parser progress into the terminal visualizer.  Parser state is the
 * source of truth; this function only decides what the user should see. */
static void agent_stream_tool_events(agent_stream_renderer *sr) {
    agent_dsml_parser *p = sr->parser;
    agent_tool_visualizer *v = &sr->viz;
    if (!v->tool_announced && p->current.name)
        agent_tool_viz_tool(sr, p->current.name);
    if (v->tool_announced && !p->current.name && !v->param_active) {
        agent_tool_viz_render_read(sr);
        if (!v->last_output_newline) agent_tool_viz_puts(sr, "\n");
        v->read_style = false;
        v->read_prefix_rendered = false;
        v->read_line_rendered = false;
        v->read_path[0] = '\0';
        v->read_start[0] = '\0';
        v->read_max[0] = '\0';
        v->read_whole[0] = '\0';
        v->tool_announced = false;
    }
    if (!v->param_active && p->state == AGENT_DSML_PARAM_VALUE && p->param_name)
        agent_tool_viz_param_begin(sr, p->param_name);
}

static void agent_stream_preflight_closed_param(agent_stream_renderer *sr) {
    if (!sr || sr->replay || sr->dsml_ignored || sr->dsml_origin_in_think ||
        sr->tool_preflight_error)
        return;
    agent_dsml_parser *p = sr->parser;
    agent_tool_visualizer *v = &sr->viz;
    if (!p || !v->param_active || strcmp(v->param_name, "old") != 0)
        return;
    if (!p->current.name || strcmp(p->current.name, "edit") != 0)
        return;

    char err[256] = {0};
    if (agent_preflight_edit_old(sr->renderer->worker, &p->current,
                                 err, sizeof(err)))
        return;

    sr->tool_preflight_error = true;
    snprintf(sr->tool_preflight_error_msg, sizeof(sr->tool_preflight_error_msg),
             "edit old selector failed before new was generated: %s",
             err[0] ? err : "old text is not a unique match");
    agent_trace(sr->renderer->worker, "edit old preflight failed: %s",
                sr->tool_preflight_error_msg);
}

static void agent_stream_feed_dsml_byte(agent_stream_renderer *sr, char c) {
    bool defer_render = sr->dsml_origin_in_think;
    bool was_param = !sr->dsml_ignored && !defer_render && sr->viz.param_active;
    agent_dsml_feed(sr->parser, &c, 1);
    if (!sr->dsml_ignored && !defer_render) {
        agent_stream_tool_events(sr);
        if (was_param) agent_tool_viz_param_value_byte(sr, c);
        if (was_param && sr->parser->state != AGENT_DSML_PARAM_VALUE &&
            sr->viz.param_active)
        {
            agent_stream_preflight_closed_param(sr);
            agent_tool_viz_param_end(sr);
        }
    }
    if (sr->parser->state == AGENT_DSML_DONE) {
        if (sr->dsml_ignored) {
            agent_stream_finish_ignored_dsml(
                sr, "tool calling is not allowed inside <think></think>");
        } else if (!defer_render) {
            agent_trace(sr->renderer->worker, "dsml done calls=%d",
                        sr->parser->calls.len);
            agent_tool_viz_finish(sr, NULL);
            sr->dsml_active = false;
        }
    } else if (sr->parser->state == AGENT_DSML_ERROR) {
        if (sr->dsml_ignored) {
            agent_stream_finish_ignored_dsml(
                sr, "malformed tool call inside <think></think>");
        } else if (defer_render) {
            agent_stream_render_rejected_thinking_block(
                sr, sr->parser->error[0] ? sr->parser->error : "parse error");
            sr->dsml_active = false;
        } else {
            char status[220];
            snprintf(status, sizeof(status), "[invalid tool call: %s]\n",
                     sr->parser->error[0] ? sr->parser->error : "parse error");
            agent_trace(sr->renderer->worker, "dsml error %s",
                        sr->parser->error[0] ? sr->parser->error : "parse error");
            agent_tool_viz_dump_invalid_dsml(sr);
            agent_tool_viz_finish(sr, status);
            sr->dsml_active = false;
        }
    }
}

/* Start a DSML block from the streaming detector.  The detector may accept a
 * known malformed opening form for robustness, but the parser is seeded with
 * canonical bytes so all later parsing remains strict. */
static void agent_stream_start_dsml(agent_stream_renderer *sr, bool in_think) {
    sr->dsml_active = true;
    sr->dsml_ignored = false;
    sr->dsml_origin_in_think = in_think;
    sr->dsml_origin_rendered = false;
    sr->dsml_start_len = 0;
    sr->post_think_gap = false;
    agent_trace(sr->renderer->worker, "dsml start detected%s",
                in_think ? " inside thinking" : "");
    agent_dsml_start(sr->parser);
    if (!in_think) {
        agent_tool_viz_start(sr);
        agent_stream_tool_events(sr);
    }
}

static void agent_stream_note_plain_dsml_byte(agent_stream_renderer *sr, char c);
static void agent_stream_note_thinking_dsml_byte(agent_stream_renderer *sr,
                                                 char c);

static void agent_stream_flush_start_tail(agent_stream_renderer *sr) {
    if (!sr->dsml_start_len) return;
    sr->post_think_gap = false;
    for (size_t i = 0; i < sr->dsml_start_len; i++) {
        renderer_write_char(sr->renderer, sr->dsml_start_tail[i]);
        agent_stream_note_thinking_dsml_byte(sr, sr->dsml_start_tail[i]);
        agent_stream_note_plain_dsml_byte(sr, sr->dsml_start_tail[i]);
        if (sr->parser->state == AGENT_DSML_ERROR) break;
    }
    sr->dsml_start_len = 0;
}

static bool agent_stream_dsml_start_match(const char *tail, size_t len,
                                          bool *complete,
                                          bool *implicit_invoke) {
    static const char canonical[] = "<｜DSML｜tool_calls>";
    static const char missing_bar[] = "<DSML｜tool_calls>";
    static const char invoke[] = "<｜DSML｜invoke";
    static const char invoke_missing_bar[] = "<DSML｜invoke";
    struct {
        const char *text;
        bool implicit_invoke;
    } forms[] = {
        {canonical, false},
        {missing_bar, false},
        {invoke, true},
        {invoke_missing_bar, true},
    };
    *complete = false;
    *implicit_invoke = false;
    for (size_t i = 0; i < sizeof(forms)/sizeof(forms[0]); i++) {
        size_t form_len = strlen(forms[i].text);
        if (len <= form_len && memcmp(forms[i].text, tail, len) == 0) {
            *complete = len == form_len;
            *implicit_invoke = forms[i].implicit_invoke;
            return true;
        }
    }
    return false;
}

static bool agent_tail_matches(const char *tail, size_t len,
                               const char *needle, size_t needle_len) {
    return len >= needle_len &&
           memcmp(tail + len - needle_len, needle, needle_len) == 0;
}

/* Detect DSML-looking control markers in text that is not currently owned by
 * the executable DSML parser.  This helper intentionally has no policy: inside
 * <think> the marker means "tool call attempted too early", while in normal
 * assistant output it means malformed DSML that the model should see as a tool
 * error. */
static bool agent_dsml_marker_detector_feed(agent_dsml_marker_detector *d,
                                            char c) {
    if (d->len == sizeof(d->tail)) {
        memmove(d->tail, d->tail + 1, sizeof(d->tail) - 1);
        d->len--;
    }
    d->tail[d->len++] = c;

    static const char fullwidth_marker[] = "｜DSML｜";
    static const char ascii_marker[] = "|DSML|";
    static const char missing_open[] = "<DSML｜";
    static const char missing_close[] = "</DSML｜";
    return agent_tail_matches(d->tail, d->len,
                              fullwidth_marker, sizeof(fullwidth_marker) - 1) ||
           agent_tail_matches(d->tail, d->len,
                              ascii_marker, sizeof(ascii_marker) - 1) ||
           agent_tail_matches(d->tail, d->len,
                              missing_open, sizeof(missing_open) - 1) ||
           agent_tail_matches(d->tail, d->len,
                              missing_close, sizeof(missing_close) - 1);
}

static void agent_stream_note_thinking_dsml_byte(agent_stream_renderer *sr,
                                                 char c) {
    if (!sr->in_think || sr->dsml_active || sr->dsml_in_think) return;
    if (agent_dsml_marker_detector_feed(&sr->think_dsml, c))
        sr->dsml_in_think = true;
}

static void agent_stream_note_plain_dsml_byte(agent_stream_renderer *sr,
                                              char c) {
    if (sr->parser->state == AGENT_DSML_ERROR) return;
    if (sr->dsml_active || sr->in_think || sr->dsml_in_think) return;
    if (agent_dsml_marker_detector_feed(&sr->plain_dsml, c)) {
        agent_stream_malformed_dsml(
            sr, "DSML markup outside a valid tool_calls block");
    }
}

/* Route ordinary assistant bytes either to normal markdown rendering or into
 * the DSML detector.  The detector must hold short prefixes because the model
 * can split "<｜DSML｜tool_calls>" across arbitrary tokens. */
static void agent_stream_normal_byte(agent_stream_renderer *sr, char c) {
    static const char start[] = "<｜DSML｜tool_calls>";
    static const char canonical_invoke[] = "<｜DSML｜invoke";
    if (sr->parser->state == AGENT_DSML_ERROR) return;

    /* DeepSeek usually emits one or more blank lines after </think> before
     * either prose or a DSML tool stanza.  At that point the bytes are just a
     * visual gap between the hidden thinking phase and the real answer, and
     * printing them makes tool calls appear after odd empty lines.  We only
     * suppress whitespace in this very narrow post-thinking window; once the
     * first non-space byte arrives, normal rendering resumes. */
    if (sr->post_think_gap &&
        (c == ' ' || c == '\t' || c == '\r' || c == '\n'))
    {
        return;
    }

    if (sr->dsml_start_len || c == start[0]) {
        if (sr->dsml_start_len < sizeof(sr->dsml_start_tail))
            sr->dsml_start_tail[sr->dsml_start_len++] = c;
        bool complete = false, implicit_invoke = false;
        if (agent_stream_dsml_start_match(sr->dsml_start_tail, sr->dsml_start_len,
                                          &complete, &implicit_invoke))
        {
            if (complete) {
                /* Accept the common missing-leading-bar typo
                 * "<DSML｜tool_calls>" here, but seed the parser with the
                 * canonical marker so the rest of the DSML parser stays
                 * strict and simple.  Also accept a direct invoke opener as an
                 * implicit tool_calls block; the model often knows it wants a
                 * tool but forgets the outer wrapper. */
                agent_stream_start_dsml(sr, sr->in_think);
                if (implicit_invoke) {
                    for (size_t i = 0; i < sizeof(canonical_invoke) - 1; i++)
                        agent_stream_feed_dsml_byte(sr, canonical_invoke[i]);
                }
            }
            return;
        }
        if (sr->dsml_start_len > 1 &&
            sr->dsml_start_tail[sr->dsml_start_len - 1] == start[0])
        {
            sr->post_think_gap = false;
            size_t flush = sr->dsml_start_len - 1;
            for (size_t i = 0; i < flush; i++) {
                renderer_write_char(sr->renderer, sr->dsml_start_tail[i]);
                agent_stream_note_plain_dsml_byte(sr, sr->dsml_start_tail[i]);
                if (sr->parser->state == AGENT_DSML_ERROR) break;
            }
            if (sr->parser->state == AGENT_DSML_ERROR) {
                sr->dsml_start_len = 0;
                return;
            }
            sr->dsml_start_tail[0] = start[0];
            sr->dsml_start_len = 1;
            return;
        }
        agent_stream_flush_start_tail(sr);
        return;
    }

    sr->post_think_gap = false;
    renderer_write_char(sr->renderer, c);
    agent_stream_note_thinking_dsml_byte(sr, c);
    agent_stream_note_plain_dsml_byte(sr, c);
}

/* This is the single streaming display state machine for assistant output.  It
 * hides raw DSML as soon as the tool_calls marker is complete, lets the DSML
 * parser continue building executable calls, and paints semantic tool output
 * from parser state changes.  The sampled transcript remains unchanged: only
 * the terminal projection is rewritten. */
static void agent_stream_text(agent_stream_renderer *sr, const char *text, size_t len, bool finish) {
    const char *think_open = "<think>";
    const char *think_close = "</think>";
    size_t total = sr->pending_len + len;
    char *buf = xmalloc(total ? total : 1);
    if (sr->pending_len) memcpy(buf, sr->pending, sr->pending_len);
    if (len) memcpy(buf + sr->pending_len, text, len);
    sr->pending_len = 0;

    /* The UI may reset terminal attributes while redrawing the editable prompt
     * between generated chunks.  If a DSML parameter is still streaming, make
     * each new token fragment self-contained by restoring the active parameter
     * color before visible bytes are projected.  This keeps the prompt normal
     * without sacrificing long write/edit content coloring. */
    if (len) agent_tool_viz_restore_param_color(sr);
    if (len && !sr->dsml_active) renderer_restore_text_attrs(sr->renderer);

    size_t i = 0;
    while (i < total) {
        char *cur = buf + i;
        size_t rem = total - i;
        if (!sr->dsml_active && bytes_has_prefix(cur, rem, think_open)) {
            agent_stream_flush_start_tail(sr);
            sr->post_think_gap = false;
            sr->in_think = true;
            sr->renderer->in_think = true;
            i += strlen(think_open);
            continue;
        }
        if (!sr->dsml_active && bytes_has_prefix(cur, rem, think_close)) {
            agent_stream_flush_start_tail(sr);
            sr->in_think = false;
            sr->renderer->in_think = false;
            renderer_reset_color(sr->renderer);
            if (!sr->renderer->last_output_newline)
                renderer_write(sr->renderer, "\n", 1);
            renderer_write(sr->renderer, "\n", 1);
            sr->renderer->last_output_newline = true;
            sr->post_think_gap = true;
            i += strlen(think_close);
            continue;
        }
        if (!finish && !sr->dsml_active && cur[0] == '<' &&
            (bytes_is_partial_prefix(cur, rem, think_open) ||
             bytes_is_partial_prefix(cur, rem, think_close)))
        {
            if (rem < sizeof(sr->pending)) {
                memcpy(sr->pending, cur, rem);
                sr->pending_len = rem;
            }
            break;
        }

        if (sr->dsml_active) {
            agent_stream_feed_dsml_byte(sr, cur[0]);
        } else if (sr->in_think) {
            /* Tool calls are executable only after thinking has closed.  Still
             * route thinking bytes through the DSML start detector so an
             * accidental in-think tool stanza can be suppressed cleanly instead
             * of being shown as raw markup or, worse, executed. */
            agent_stream_normal_byte(sr, cur[0]);
        } else {
            agent_stream_normal_byte(sr, cur[0]);
        }
        i++;
    }
    free(buf);

    if (finish) {
        agent_stream_flush_start_tail(sr);
        sr->post_think_gap = false;
        if (sr->dsml_active) {
            if (sr->dsml_origin_in_think) {
                char err[192];
                if (agent_thinking_tool_block_validate(sr->parser,
                                                       err, sizeof(err)))
                    agent_stream_render_validated_thinking_block(sr);
                else
                    agent_stream_render_rejected_thinking_block(sr, err);
                sr->dsml_active = false;
            } else if (sr->dsml_ignored) {
                agent_stream_finish_ignored_dsml(
                    sr, "tool calling is not allowed inside <think></think>");
            } else {
                agent_tool_viz_finish(sr, sr->tool_preflight_error ?
                                      "[tool call stopped: edit old selector failed]\n" :
                                      "[tool call interrupted]\n");
                sr->dsml_active = false;
            }
        }
        if (sr->dsml_in_think && !sr->dsml_in_think_reported) {
            agent_stream_finish_ignored_dsml(
                sr, "malformed DSML tool call inside <think></think>");
        }
    }
}

/* ============================================================================
 * Worker Progress And Generic Buffers
 * ============================================================================
 */

static void worker_progress_cb(void *ud, const char *event, int current, int total) {
    (void)total;
    agent_worker *w = ud;
    if (!w || !event) return;
    if (strcmp(event, "prefill_chunk") && strcmp(event, "prefill_display")) return;
    worker_apply_pending_power(w);
    pthread_mutex_lock(&w->mu);
    int done = current - w->progress_base;
    if (done < 0) done = 0;
    if (done > w->status.prefill_total) done = w->status.prefill_total;
    w->status.prefill_done = done;
    double elapsed = now_sec() - w->progress_started_at;
    w->status.prefill_tps =
        done > 0 && elapsed > 0.0 ? (double)done / elapsed : 0.0;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static bool worker_should_interrupt(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool interrupt = w->interrupt || w->stop;
    pthread_mutex_unlock(&w->mu);
    return interrupt;
}

/* Ctrl+C is a latched request consumed by the worker.  Once an interrupted
 * operation has reached a stable append-only boundary and is about to publish
 * IDLE, the request must be acknowledged; otherwise the editor can observe an
 * idle worker with a stale interrupt still pending. */
static void worker_clear_interrupt(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->interrupt = false;
    pthread_mutex_unlock(&w->mu);
}

static bool agent_err_is_interrupted(const char *err) {
    return err && !strcmp(err, "interrupted");
}

static bool worker_cancel_session_cb(void *ud) {
    return worker_should_interrupt(ud);
}

static void agent_buf_append(agent_buf *b, const char *s, size_t n) {
    if (!n || b->truncated) return;
    const size_t max = 128 * 1024;
    if (b->len + n > max) {
        n = max > b->len ? max - b->len : 0;
        b->truncated = true;
    }
    if (!n) return;
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 4096;
        while (cap < b->len + n + 1) cap *= 2;
        b->ptr = xrealloc(b->ptr, cap);
        b->cap = cap;
    }
    memcpy(b->ptr + b->len, s, n);
    b->len += n;
    b->ptr[b->len] = '\0';
}

void agent_buf_append_full(agent_buf *b, const char *s, size_t n) {
    if (!n || b->truncated) return;
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 4096;
        while (cap < b->len + n + 1) cap *= 2;
        b->ptr = xrealloc(b->ptr, cap);
        b->cap = cap;
    }
    memcpy(b->ptr + b->len, s, n);
    b->len += n;
    b->ptr[b->len] = '\0';
}

void agent_buf_puts(agent_buf *b, const char *s) {
    agent_buf_append(b, s, strlen(s));
}

char *agent_buf_take(agent_buf *b) {
    if (!b->ptr) return xstrdup("");
    char *p = b->ptr;
    memset(b, 0, sizeof(*b));
    return p;
}

static bool agent_tokens_equal(const ds4_tokens *a, const ds4_tokens *b) {
    if (!a || !b || a->len != b->len) return false;
    for (int i = 0; i < a->len; i++) {
        if (a->v[i] != b->v[i]) return false;
    }
    return true;
}

static bool agent_mkdir_p(const char *path) {
    if (!path || !path[0]) return false;
    char *tmp = xstrdup(path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(tmp, 0700) != 0 && errno != EEXIST) {
            free(tmp);
            return false;
        }
        *p = '/';
    }
    bool ok = mkdir(tmp, 0700) == 0 || errno == EEXIST;
    free(tmp);
    return ok;
}

static char *agent_default_cache_dir(void) {
    const char *home = getenv("HOME");
    if (!home || !home[0]) home = ".";
    agent_buf b = {0};
    agent_buf_puts(&b, home);
    if (b.len == 0 || b.ptr[b.len - 1] != '/') agent_buf_puts(&b, "/");
    agent_buf_puts(&b, ".ds4/kvcache");
    return agent_buf_take(&b);
}

static char *agent_kv_path_for_sha(const char *dir, const char sha[41]) {
    char name[44];
    memcpy(name, sha, 40);
    memcpy(name + 40, ".kv", 4);
    return ds4_kvstore_path_join(dir, name);
}

static void agent_le_put64(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}

/* Agent session IDs are intentionally independent from the rendered transcript:
 * once a session has a title and creation time, resaving it keeps the same file
 * name while the transcript and KV payload evolve. */
static void agent_session_identity_sha(const char *title, uint64_t created_at,
                                       char sha_out[41]) {
    size_t title_len = title ? strlen(title) : 0;
    agent_buf b = {0};
    agent_buf_append(&b, title ? title : "", title_len);
    uint8_t ts[8];
    agent_le_put64(ts, created_at);
    agent_buf_append(&b, (const char *)ts, sizeof(ts));
    ds4_kvstore_sha1_bytes_hex(b.ptr ? b.ptr : "", b.len, sha_out);
    free(b.ptr);
}

static void agent_worker_clear_session_identity(agent_worker *w) {
    w->session_sha[0] = '\0';
    free(w->session_title);
    w->session_title = NULL;
    w->session_created_at = 0;
    free(w->legacy_session_path_to_delete);
    w->legacy_session_path_to_delete = NULL;
}

typedef struct {
    bool has_title_trailer;
    bool legacy_identity;
    char *title;
    uint64_t created_at;
    char sha[41];
} agent_kv_session_meta;

static void agent_kv_session_meta_free(agent_kv_session_meta *m) {
    free(m->title);
    memset(m, 0, sizeof(*m));
}

/* ============================================================================
 * Agent KV Store And Session Persistence
 * ============================================================================
 */

static char *agent_session_title_from_text(const char *text, size_t text_len,
                                           size_t max_bytes);

/* Agent sessions deliberately use a different policy from ds4-server:
 *
 * - sysprompt.kv is a fixed bootstrap checkpoint for the current tool/system
 *   prompt.  Because its name is fixed, the current rendered text is compared
 *   with the text stored in the file before loading.  A mismatch simply rebuilds
 *   and overwrites the file.
 * - conversation sessions are explicit saves only.  Their stable file name is
 *   SHA1(title || created_at_le64).kv, where title is the first user prompt and
 *   created_at is preserved across future saves.  The title is stored in an
 *   agent-only trailer after the KV payload.
 *
 * The DS4 payload stores the exact token sequence and graph state.  The rendered
 * text is retained for listing, history rendering, and stripped-session rebuilds. */
static bool agent_kv_read_text(FILE *fp, uint32_t text_bytes,
                               char **text_out, char *err, size_t err_len) {
    char *text = xmalloc((size_t)text_bytes + 1);
    if (fread(text, 1, text_bytes, fp) != text_bytes) {
        if (err && err_len) snprintf(err, err_len, "truncated cached text");
        free(text);
        return false;
    }
    text[text_bytes] = '\0';
    *text_out = text;
    return true;
}

static bool agent_kv_write_title_trailer(FILE *fp, const char *title,
                                         char *err, size_t err_len) {
    size_t title_len = title ? strlen(title) : 0;
    if (title_len > UINT32_MAX) {
        snprintf(err, err_len, "agent session title is too large");
        return false;
    }
    uint8_t tb[4];
    ds4_kvstore_le_put32(tb, (uint32_t)title_len);
    return fwrite(tb, 1, sizeof(tb), fp) == sizeof(tb) &&
           fwrite(title ? title : "", 1, title_len, fp) == title_len;
}

/* Read the optional agent title trailer without disturbing the payload cursor.
 * The caller is positioned just after rendered text, which is also the payload
 * start expected by ds4_session_load_payload(). */
static bool agent_kv_read_title_trailer(FILE *fp, const ds4_kvstore_entry *hdr,
                                        char **title_out,
                                        char *err, size_t err_len) {
    off_t payload_pos = ftello(fp);
    if (payload_pos < 0) {
        if (err && err_len) snprintf(err, err_len, "%s", strerror(errno));
        return false;
    }
    if (hdr->payload_bytes > (uint64_t)LLONG_MAX ||
        fseeko(fp, (off_t)hdr->payload_bytes, SEEK_CUR) != 0)
    {
        if (err && err_len) snprintf(err, err_len, "%s", strerror(errno));
        return false;
    }

    uint8_t tb[4];
    if (fread(tb, 1, sizeof(tb), fp) != sizeof(tb)) {
        if (err && err_len) snprintf(err, err_len, "missing agent session title trailer");
        fseeko(fp, payload_pos, SEEK_SET);
        return false;
    }
    uint32_t title_bytes = ds4_kvstore_le_get32(tb);
    char *title = xmalloc((size_t)title_bytes + 1);
    if (fread(title, 1, title_bytes, fp) != title_bytes) {
        if (err && err_len) snprintf(err, err_len, "truncated agent session title trailer");
        free(title);
        fseeko(fp, payload_pos, SEEK_SET);
        return false;
    }
    title[title_bytes] = '\0';
    if (fseeko(fp, payload_pos, SEEK_SET) != 0) {
        if (err && err_len) snprintf(err, err_len, "%s", strerror(errno));
        free(title);
        return false;
    }
    *title_out = title;
    return true;
}

static void agent_kv_identity_sha(const ds4_kvstore_entry *hdr,
                                  const char *text, uint32_t text_bytes,
                                  const char *title,
                                  char sha_out[41]) {
    if (hdr->ext_flags & DS4_KVSTORE_EXT_SESSION_TITLE) {
        agent_session_identity_sha(title ? title : "", hdr->created_at, sha_out);
    } else {
        ds4_kvstore_sha1_bytes_hex(text, text_bytes, sha_out);
    }
}

/* Load a KV file and optionally verify either its session identity or exact
 * rendered text.  sysprompt.kv uses exact text because the file name is fixed;
 * saved sessions use their filename SHA: modern agent sessions hash the title
 * trailer plus created_at, while legacy sessions still hash rendered text. */
static bool agent_kv_load_path(agent_worker *w, const char *path,
                               const char *expected_sha,
                               const char *expected_text,
                               size_t expected_text_len,
                               ds4_tokens *loaded_tokens,
                               agent_kv_session_meta *meta_out,
                               char *err, size_t err_len) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        snprintf(err, err_len, "%s", strerror(errno));
        return false;
    }

    ds4_kvstore_entry hdr = {0};
    uint32_t text_bytes = 0;
    bool ok = ds4_kvstore_read_header(fp, &hdr, &text_bytes);
    if (!ok) snprintf(err, err_len, "invalid KV header");

    char *text = NULL;
    if (ok) ok = agent_kv_read_text(fp, text_bytes, &text, err, err_len);
    char *title = NULL;
    bool has_title = ok && (hdr.ext_flags & DS4_KVSTORE_EXT_SESSION_TITLE);
    if (has_title)
        ok = agent_kv_read_title_trailer(fp, &hdr, &title, err, err_len);
    uint32_t expected_tokens = hdr.tokens;
    if (ok && hdr.payload_bytes != 0 &&
        hdr.model_id != (uint8_t)ds4_engine_model_id(w->engine))
    {
        snprintf(err, err_len, "KV checkpoint was written for a different model");
        ok = false;
    }
    if (ok && hdr.payload_bytes != 0 &&
        hdr.quant_bits != (uint8_t)ds4_engine_routed_quant_bits(w->engine))
    {
        snprintf(err, err_len, "KV checkpoint was written for a different quantization");
        ok = false;
    }
    if (ok && expected_text) {
        if ((size_t)text_bytes != expected_text_len ||
            memcmp(text, expected_text, expected_text_len) != 0)
        {
            snprintf(err, err_len, "cached text does not match current system prompt");
            ok = false;
        }
    }
    if (ok && expected_sha) {
        char actual_sha[41];
        agent_kv_identity_sha(&hdr, text, text_bytes, title, actual_sha);
        if (strcmp(actual_sha, expected_sha)) {
            snprintf(err, err_len, "cached session identity does not match file name");
            ok = false;
        }
    }

    char load_err[160] = {0};
    if (ok && hdr.payload_bytes == 0) {
        ds4_tokens rebuilt = {0};
        ds4_tokenize_rendered_chat(w->engine, text, &rebuilt);
        expected_tokens = (uint32_t)rebuilt.len;
        if (agent_worker_sync_tokens(w, &rebuilt, true, err, err_len) != 0) {
            ds4_session_invalidate(w->session);
            ok = false;
        }
        ds4_tokens_free(&rebuilt);
    } else if (ok &&
               ds4_session_load_payload(w->session, fp, hdr.payload_bytes,
                                        load_err, sizeof(load_err)) != 0)
    {
        snprintf(err, err_len, "%s", load_err[0] ? load_err : "failed to load KV payload");
        ds4_session_invalidate(w->session);
        ok = false;
    }
    fclose(fp);

    if (ok) {
        const ds4_tokens *live = ds4_session_tokens(w->session);
        if (!live || live->len != (int)expected_tokens) {
            snprintf(err, err_len, "KV payload token count mismatch");
            ds4_session_invalidate(w->session);
            ok = false;
        } else if (loaded_tokens) {
            ds4_tokens_free(loaded_tokens);
            ds4_tokens_copy(loaded_tokens, live);
        }
        if (meta_out) {
            agent_kv_session_meta_free(meta_out);
            meta_out->has_title_trailer = has_title;
            meta_out->legacy_identity = !has_title;
            meta_out->created_at = hdr.created_at;
            agent_kv_identity_sha(&hdr, text, text_bytes, title, meta_out->sha);
            meta_out->title = has_title ?
                xstrdup(title) :
                agent_session_title_from_text(text, text_bytes, 0);
        }
    }
    free(title);
    free(text);
    return ok;
}

/* Save the current live KV under the rendered transcript identity.  The caller
 * decides the policy: fixed sysprompt path or SHA-named session path. */
static bool agent_kv_save_path(agent_worker *w, const char *path,
                               const ds4_tokens *tokens,
                               const char *reason,
                               char sha_out[41],
                               const char *session_title,
                               uint64_t session_created_at,
                               char *err, size_t err_len) {
    const ds4_tokens *live = ds4_session_tokens(w->session);
    if (!agent_tokens_equal(live, tokens)) {
        snprintf(err, err_len, "live KV state does not match session transcript");
        return false;
    }
    const int quant_bits = ds4_engine_routed_quant_bits(w->engine);
    if (quant_bits != 2 && quant_bits != 4) {
        snprintf(err, err_len, "unsupported routed quantization for KV save");
        return false;
    }
    const int model_id = ds4_engine_model_id(w->engine);

    size_t text_len = 0;
    char *text = ds4_kvstore_render_tokens_text(w->engine, tokens, &text_len);
    if (!text) {
        snprintf(err, err_len, "failed to render KV text key");
        return false;
    }
    if (text_len > UINT32_MAX) {
        snprintf(err, err_len, "rendered KV text key is too large");
        free(text);
        return false;
    }
    const bool session_identity = session_title != NULL;
    uint64_t now = (uint64_t)time(NULL);
    uint64_t created_at = session_identity && session_created_at ?
        session_created_at : now;
    char sha[41];
    if (session_identity)
        agent_session_identity_sha(session_title, created_at, sha);
    else
        ds4_kvstore_sha1_bytes_hex(text, text_len, sha);
    if (sha_out) memcpy(sha_out, sha, sizeof(sha));

    ds4_session_payload_file staged = {0};
    char save_err[160] = {0};
    if (ds4_session_stage_payload(w->session, &staged,
                                  save_err, sizeof(save_err)) != 0) {
        snprintf(err, err_len, "%s",
                 save_err[0] ? save_err : "session has no valid KV payload");
        free(text);
        return false;
    }
    uint64_t payload_bytes = staged.bytes;

    agent_buf tmpl = {0};
    agent_buf_puts(&tmpl, path);
    agent_buf_puts(&tmpl, ".tmp.XXXXXX");
    char *tmp = agent_buf_take(&tmpl);
    int fd = mkstemp(tmp);
    if (fd < 0) {
        snprintf(err, err_len, "%s", strerror(errno));
        ds4_session_payload_file_free(&staged);
        free(tmp);
        free(text);
        return false;
    }

    FILE *fp = fdopen(fd, "wb");
    if (!fp) {
        snprintf(err, err_len, "%s", strerror(errno));
        close(fd);
        unlink(tmp);
        ds4_session_payload_file_free(&staged);
        free(tmp);
        free(text);
        return false;
    }

    uint8_t h[DS4_KVSTORE_FIXED_HEADER];
    ds4_kvstore_fill_header(h, (uint8_t)model_id, (uint8_t)quant_bits,
                            ds4_kvstore_reason_code(reason),
                            session_identity ? DS4_KVSTORE_EXT_SESSION_TITLE : 0,
                            (uint32_t)tokens->len, 0,
                            (uint32_t)ds4_session_ctx(w->session),
                            created_at, now, payload_bytes);
    uint8_t tb[4];
    ds4_kvstore_le_put32(tb, (uint32_t)text_len);

    errno = 0;
    bool ok = fwrite(h, 1, sizeof(h), fp) == sizeof(h) &&
              fwrite(tb, 1, sizeof(tb), fp) == sizeof(tb) &&
              fwrite(text, 1, text_len, fp) == text_len &&
              ds4_session_write_staged_payload(&staged, fp,
                                               save_err, sizeof(save_err)) == 0 &&
              (!session_identity ||
               agent_kv_write_title_trailer(fp, session_title,
                                            save_err, sizeof(save_err))) &&
              fflush(fp) == 0;
    int saved_errno = errno;
    if (fclose(fp) != 0) {
        if (!saved_errno) saved_errno = errno;
        ok = false;
    }
    if (ok && rename(tmp, path) != 0) {
        saved_errno = errno;
        ok = false;
    }
    if (!ok) {
        snprintf(err, err_len, "%s",
                 saved_errno ? strerror(saved_errno) :
                 (save_err[0] ? save_err : "failed to write KV file"));
        unlink(tmp);
    }

    ds4_session_payload_file_free(&staged);
    free(tmp);
    free(text);
    return ok;
}

static void agent_worker_build_system_tokens(agent_worker *w, ds4_tokens *out) {
    ds4_chat_begin(w->engine, out);
    if (w->cfg->gen.think_mode == DS4_THINK_MAX &&
        effective_think_mode(w->cfg) == DS4_THINK_MAX)
        ds4_chat_append_max_effort_prefix(w->engine, out);
    agent_append_system_prompt(w, w->engine, out, w->cfg->gen.system);
    const agent_path_list *roots = agent_working_directories(w);
    agent_append_project_instruction_prompt(w->engine, out,
                                            roots && roots->len ? roots->v[0] : NULL);
    if (roots) {
        char msg[PATH_MAX + 192];
        snprintf(msg, sizeof(msg),
                 "Configured ds4-agent working directory: %s. "
                 "Local file tools check all configured working directories; ",
                 roots->v[0]);
        ds4_chat_append_message(w->engine, out, "system", msg);
    }
}

static void agent_publish_system_status(agent_worker *w, const char *msg) {
    if (w->cfg->non_interactive) return;
    if (isatty(STDOUT_FILENO)) {
        static const char marker[] = "\x1b[33m✦ \x1b[38;5;218m";
        agent_publish(w, marker, sizeof(marker) - 1);
        agent_publish(w, msg, strlen(msg));
        agent_publish(w, "\x1b[0m\n", strlen("\x1b[0m\n"));
    } else {
        agent_publish(w, "✦ ", strlen("✦ "));
        agent_publish(w, msg, strlen(msg));
        agent_publish(w, "\n", 1);
    }
}

static void agent_publishf_system_status(agent_worker *w, const char *fmt, ...) {
    char stack[1024];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(stack, sizeof(stack), fmt, ap);
    va_end(ap);
    if (n <= 0) return;
    if ((size_t)n < sizeof(stack)) {
        agent_publish_system_status(w, stack);
        return;
    }

    char *heap = xmalloc((size_t)n + 1);
    va_start(ap, fmt);
    vsnprintf(heap, (size_t)n + 1, fmt, ap);
    va_end(ap);
    agent_publish_system_status(w, heap);
    free(heap);
}

static int agent_web_confirm(void *privdata, const char *message,
                             char *err, size_t err_len) {
    agent_worker *w = privdata;
    if (!w || w->cfg->non_interactive) {
        snprintf(err, err_len,
                 "visible Chrome browser startup requires interactive approval");
        return 0;
    }

    pthread_mutex_lock(&w->mu);
    w->web_approval_pending = true;
    w->web_approval_answered = false;
    w->web_approval_result = false;
    w->web_approval_error[0] = '\0';
    snprintf(w->web_approval_message, sizeof(w->web_approval_message),
             "%s", message ? message : "Start visible Chrome browser? (y/n) ");
    agent_wake_locked(w);
    while (!w->stop && !w->interrupt && !w->web_approval_answered)
        pthread_cond_wait(&w->cond, &w->mu);
    bool ok = w->web_approval_result;
    if (!w->web_approval_answered && (w->stop || w->interrupt)) {
        ok = false;
        w->web_approval_pending = false;
        snprintf(w->web_approval_error, sizeof(w->web_approval_error),
                 "interrupted");
    }
    if (!ok) {
        snprintf(err, err_len, "%s",
                 w->web_approval_error[0] ? w->web_approval_error :
                 "user denied Chrome browser start");
    }
    pthread_mutex_unlock(&w->mu);
    return ok ? 1 : 0;
}

static void agent_web_log(void *privdata, const char *message) {
    agent_worker *w = privdata;
    if (!w || !message || !message[0]) return;
    agent_trace(w, "web: %s", message);
}

static bool agent_web_cancel(void *privdata) {
    return worker_should_interrupt(privdata);
}

bool worker_take_web_approval_request(agent_worker *w,
                                      char *message, size_t message_len) {
    pthread_mutex_lock(&w->mu);
    bool pending = w->web_approval_pending;
    if (pending) {
        snprintf(message, message_len, "%s", w->web_approval_message);
        w->web_approval_pending = false;
    }
    pthread_mutex_unlock(&w->mu);
    return pending;
}

void worker_answer_web_approval(agent_worker *w, bool allow,
                                const char *deny_error) {
    pthread_mutex_lock(&w->mu);
    w->web_approval_result = allow;
    w->web_approval_answered = true;
    if (!allow)
        snprintf(w->web_approval_error, sizeof(w->web_approval_error),
                 "%s", deny_error && deny_error[0] ? deny_error :
                 "user denied Chrome browser start");
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

bool worker_take_path_approval_request(agent_worker *w,
                                       char *message, size_t message_len,
                                       char options[3][PATH_MAX],
                                       int *option_count) {
    pthread_mutex_lock(&w->mu);
    bool pending = w->path_approval_pending;
    if (pending) {
        snprintf(message, message_len, "%s", w->path_approval_message);
        if (option_count) *option_count = w->path_approval_option_count;
        for (int i = 0; i < w->path_approval_option_count && i < 3; i++)
            snprintf(options[i], PATH_MAX, "%s", w->path_approval_options[i]);
        w->path_approval_pending = false;
    } else if (option_count) {
        *option_count = 0;
    }
    pthread_mutex_unlock(&w->mu);
    return pending;
}

void worker_answer_path_approval(agent_worker *w, bool allow,
                                 const char *choice,
                                 const char *deny_error) {
    pthread_mutex_lock(&w->mu);
    w->path_approval_result = allow;
    w->path_approval_answered = true;
    if (allow)
        snprintf(w->path_approval_choice, sizeof(w->path_approval_choice),
                 "%s", choice ? choice : "");
    if (!allow)
        snprintf(w->path_approval_error, sizeof(w->path_approval_error),
                 "%s", deny_error && deny_error[0] ? deny_error :
                 "user denied working directory add");
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static char *worker_request_question(agent_worker *w,
                                     const char *question,
                                     char **choices,
                                     int choice_count) {
    if (!w || !w->cfg || w->cfg->non_interactive)
        return xstrdup("Tool error: ask_question requires an interactive user\n");
    if (!question || !question[0])
        return xstrdup("Tool error: ask_question requires question\n");

    pthread_mutex_lock(&w->mu);
    w->question_pending = true;
    w->question_answered = false;
    w->question_interrupted = false;
    free(w->question_answer);
    w->question_answer = NULL;
    w->question_error[0] = '\0';
    snprintf(w->question_message, sizeof(w->question_message), "%s", question);
    w->question_choice_count = 0;
    for (int i = 0; i < choice_count && i < AGENT_ASK_QUESTION_MAX_CHOICES; i++) {
        if (!choices[i] || !choices[i][0]) continue;
        snprintf(w->question_choices[w->question_choice_count],
                 AGENT_ASK_QUESTION_CHOICE_MAX, "%s", choices[i]);
        w->question_choice_count++;
    }
    agent_wake_locked(w);
    pthread_cond_signal(&w->cond);
    while (!w->stop && !w->interrupt && !w->question_answered)
        pthread_cond_wait(&w->cond, &w->mu);

    bool interrupted = w->question_interrupted;
    char *answer = w->question_answer ? xstrdup(w->question_answer) : NULL;
    char err[160];
    snprintf(err, sizeof(err), "%s", w->question_error);
    if (!w->question_answered && (w->stop || w->interrupt)) {
        interrupted = true;
        w->question_pending = false;
        snprintf(err, sizeof(err), "interrupted");
    }
    w->question_answered = false;
    free(w->question_answer);
    w->question_answer = NULL;
    pthread_mutex_unlock(&w->mu);

    agent_buf result = {0};
    if (interrupted) {
        agent_buf_puts(&result, "Question interrupted by user.\n");
    } else if (answer && answer[0]) {
        agent_buf_puts(&result, "Answer: ");
        agent_buf_puts(&result, answer);
        agent_buf_puts(&result, "\n");
    } else {
        agent_buf_puts(&result, "Tool error: ");
        agent_buf_puts(&result, err[0] ? err : "ask_question received no answer");
        agent_buf_puts(&result, "\n");
    }
    free(answer);
    return agent_buf_take(&result);
}

bool worker_take_question_request(agent_worker *w,
                                  char *message,
                                  size_t message_len,
                                  char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                  int *choice_count) {
    pthread_mutex_lock(&w->mu);
    bool pending = w->question_pending;
    if (pending) {
        snprintf(message, message_len, "%s", w->question_message);
        if (choice_count) *choice_count = w->question_choice_count;
        for (int i = 0; i < w->question_choice_count &&
                        i < AGENT_ASK_QUESTION_MAX_CHOICES; i++)
            snprintf(choices[i], AGENT_ASK_QUESTION_CHOICE_MAX, "%s",
                     w->question_choices[i]);
        w->question_pending = false;
    } else if (choice_count) {
        *choice_count = 0;
    }
    pthread_mutex_unlock(&w->mu);
    return pending;
}

void worker_answer_question(agent_worker *w,
                            bool interrupted,
                            const char *answer,
                            const char *err) {
    pthread_mutex_lock(&w->mu);
    w->question_interrupted = interrupted;
    w->question_answered = true;
    free(w->question_answer);
    w->question_answer = answer ? xstrdup(answer) : NULL;
    snprintf(w->question_error, sizeof(w->question_error), "%s",
             err && err[0] ? err : "");
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

/* When a model turn finishes with a tool call, queued user messages should not
 * preempt that tool.  The worker asks the UI thread for the queue contents only
 * after the tool result is appended, so the next model input can contain both
 * the tool observation and the user's pending correction. */
static char *worker_request_queued_user_drain(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->queued_user_drain_pending = true;
    w->queued_user_drain_answered = false;
    free(w->queued_user_drain_text);
    w->queued_user_drain_text = NULL;
    agent_wake_locked(w);
    pthread_cond_signal(&w->cond);
    while (!w->stop && !w->queued_user_drain_answered)
        pthread_cond_wait(&w->cond, &w->mu);
    char *text = w->queued_user_drain_text;
    w->queued_user_drain_text = NULL;
    w->queued_user_drain_pending = false;
    w->queued_user_drain_answered = false;
    pthread_mutex_unlock(&w->mu);
    return text;
}

bool worker_take_queued_user_drain_request(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool pending = w->queued_user_drain_pending;
    if (pending) w->queued_user_drain_pending = false;
    pthread_mutex_unlock(&w->mu);
    return pending;
}

void worker_answer_queued_user_drain(agent_worker *w, char *text) {
    pthread_mutex_lock(&w->mu);
    free(w->queued_user_drain_text);
    w->queued_user_drain_text = text;
    w->queued_user_drain_answered = true;
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void agent_worker_model_gate_lock(agent_worker *w,
                                         agent_worker_state resume_state) {
    if (!w || !w->model_gate) return;
    pthread_mutex_lock(&w->mu);
    w->status.state = AGENT_WORKER_WAITING_MODEL;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);

    pthread_mutex_lock(w->model_gate);

    /* Publish this worker's session id as the engine owner while the gate
     * is held.  The manager samples this id for footer snapshots.
     * The model_gate mutex provides full ordering; a volatile store
     * suffices for the single-word write. */
    if (w->engine_owner_ptr)
        *w->engine_owner_ptr = w->session_slot_id;

    pthread_mutex_lock(&w->mu);
    if (w->status.state == AGENT_WORKER_WAITING_MODEL)
        w->status.state = resume_state;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void agent_worker_model_gate_unlock(agent_worker *w) {
    if (!w || !w->model_gate) return;
    /* Clear engine owner before releasing the gate so the next gate
     * acquirer publishes its own id.  A brief window with no owner is
     * allowed; the next footer snapshot will observe the new owner. */
    if (w->engine_owner_ptr)
        *w->engine_owner_ptr = 0;
    pthread_mutex_unlock(w->model_gate);
}

static int agent_worker_session_sync(agent_worker *w,
                                     const ds4_tokens *tokens,
                                     agent_worker_state resume_state,
                                     char *err,
                                     size_t err_len) {
    agent_worker_model_gate_lock(w, resume_state);
    int rc = ds4_session_sync(w->session, tokens, err, err_len);
    agent_worker_model_gate_unlock(w);
    return rc;
}

static int agent_worker_session_eval(agent_worker *w,
                                     int token,
                                     agent_worker_state resume_state,
                                     char *err,
                                     size_t err_len) {
    agent_worker_model_gate_lock(w, resume_state);
    int rc = ds4_session_eval(w->session, token, err, err_len);
    agent_worker_model_gate_unlock(w);
    return rc;
}

static int agent_worker_session_set_power(agent_worker *w, int power) {
    agent_worker_model_gate_lock(w, AGENT_WORKER_IDLE);
    int rc = ds4_session_set_power(w->session, power);
    agent_worker_model_gate_unlock(w);
    return rc;
}

/* Synchronize the live DS4 session to a transcript.  This is the agent's main
 * cache-saving operation: if the requested transcript extends the live session,
 * only the suffix is prefetched; otherwise the DS4 session rebuilds from the
 * longest common prefix it can retain. */
static int agent_worker_sync_tokens(agent_worker *w, const ds4_tokens *tokens,
                                    bool publish_progress,
                                    char *err, size_t err_len) {
    int old_pos = ds4_session_pos(w->session);
    int common = ds4_session_common_prefix(w->session, tokens);
    int cached = common == old_pos && tokens->len >= old_pos ? common : 0;
    int suffix = tokens->len - cached;
    if (suffix < 0) suffix = tokens->len;

    if (publish_progress) {
        pthread_mutex_lock(&w->mu);
        unsigned prefill_label = w->status.state == AGENT_WORKER_PREFILL ?
            w->status.prefill_label : agent_next_prefill_label();
        w->status.state = AGENT_WORKER_PREFILL;
        w->progress_base = cached;
        w->progress_started_at = now_sec();
        w->status.prefill_done = 0;
        w->status.prefill_total = suffix;
        w->status.prefill_label = prefill_label;
        w->status.prefill_tps = 0.0;
        w->status.generated = 0;
        w->status.gen_tps = 0.0;
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);
    }

    ds4_session_set_progress(w->session, publish_progress ? worker_progress_cb : NULL,
                             publish_progress ? w : NULL);
    ds4_session_set_display_progress(w->session,
                                     publish_progress ? worker_progress_cb : NULL,
                                     publish_progress ? w : NULL);
    ds4_session_set_cancel(w->session, worker_cancel_session_cb, w);
    int rc = agent_worker_session_sync(w, tokens, AGENT_WORKER_PREFILL,
                                       err, err_len);
    ds4_session_set_cancel(w->session, NULL, NULL);
    ds4_session_set_progress(w->session, NULL, NULL);
    ds4_session_set_display_progress(w->session, NULL, NULL);
    return rc;
}

/* Start a new session at the system/tool prompt.  A fixed sysprompt.kv
 * checkpoint avoids paying this prefill cost repeatedly, but only when the
 * rendered prompt text still matches the file.  The same fixed path is shared
 * by Flash and Pro; agent_kv_load_path() checks the model id, so switching
 * model families rebuilds this cache instead of restoring incompatible KV. */
static bool agent_worker_reset_to_sysprompt(agent_worker *w, char *err, size_t err_len) {
    ds4_tokens sys = {0};
    agent_worker_build_system_tokens(w, &sys);

    size_t text_len = 0;
    char *text = ds4_kvstore_render_tokens_text(w->engine, &sys, &text_len);
    if (!text) {
        snprintf(err, err_len, "failed to render system prompt");
        ds4_tokens_free(&sys);
        return false;
    }

    bool loaded = false;
    char load_err[160] = {0};
    if (w->sysprompt_path) {
        loaded = agent_kv_load_path(w, w->sysprompt_path, NULL,
                                    text, text_len, &w->transcript,
                                    NULL,
                                    load_err, sizeof(load_err));
        if (loaded) {
            agent_trace(w, "sysprompt kv hit file=%s tokens=%d",
                        w->sysprompt_path, w->transcript.len);
        }
    }

    if (!loaded) {
        if (w->sysprompt_path)
            agent_publish_system_status(w, "Updating system prompt cache...");
        ds4_tokens_free(&w->transcript);
        ds4_tokens_copy(&w->transcript, &sys);
        if (agent_worker_sync_tokens(w, &w->transcript, true, err, err_len) != 0) {
            free(text);
            ds4_tokens_free(&sys);
            return false;
        }
        if (w->sysprompt_path) {
            char save_err[160] = {0};
            char ignored_sha[41];
            if (!agent_kv_save_path(w, w->sysprompt_path, &w->transcript,
                                    "agent-system", ignored_sha,
                                    NULL, 0,
                                    save_err, sizeof(save_err)))
            {
                if (w->cfg->non_interactive) {
                    fprintf(stderr, "ds4-agent: failed to save system prompt KV: %s\n",
                            save_err);
                } else {
                    agent_buf b = {0};
                    agent_buf_puts(&b, "\nds4-agent: failed to save system prompt KV: ");
                    agent_buf_puts(&b, save_err);
                    agent_buf_puts(&b, "\n");
                    char *msg = agent_buf_take(&b);
                    agent_publish(w, msg, strlen(msg));
                    free(msg);
                }
            } else {
                agent_trace(w, "sysprompt kv stored file=%s tokens=%d",
                            w->sysprompt_path, w->transcript.len);
            }
        }
    }

    agent_worker_note_system_prompt_seen(w);
    pthread_mutex_lock(&w->mu);
    w->user_activity = false;
    w->session_dirty = false;
    w->status.state = AGENT_WORKER_IDLE;
    w->status.prefill_done = 0;
    w->status.prefill_total = 0;
    w->status.prefill_tps = 0.0;
    w->status.generated = 0;
    w->status.gen_tps = 0.0;
    w->status.greedy_sampling = false;
    w->status.error[0] = '\0';
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
    w->datetime_context_injected = false;
    agent_worker_clear_session_identity(w);
    free(text);
    ds4_tokens_free(&sys);
    return true;
}

static bool agent_worker_should_stop(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool stop = w->stop;
    pthread_mutex_unlock(&w->mu);
    return stop;
}

static bool agent_worker_wait_distributed_route(agent_worker *w, char *err, size_t err_len) {
    if (!w || !w->cfg ||
        w->cfg->engine.distributed.role != DS4_DISTRIBUTED_COORDINATOR)
        return true;

    char last[160] = {0};
    unsigned ticks = 0;
    const struct timespec delay = {0, 250000000L};
    for (;;) {
        int ready = ds4_session_distributed_route_ready(w->session, err, err_len);
        if (ready > 0) {
            if (ticks != 0) {
                if (w->cfg->non_interactive)
                    fprintf(stderr, "ds4-agent: distributed route ready\n");
                else
                    agent_publish_system_status(w, "Distributed route ready.");
            }
            if (err_len) err[0] = '\0';
            return true;
        }
        if (ready < 0) return false;

        const char *why = err && err[0] ? err : "route incomplete";
        if (strcmp(last, why) != 0 || (ticks % 20u) == 0) {
            if (w->cfg->non_interactive) {
                fprintf(stderr, "ds4-agent: waiting for distributed route: %s\n", why);
            } else {
                char msg[224];
                snprintf(msg, sizeof(msg), "Waiting for distributed route: %s", why);
                agent_publish_system_status(w, msg);
            }
            snprintf(last, sizeof(last), "%s", why);
        }
        if (agent_worker_should_stop(w)) {
            snprintf(err, err_len, "agent stopped while waiting for distributed route");
            return false;
        }
        nanosleep(&delay, NULL);
        ticks++;
    }
}

static bool agent_worker_has_user_session(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool yes = w->user_activity;
    pthread_mutex_unlock(&w->mu);
    return yes;
}

bool agent_worker_needs_save(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool yes = w->user_activity && w->session_dirty;
    pthread_mutex_unlock(&w->mu);
    return yes;
}

/* Save the current session under its stable agent identity.  The worker owns
 * the live KV, so busy /save requests are deferred until a stable append-only
 * point and then executed by the worker thread. */
static bool agent_worker_save_session_now(agent_worker *w, char sha_out[41],
                                          int *tokens_out,
                                          char *err, size_t err_len) {
    if (!agent_worker_has_user_session(w)) {
        snprintf(err, err_len, "nothing to save");
        return false;
    }

    if (agent_worker_sync_tokens(w, &w->transcript, false, err, err_len) != 0)
        return false;
    if (!agent_mkdir_p(w->cache_dir)) {
        snprintf(err, err_len, "failed to create %s", w->cache_dir);
        return false;
    }

    size_t text_len = 0;
    char *text = ds4_kvstore_render_tokens_text(w->engine, &w->transcript,
                                                &text_len);
    if (!text) {
        snprintf(err, err_len, "failed to render session text");
        return false;
    }
    if (!w->session_title) {
        w->session_title = agent_session_title_from_text(text, text_len, 0);
    }
    if (w->session_created_at == 0)
        w->session_created_at = (uint64_t)time(NULL);

    char sha[41];
    agent_session_identity_sha(w->session_title, w->session_created_at, sha);
    char *path = agent_kv_path_for_sha(w->cache_dir, sha);

    bool ok = agent_kv_save_path(w, path, &w->transcript,
                                 "agent-session", sha_out,
                                 w->session_title, w->session_created_at,
                                 err, err_len);
    if (ok) {
        memcpy(w->session_sha, sha, sizeof(w->session_sha));
        if (w->legacy_session_path_to_delete &&
            strcmp(w->legacy_session_path_to_delete, path) != 0)
        {
            unlink(w->legacy_session_path_to_delete);
        }
        free(w->legacy_session_path_to_delete);
        w->legacy_session_path_to_delete = NULL;
        pthread_mutex_lock(&w->mu);
        w->session_dirty = false;
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);
        if (tokens_out) *tokens_out = w->transcript.len;
    }
    free(path);
    free(text);
    return ok;
}

bool agent_worker_save_session(agent_worker *w, char *err, size_t err_len) {
    if (!worker_is_idle(w)) {
        snprintf(err, err_len, "model is busy");
        return false;
    }
    char sha[41];
    int tokens = 0;
    bool ok = agent_worker_save_session_now(w, sha, &tokens, err, err_len);
    if (ok) printf("saved session %.8s (%d tokens)\n", sha, tokens);
    return ok;
}

/* ============================================================================
 * Session Listing, History Rendering, And Completion
 * ============================================================================
 */

static void agent_format_age(uint64_t when, char *buf, size_t len) {
    uint64_t now = (uint64_t)time(NULL);
    uint64_t age = when && now > when ? now - when : 0;
    if (age < 60) snprintf(buf, len, "%llus ago", (unsigned long long)age);
    else if (age < 3600) snprintf(buf, len, "%llum ago", (unsigned long long)(age / 60));
    else if (age < 86400) snprintf(buf, len, "%lluh ago", (unsigned long long)(age / 3600));
    else snprintf(buf, len, "%llud ago", (unsigned long long)(age / 86400));
}

static char *agent_session_title_from_span(const char *p, const char *end,
                                           size_t max_bytes,
                                           const char *empty_title) {
    bool limited = max_bytes != 0;
    if (limited && max_bytes < 4) max_bytes = 4;
    while (p < end && isspace((unsigned char)*p)) p++;
    while (end > p && isspace((unsigned char)end[-1])) end--;

    agent_buf b = {0};
    bool space = false;
    bool truncated = false;
    for (const char *s = p; s < end; s++) {
        unsigned char c = (unsigned char)*s;
        if (isspace(c)) {
            space = b.len != 0;
            continue;
        }
        if (space && (!limited || b.len + 4 < max_bytes)) {
            agent_buf_puts(&b, " ");
            space = false;
        }
        if (limited && b.len + 4 > max_bytes) {
            truncated = true;
            break;
        }
        agent_buf_append(&b, s, 1);
    }
    if (truncated) agent_buf_puts(&b, "...");
    if (!b.ptr || !b.len) {
        free(b.ptr);
        return xstrdup(empty_title);
    }
    return agent_buf_take(&b);
}

static char *agent_session_title_from_prompt(const char *prompt,
                                             size_t max_bytes) {
    const char *p = prompt ? prompt : "";
    return agent_session_title_from_span(p, p + strlen(p), max_bytes,
                                         "(empty user prompt)");
}

/* Extract a human-readable title from the first user turn stored in the
 * rendered transcript.  max_bytes==0 means "full normalized title"; callers
 * that render to the terminal pass an explicit display budget. */
static char *agent_session_title_from_text(const char *text, size_t text_len,
                                           size_t max_bytes) {
    static const char user_mark[] = "<｜User｜>";
    static const char assistant_mark[] = "<｜Assistant｜>";
    const char *p = text ? strstr(text, user_mark) : NULL;
    if (!p) return xstrdup("(no user prompt)");
    p += strlen(user_mark);
    const char *end = text + text_len;
    const char *assistant = strstr(p, assistant_mark);
    const char *next_user = strstr(p, user_mark);
    if (assistant && assistant < end) end = assistant;
    if (next_user && next_user < end) end = next_user;
    return agent_session_title_from_span(p, end, max_bytes,
                                         "(empty user prompt)");
}

static char *agent_session_title_clip(const char *title, size_t max_bytes) {
    if (!title) return xstrdup("(no user prompt)");
    size_t len = strlen(title);
    if (max_bytes == 0 || len <= max_bytes) return xstrdup(title);
    if (max_bytes < 4) max_bytes = 4;
    agent_buf b = {0};
    agent_buf_append(&b, title, max_bytes - 3);
    agent_buf_puts(&b, "...");
    return agent_buf_take(&b);
}

static char *agent_session_title_from_file(const char *path, size_t max_bytes) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return xstrdup("(unreadable session)");
    ds4_kvstore_entry hdr = {0};
    uint32_t text_bytes = 0;
    char *text = NULL;
    char *trailer_title = NULL;
    bool ok = ds4_kvstore_read_header(fp, &hdr, &text_bytes) &&
              agent_kv_read_text(fp, text_bytes, &text, NULL, 0);
    if (ok && (hdr.ext_flags & DS4_KVSTORE_EXT_SESSION_TITLE))
        ok = agent_kv_read_title_trailer(fp, &hdr, &trailer_title, NULL, 0);
    fclose(fp);
    char *title = ok ?
        (trailer_title ?
            agent_session_title_clip(trailer_title, max_bytes) :
            agent_session_title_from_text(text, text_bytes, max_bytes)) :
        xstrdup("(unreadable session)");
    free(trailer_title);
    free(text);
    return title;
}

#define AGENT_HISTORY_DEFAULT_TURNS 3
#define AGENT_HISTORY_MAX_TURNS 200
#define AGENT_HISTORY_ASSISTANT_MAX_LINES 80
#define AGENT_HISTORY_ASSISTANT_MAX_BYTES 12000

typedef enum {
    AGENT_HISTORY_MARK_NONE,
    AGENT_HISTORY_MARK_USER,
    AGENT_HISTORY_MARK_ASSISTANT,
    AGENT_HISTORY_MARK_EOS,
} agent_history_mark;

typedef struct {
    const char **v;
    agent_history_mark *mark;
    int len;
    int cap;
} agent_history_ptrs;

static void agent_history_ptrs_push(agent_history_ptrs *p, const char *s,
                                    agent_history_mark mark) {
    if (p->len == p->cap) {
        p->cap = p->cap ? p->cap * 2 : 16;
        p->v = xrealloc(p->v, (size_t)p->cap * sizeof(p->v[0]));
        p->mark = xrealloc(p->mark, (size_t)p->cap * sizeof(p->mark[0]));
    }
    p->v[p->len] = s;
    p->mark[p->len] = mark;
    p->len++;
}

static const char *agent_memmem(const char *hay, size_t hay_len,
                                const char *needle, size_t needle_len) {
    if (!needle_len) return hay;
    if (needle_len > hay_len) return NULL;
    const char first = needle[0];
    const char *end = hay + hay_len - needle_len + 1;
    for (const char *p = hay; p < end; p++) {
        if (*p == first && memcmp(p, needle, needle_len) == 0) return p;
    }
    return NULL;
}

static const char *agent_history_next_marker(const char *p, const char *end,
                                             agent_history_mark *mark,
                                             size_t *mark_len) {
    static const char user_mark[] = "<｜User｜>";
    static const char assistant_mark[] = "<｜Assistant｜>";
    static const char eos_mark[] = "<｜end▁of▁sentence｜>";
    const char *u = agent_memmem(p, (size_t)(end - p),
                                 user_mark, sizeof(user_mark) - 1);
    const char *a = agent_memmem(p, (size_t)(end - p),
                                 assistant_mark, sizeof(assistant_mark) - 1);
    const char *e = agent_memmem(p, (size_t)(end - p),
                                 eos_mark, sizeof(eos_mark) - 1);
    if (!u && !a && !e) return NULL;
    if (u && (!a || u < a) && (!e || u < e)) {
        if (mark) *mark = AGENT_HISTORY_MARK_USER;
        if (mark_len) *mark_len = sizeof(user_mark) - 1;
        return u;
    }
    if (a && (!e || a < e)) {
        if (mark) *mark = AGENT_HISTORY_MARK_ASSISTANT;
        if (mark_len) *mark_len = sizeof(assistant_mark) - 1;
        return a;
    }
    if (mark) *mark = AGENT_HISTORY_MARK_EOS;
    if (mark_len) *mark_len = sizeof(eos_mark) - 1;
    return e;
}

static void agent_history_trim(const char **p, const char **end) {
    while (*p < *end && isspace((unsigned char)**p)) (*p)++;
    while (*end > *p && isspace((unsigned char)(*end)[-1])) (*end)--;
}

static bool agent_history_has_prefix(const char *p, const char *end,
                                     const char *prefix) {
    size_t n = strlen(prefix);
    return (size_t)(end - p) >= n && memcmp(p, prefix, n) == 0;
}

/* Tool messages are rendered as user turns in the transcript.  Return the
 * inner payload for the current <tool_result> wrapper so /history skips these
 * pseudo-user turns and displays their content without leaking the wrapper. */
static bool agent_history_tool_result_payload(const char **p, const char **end) {
    const char *s = *p, *e = *end;
    agent_history_trim(&s, &e);

    const char *open = "<tool_result>";
    const char *close = "</tool_result>";
    const size_t open_len = strlen(open);
    const size_t close_len = strlen(close);
    if (!agent_history_has_prefix(s, e, open)) return false;

    s += open_len;
    if ((size_t)(e - s) >= close_len &&
        memcmp(e - close_len, close, close_len) == 0)
    {
        e -= close_len;
    }
    *p = s;
    *end = e;
    return true;
}

static bool agent_history_is_tool_user(const char *p, const char *end) {
    agent_history_trim(&p, &end);
    return agent_history_tool_result_payload(&p, &end) ||
           agent_history_has_prefix(p, end, "Tool:") ||
           agent_history_has_prefix(p, end, "Tool result");
}

static void agent_history_ptrs_free(agent_history_ptrs *p) {
    free(p->v);
    free(p->mark);
    memset(p, 0, sizeof(*p));
}

/* Find the oldest rendered-chat marker needed to show the last N user turns.
 * Tool-result pseudo-user turns are skipped while human turns exist, so
 * /history stays centered on the human conversation.  Compacted sessions can
 * legitimately have a tail made only of tool result turns; in that case we
 * fall back to recent tool/assistant events instead of showing an empty
 * history. */
static const char *agent_history_start_for_turns(const char *text, size_t len,
                                                 int user_turns,
                                                 bool *tool_only) {
    const char *end = text + len;
    agent_history_ptrs marks = {0};
    agent_history_ptrs users = {0};
    agent_history_ptrs all_users = {0};
    const char *p = text;
    while (p < end) {
        agent_history_mark mark = AGENT_HISTORY_MARK_NONE;
        size_t mark_len = 0;
        const char *m = agent_history_next_marker(p, end, &mark, &mark_len);
        if (!m) break;
        agent_history_ptrs_push(&marks, m, mark);
        const char *content = m + mark_len;
        agent_history_mark next_mark = AGENT_HISTORY_MARK_NONE;
        size_t next_len = 0;
        const char *next = agent_history_next_marker(content, end,
                                                     &next_mark, &next_len);
        const char *content_end = next ? next : end;
        if (mark == AGENT_HISTORY_MARK_USER) {
            agent_history_ptrs_push(&all_users, m, mark);
            if (!agent_history_is_tool_user(content, content_end))
                agent_history_ptrs_push(&users, m, mark);
        }
        p = content_end;
    }

    const char *start = end;
    if (tool_only) *tool_only = false;
    if (users.len > 0) {
        int idx = users.len - user_turns;
        if (idx < 0) idx = 0;
        start = users.v[idx];
    } else if (all_users.len > 0) {
        int idx = all_users.len - user_turns;
        if (idx < 0) idx = 0;
        start = all_users.v[idx];
        if (tool_only) *tool_only = true;

        /* Tool result messages are stored as user-role turns after the
         * assistant DSML stanza that produced them.  Include that preceding
         * assistant marker when it is still in the retained tail, otherwise
         * replay shows the result but hides the call that caused it. */
        for (int i = marks.len - 1; i >= 0; i--) {
            if (marks.v[i] >= start) continue;
            if (marks.mark[i] == AGENT_HISTORY_MARK_USER) break;
            if (marks.mark[i] == AGENT_HISTORY_MARK_ASSISTANT) {
                start = marks.v[i];
                break;
            }
        }
    }
    agent_history_ptrs_free(&marks);
    agent_history_ptrs_free(&users);
    agent_history_ptrs_free(&all_users);
    return start;
}

static bool agent_history_latest_compaction_summary(const char *text,
                                                    size_t len,
                                                    const char **sum_start,
                                                    const char **sum_end) {
    static const char start_mark[] =
        "[ds4-agent compacted earlier conversation. Durable task-state summary follows.]";
    static const char end_mark[] =
        "[End compacted summary. Recent conversation continues verbatim below.]";
    const char *end = text + len;
    const char *scan = text;
    const char *best_start = NULL;
    const char *best_end = NULL;
    while (scan < end) {
        const char *s = agent_memmem(scan, (size_t)(end - scan),
                                     start_mark, sizeof(start_mark) - 1);
        if (!s) break;
        const char *content = s + sizeof(start_mark) - 1;
        const char *e = agent_memmem(content, (size_t)(end - content),
                                     end_mark, sizeof(end_mark) - 1);
        if (!e) break;
        best_start = content;
        best_end = e;
        scan = e + sizeof(end_mark) - 1;
    }
    if (!best_start || !best_end) return false;
    agent_history_trim(&best_start, &best_end);
    if (best_start >= best_end) return false;
    if (sum_start) *sum_start = best_start;
    if (sum_end) *sum_end = best_end;
    return true;
}

static void agent_history_publish_limited(agent_worker *w, const char *p,
                                          const char *end, int max_lines,
                                          size_t max_bytes);

static void agent_history_render_compaction_summary(agent_worker *w,
                                                    const char *text,
                                                    size_t len) {
    const char *p = NULL, *end = NULL;
    if (!agent_history_latest_compaction_summary(text, len, &p, &end)) return;
    bool color = isatty(STDOUT_FILENO) != 0;
    if (color) {
        const char *s = "\n\x1b[1;95mCompacted Summary:\x1b[0m\n";
        agent_publish(w, s, strlen(s));
    } else {
        agent_publish(w, "\nCompacted Summary:\n",
                      strlen("\nCompacted Summary:\n"));
    }
    agent_history_publish_limited(w, p, end, 80, 12000);
}

static const char *agent_history_skip_utf8_continuation(const char *p,
                                                        const char *end) {
    while (p < end && (((unsigned char)*p) & 0xc0) == 0x80) p++;
    return p;
}

static const char *agent_history_tail_start(const char *p, const char *end,
                                            int max_lines, size_t max_bytes,
                                            bool *truncated) {
    *truncated = false;
    if (p >= end) return p;

    const char *start = p;
    size_t len = (size_t)(end - p);
    if (max_bytes && len > max_bytes) {
        start = end - max_bytes;
        *truncated = true;
    }

    if (max_lines > 0) {
        const char *scan = end;
        if (scan > p && scan[-1] == '\n') scan--;
        const char *line_start = p;
        int lines = 0;
        while (scan > p) {
            scan--;
            if (*scan == '\n' && ++lines == max_lines) {
                line_start = scan + 1;
                break;
            }
        }
        if (line_start > p) *truncated = true;
        if (line_start > start) start = line_start;
    }

    return agent_history_skip_utf8_continuation(start, end);
}

static void agent_history_publish_limited(agent_worker *w, const char *p,
                                          const char *end, int max_lines,
                                          size_t max_bytes) {
    bool truncated = false;
    const char *start = agent_history_tail_start(p, end, max_lines, max_bytes,
                                                 &truncated);
    if (truncated)
        agent_publish(w, "\n... earlier history truncated; showing tail ...\n",
                      strlen("\n... earlier history truncated; showing tail ...\n"));
    agent_publish(w, start, (size_t)(end - start));
    if (end > start && end[-1] != '\n') agent_publish(w, "\n", 1);
}

static void agent_history_render_assistant(agent_worker *w,
                                           const char *p, const char *end) {
    agent_history_trim(&p, &end);
    if (p >= end) return;
    bool source_truncated = false;
    (void)agent_history_tail_start(p, end,
                                   AGENT_HISTORY_ASSISTANT_MAX_LINES,
                                   AGENT_HISTORY_ASSISTANT_MAX_BYTES,
                                   &source_truncated);
    bool use_color = isatty(STDOUT_FILENO) != 0;
    agent_tail_capture tail = {
        .cap = source_truncated ? AGENT_HISTORY_ASSISTANT_MAX_BYTES : 0,
    };
    agent_token_renderer renderer = {
        .engine = w->engine,
        .worker = w,
        .format_thinking = true,
        /* History replay should look like the original live output: the user is
         * switching back to a session, not reading a different transcript
         * format.  Tool calls are still dry-rendered below, so replay never
         * executes tools or mutates transcript state. */
        .format_markdown = true,
        .use_color = use_color && !source_truncated,
        .last_output_newline = true,
        .capture = source_truncated ? &tail : NULL,
    };
    agent_dsml_parser dsml = {.state = AGENT_DSML_SEARCH};
    agent_stream_renderer stream = {
        .renderer = &renderer,
        .parser = &dsml,
        .replay = true,
    };

    /* Dry-run replay: the same streaming projection hides DSML and renders
     * semantic tool lines, but no tool is executed and no transcript state is
     * changed.  The saved KV payload remains the only authority for resume. */
    agent_stream_text(&stream, p, (size_t)(end - p), true);
    renderer_finish(&renderer);
    agent_dsml_parser_free(&dsml);

    if (source_truncated) {
        size_t tail_len = 0;
        char *tail_text = agent_tail_capture_take(&tail, &tail_len);
        bool rendered_truncated = tail.total > tail_len;
        bool line_truncated = false;
        const char *tail_start =
            agent_history_tail_start(tail_text, tail_text + tail_len,
                                     AGENT_HISTORY_ASSISTANT_MAX_LINES,
                                     AGENT_HISTORY_ASSISTANT_MAX_BYTES,
                                     &line_truncated);
        if (use_color) agent_publish(w, "\x1b[90m", 5);
        agent_publish(w,
                      "\n... earlier assistant history truncated; showing tail ...\n",
                      strlen("\n... earlier assistant history truncated; showing tail ...\n"));
        (void)rendered_truncated;
        agent_publish(w, tail_start, (size_t)(tail_text + tail_len - tail_start));
        if (tail_len && tail_text[tail_len - 1] != '\n') agent_publish(w, "\n", 1);
        if (use_color) agent_publish(w, "\x1b[0m", 4);
        free(tail_text);
    }
}

/* Re-render saved transcript text for /history and /switch.  It intentionally
 * uses the same assistant/token renderer as live output, so restored history
 * looks like the original terminal stream instead of raw rendered-chat text. */
static void agent_history_render_text(agent_worker *w, const char *text,
                                      size_t len, int user_turns) {
    if (user_turns <= 0) return;
    if (user_turns > AGENT_HISTORY_MAX_TURNS)
        user_turns = AGENT_HISTORY_MAX_TURNS;

    const char *end = text + len;
    agent_history_render_compaction_summary(w, text, len);

    bool tool_only = false;
    const char *p = agent_history_start_for_turns(text, len, user_turns,
                                                  &tool_only);
    if (p >= end) {
        agent_publish(w, "\n(no user history)\n", strlen("\n(no user history)\n"));
        return;
    }

    bool color = isatty(STDOUT_FILENO) != 0;
    if (color) agent_publish(w, "\n\x1b[90m", strlen("\n\x1b[90m"));
    else agent_publish(w, "\n", 1);
    if (tool_only) {
        agent_publishf(w, "--- session history: recent tool/assistant events ---\n");
    } else {
        agent_publishf(w, "--- session history: last %d user turn%s ---\n",
                       user_turns, user_turns == 1 ? "" : "s");
    }
    if (color) agent_publish(w, "\x1b[0m", 4);

    while (p < end) {
        agent_history_mark mark = AGENT_HISTORY_MARK_NONE;
        size_t mark_len = 0;
        const char *m = agent_history_next_marker(p, end, &mark, &mark_len);
        if (!m) break;
        const char *content = m + mark_len;
        agent_history_mark next_mark = AGENT_HISTORY_MARK_NONE;
        size_t next_len = 0;
        const char *next = agent_history_next_marker(content, end,
                                                     &next_mark, &next_len);
        const char *content_end = next ? next : end;
        const char *tp = content, *te = content_end;
        agent_history_trim(&tp, &te);

        if (mark == AGENT_HISTORY_MARK_USER) {
            if (agent_history_is_tool_user(tp, te)) {
                const char *payload_start = tp;
                const char *payload_end = te;
                (void)agent_history_tool_result_payload(&payload_start,
                                                        &payload_end);
                if (color) {
                    const char *s = "\x1b[90mTool result:\n";
                    agent_publish(w, s, strlen(s));
                } else {
                    agent_publish(w, "Tool result:\n", strlen("Tool result:\n"));
                }
                agent_history_publish_limited(w, payload_start, payload_end,
                                              12, 3000);
                if (color) agent_publish(w, "\x1b[0m", 4);
            } else {
                if (color) {
                    const char *s = "\x1b[1;32mUser:\x1b[0m\n";
                    agent_publish(w, s, strlen(s));
                } else {
                    agent_publish(w, "User:\n", strlen("User:\n"));
                }
                agent_history_publish_limited(w, tp, te, 24, 6000);
            }
        } else if (mark == AGENT_HISTORY_MARK_ASSISTANT) {
            if (color) {
                const char *s = "\x1b[1;37mAssistant:\x1b[0m\n";
                agent_publish(w, s, strlen(s));
            } else {
                agent_publish(w, "Assistant:\n", strlen("Assistant:\n"));
            }
            agent_history_render_assistant(w, tp, te);
        }
        p = content_end;
    }

    if (color) {
        const char *s = "\x1b[90m--- end history ---\x1b[0m\n";
        agent_publish(w, s, strlen(s));
    } else {
        agent_publish(w, "--- end history ---\n", strlen("--- end history ---\n"));
    }
}

/* Render recent saved transcript text without mutating the live session. */
static bool agent_worker_show_history(agent_worker *w, int user_turns,
                                      char *err, size_t err_len) {
    if (!worker_is_idle(w)) {
        snprintf(err, err_len, "model is busy");
        return false;
    }
    size_t text_len = 0;
    char *text = ds4_kvstore_render_tokens_text(w->engine, &w->transcript,
                                                &text_len);
    if (!text) {
        snprintf(err, err_len, "failed to render session text");
        return false;
    }
    agent_history_render_text(w, text, text_len, user_turns);
    free(text);
    return true;
}

typedef struct {
    ds4_kvstore_entry entry;
    char *title;
} agent_session_list_item;

static int agent_session_list_cmp_recent(const void *a, const void *b) {
    const agent_session_list_item *sa = a, *sb = b;
    uint64_t ta = sa->entry.last_used ? sa->entry.last_used : sa->entry.created_at;
    uint64_t tb = sb->entry.last_used ? sb->entry.last_used : sb->entry.created_at;
    if (ta < tb) return 1;
    if (ta > tb) return -1;
    return strcmp(sa->entry.sha, sb->entry.sha);
}

static void agent_session_list_free(agent_session_list_item *v, int n) {
    for (int i = 0; i < n; i++) {
        ds4_kvstore_entry_free(&v[i].entry);
        free(v[i].title);
    }
    free(v);
}

static void agent_session_list_push(agent_session_list_item **v, int *len,
                                    int *cap, ds4_kvstore_entry entry,
                                    char *title) {
    if (*len == *cap) {
        *cap = *cap ? *cap * 2 : 16;
        *v = xrealloc(*v, (size_t)*cap * sizeof((*v)[0]));
    }
    (*v)[(*len)++] = (agent_session_list_item){
        .entry = entry,
        .title = title,
    };
}

/* Print resumable sessions from ~/.ds4/kvcache.  sysprompt.kv is intentionally
 * ignored because it is an implementation cache, not a user session. */
static void agent_worker_list_sessions(agent_worker *w) {
    DIR *d = opendir(w->cache_dir);
    if (!d) {
        printf("no sessions: %s\n", strerror(errno));
        return;
    }

    int cols = renderer_terminal_cols();
    size_t title_budget = cols > 16 ? (size_t)(cols - 12) : 20;
    if (title_budget > 160) title_budget = 160;

    agent_session_list_item *sessions = NULL;
    int sessions_len = 0, sessions_cap = 0;
    const uint8_t model_id = (uint8_t)ds4_engine_model_id(w->engine);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        char sha[41];
        if (!ds4_kvstore_sha_hex_name(de->d_name, sha)) continue;
        char *path = ds4_kvstore_path_join(w->cache_dir, de->d_name);
        ds4_kvstore_entry e = {0};
        if (ds4_kvstore_read_entry_file(path, sha, &e)) {
            if (e.model_id == model_id) {
                char *title = agent_session_title_from_file(path, title_budget);
                agent_session_list_push(&sessions, &sessions_len, &sessions_cap,
                                        e, title);
            } else {
                ds4_kvstore_entry_free(&e);
            }
        }
        free(path);
    }
    closedir(d);
    if (!sessions_len) {
        printf("no saved sessions\n");
        return;
    }

    qsort(sessions, (size_t)sessions_len, sizeof(sessions[0]),
          agent_session_list_cmp_recent);

    bool color = isatty(STDOUT_FILENO) != 0;
    const char *sha_on = color ? "\x1b[1;96m" : "";
    const char *title_on = color ? "\x1b[1;97m" : "";
    const char *help_on = color ? "\x1b[97m" : "";
    const char *dim = color ? "\x1b[90m" : "";
    const char *reset = color ? "\x1b[0m" : "";

    for (int i = 0; i < sessions_len; i++) {
        ds4_kvstore_entry *e = &sessions[i].entry;
        char age[32];
        agent_format_age(e->last_used ? e->last_used : e->created_at,
                         age, sizeof(age));
        printf("%s%.8s%s %s>%s %s%s%s\n",
               sha_on, e->sha, reset, dim, reset,
               title_on, sessions[i].title, reset);
        printf("         %s> %s, %u tokens, %.2f MB%s%s\n\n",
               dim, age, e->tokens,
               (double)e->file_size / (1024.0 * 1024.0),
               e->payload_bytes == 0 ? ", stripped" : "",
               reset);
    }
    printf("%sUse /switch <id> to select a session, /del <id> to remove, "
           "/strip <id> to strip KV cache.%s\n",
           help_on, reset);
    agent_session_list_free(sessions, sessions_len);
}

typedef struct {
    char sha[41];
    uint64_t last_used;
} agent_completion_session;

typedef struct {
    agent_completion_session *v;
    int len;
    int cap;
} agent_completion_sessions;

static void agent_completion_sessions_push(agent_completion_sessions *s,
                                           const char sha[41],
                                           uint64_t last_used) {
    if (s->len == s->cap) {
        s->cap = s->cap ? s->cap * 2 : 16;
        s->v = xrealloc(s->v, (size_t)s->cap * sizeof(s->v[0]));
    }
    memcpy(s->v[s->len].sha, sha, 41);
    s->v[s->len].last_used = last_used;
    s->len++;
}

static int agent_completion_session_cmp(const void *a, const void *b) {
    const agent_completion_session *sa = a, *sb = b;
    if (sa->last_used < sb->last_used) return 1;
    if (sa->last_used > sb->last_used) return -1;
    return strcmp(sa->sha, sb->sha);
}

/* Tab completion for /switch.  Suggestions are sorted by recent use and accept
 * either an empty prefix or any unambiguous hex prefix. */
static void agent_switch_completion_callback(const char *buf,
                                             linenoiseCompletions *lc) {
    agent_worker *w = agent_completion_worker;
    static const char cmd[] = "/switch";
    const size_t cmd_len = sizeof(cmd) - 1;
    if (!w || !buf || strncmp(buf, cmd, cmd_len) != 0) return;

    const char *p = buf + cmd_len;
    if (*p && *p != ' ' && *p != '\t') return;
    while (*p == ' ' || *p == '\t') p++;

    const char *prefix = p;
    size_t prefix_len = strlen(prefix);
    for (size_t i = 0; i < prefix_len; i++) {
        if (!isxdigit((unsigned char)prefix[i])) return;
    }
    if (prefix_len > 40) return;

    DIR *d = opendir(w->cache_dir);
    if (!d) return;

    agent_completion_sessions sessions = {0};
    const uint8_t model_id = (uint8_t)ds4_engine_model_id(w->engine);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        char sha[41];
        if (!ds4_kvstore_sha_hex_name(de->d_name, sha)) continue;
        if (prefix_len && strncasecmp(sha, prefix, prefix_len) != 0) continue;

        uint64_t last_used = 0;
        char *path = ds4_kvstore_path_join(w->cache_dir, de->d_name);
        ds4_kvstore_entry e = {0};
        if (ds4_kvstore_read_entry_file(path, sha, &e)) {
            if (e.model_id == model_id) last_used = e.last_used;
            else last_used = UINT64_MAX;
            ds4_kvstore_entry_free(&e);
        } else {
            last_used = UINT64_MAX;
        }
        free(path);
        if (last_used == UINT64_MAX) continue;
        agent_completion_sessions_push(&sessions, sha, last_used);
    }
    closedir(d);

    qsort(sessions.v, (size_t)sessions.len, sizeof(sessions.v[0]),
          agent_completion_session_cmp);
    for (int i = 0; i < sessions.len; i++) {
        char line[64];
        int sha_chars = prefix_len > 8 ? 40 : 8;
        snprintf(line, sizeof(line), "/switch %.*s",
                 sha_chars, sessions.v[i].sha);
        linenoiseAddCompletion(lc, line);
    }
    free(sessions.v);
}

/* Resolve a user-provided SHA prefix to exactly one saved session file. */
static bool agent_worker_find_session(agent_worker *w, const char *prefix,
                                      char sha_out[41], char **path_out,
                                      char *err, size_t err_len) {
    size_t plen = strlen(prefix);
    if (plen == 0 || plen > 40) {
        snprintf(err, err_len, "invalid session SHA prefix");
        return false;
    }
    for (size_t i = 0; i < plen; i++) {
        if (!isxdigit((unsigned char)prefix[i])) {
            snprintf(err, err_len, "invalid session SHA prefix");
            return false;
        }
    }

    DIR *d = opendir(w->cache_dir);
    if (!d) {
        snprintf(err, err_len, "%s", strerror(errno));
        return false;
    }
    int matches = 0;
    char match_sha[41] = {0};
    char *match_path = NULL;
    const uint8_t model_id = (uint8_t)ds4_engine_model_id(w->engine);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        char sha[41];
        if (!ds4_kvstore_sha_hex_name(de->d_name, sha)) continue;
        if (strncasecmp(sha, prefix, plen) != 0) continue;
        char *path = ds4_kvstore_path_join(w->cache_dir, de->d_name);
        ds4_kvstore_entry e = {0};
        bool same_model = ds4_kvstore_read_entry_file(path, sha, &e) &&
                          e.model_id == model_id;
        ds4_kvstore_entry_free(&e);
        if (!same_model) {
            free(path);
            continue;
        }
        matches++;
        if (matches == 1) {
            memcpy(match_sha, sha, sizeof(match_sha));
            match_path = path;
        } else {
            free(path);
        }
    }
    closedir(d);
    if (matches == 0) {
        snprintf(err, err_len, "no saved session matches %.40s", prefix);
        return false;
    }
    if (matches > 1) {
        snprintf(err, err_len, "session prefix %.40s is ambiguous", prefix);
        free(match_path);
        return false;
    }
    memcpy(sha_out, match_sha, 41);
    *path_out = match_path;
    return true;
}

static bool agent_worker_delete_session(agent_worker *w, const char *prefix,
                                        char sha_out[41],
                                        char *err, size_t err_len) {
    char sha[41];
    char *path = NULL;
    if (!agent_worker_find_session(w, prefix, sha, &path, err, err_len))
        return false;
    if (unlink(path) != 0) {
        snprintf(err, err_len, "%s", strerror(errno));
        free(path);
        return false;
    }
    if (sha_out) memcpy(sha_out, sha, 41);
    free(path);
    return true;
}

/* Strip the heavy backend payload from a saved session while preserving its
 * rendered transcript. Loading such a file later tokenizes the text and
 * rebuilds the live KV with a full prefill. */
static bool agent_worker_strip_session(agent_worker *w, const char *prefix,
                                       char sha_out[41],
                                       uint32_t *tokens_out,
                                       char *err, size_t err_len) {
    if (err && err_len) err[0] = '\0';
    char sha[41];
    char *path = NULL;
    if (!agent_worker_find_session(w, prefix, sha, &path, err, err_len))
        return false;

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        snprintf(err, err_len, "%s", strerror(errno));
        free(path);
        return false;
    }

    ds4_kvstore_entry hdr = {0};
    uint32_t text_bytes = 0;
    char *text = NULL;
    char *title = NULL;
    bool ok = ds4_kvstore_read_header(fp, &hdr, &text_bytes) &&
              agent_kv_read_text(fp, text_bytes, &text, err, err_len);
    if (ok && (hdr.ext_flags & DS4_KVSTORE_EXT_SESSION_TITLE))
        ok = agent_kv_read_title_trailer(fp, &hdr, &title, err, err_len);
    fclose(fp);
    if (!ok) {
        if (!err[0]) snprintf(err, err_len, "failed to read session");
        free(title);
        free(text);
        free(path);
        return false;
    }

    char actual_sha[41];
    agent_kv_identity_sha(&hdr, text, text_bytes, title, actual_sha);
    if (strcmp(actual_sha, sha)) {
        snprintf(err, err_len, "cached session identity does not match file name");
        free(title);
        free(text);
        free(path);
        return false;
    }

    ds4_tokens stripped_tokens = {0};
    ds4_tokenize_rendered_chat(w->engine, text, &stripped_tokens);
    uint32_t stripped_token_count = (uint32_t)stripped_tokens.len;
    ds4_tokens_free(&stripped_tokens);

    agent_buf tmpl = {0};
    agent_buf_puts(&tmpl, path);
    agent_buf_puts(&tmpl, ".tmp.XXXXXX");
    char *tmp = agent_buf_take(&tmpl);
    int fd = mkstemp(tmp);
    if (fd < 0) {
        snprintf(err, err_len, "%s", strerror(errno));
        free(tmp);
        free(text);
        free(path);
        return false;
    }

    fp = fdopen(fd, "wb");
    if (!fp) {
        snprintf(err, err_len, "%s", strerror(errno));
        close(fd);
        unlink(tmp);
        free(tmp);
        free(text);
        free(path);
        return false;
    }

    uint8_t h[DS4_KVSTORE_FIXED_HEADER];
    uint64_t now = (uint64_t)time(NULL);
    ds4_kvstore_fill_header(h, hdr.model_id, hdr.quant_bits, hdr.reason, hdr.ext_flags,
                            stripped_token_count, hdr.hits, hdr.ctx_size,
                            hdr.created_at, now, 0);
    uint8_t tb[4];
    ds4_kvstore_le_put32(tb, text_bytes);

    errno = 0;
    ok = fwrite(h, 1, sizeof(h), fp) == sizeof(h) &&
         fwrite(tb, 1, sizeof(tb), fp) == sizeof(tb) &&
         fwrite(text, 1, text_bytes, fp) == text_bytes &&
         (!(hdr.ext_flags & DS4_KVSTORE_EXT_SESSION_TITLE) ||
          agent_kv_write_title_trailer(fp, title, err, err_len)) &&
         fflush(fp) == 0;
    int saved_errno = errno;
    if (fclose(fp) != 0) {
        if (!saved_errno) saved_errno = errno;
        ok = false;
    }
    if (ok && rename(tmp, path) != 0) {
        saved_errno = errno;
        ok = false;
    }
    if (!ok) {
        snprintf(err, err_len, "%s",
                 saved_errno ? strerror(saved_errno) : "failed to write stripped session");
        unlink(tmp);
    } else {
        if (sha_out) memcpy(sha_out, sha, 41);
        if (tokens_out) *tokens_out = stripped_token_count;
    }

    free(tmp);
    free(title);
    free(text);
    free(path);
    return ok;
}

/* Load a saved session during worker initialization instead of creating a fresh
 * system prompt session.  Unlike agent_worker_switch_session, this works before
 * the worker is marked initialized and does not require an idle worker. */
static bool agent_worker_recover_session(agent_worker *w, const char *prefix,
                                         char *err, size_t err_len) {
    char sha[41];
    char *path = NULL;
    if (!agent_worker_find_session(w, prefix, sha, &path, err, err_len))
        return false;

    bool stripped = false;
    ds4_kvstore_entry entry = {0};
    if (ds4_kvstore_read_entry_file(path, sha, &entry)) {
        stripped = entry.payload_bytes == 0;
        ds4_kvstore_entry_free(&entry);
    }

    ds4_tokens loaded = {0};
    agent_kv_session_meta meta = {0};
    bool ok = agent_kv_load_path(w, path, sha, NULL, 0, &loaded, &meta,
                                 err, err_len);
    if (ok) {
        ds4_tokens_free(&w->transcript);
        w->transcript = loaded;
        free(w->session_title);
        w->session_title = meta.title ? xstrdup(meta.title) : xstrdup("(no user prompt)");
        w->session_created_at = meta.created_at ? meta.created_at : (uint64_t)time(NULL);
        memcpy(w->session_sha, sha, sizeof(w->session_sha));
        free(w->legacy_session_path_to_delete);
        w->legacy_session_path_to_delete = meta.legacy_identity ? xstrdup(path) : NULL;
        agent_worker_note_system_prompt_seen(w);
        w->datetime_context_injected = true;
        pthread_mutex_lock(&w->mu);
        w->user_activity = true;
        w->session_dirty = false;
        w->status.state = AGENT_WORKER_IDLE;
        w->status.prefill_tps = 0.0;
        w->status.greedy_sampling = false;
        w->status.error[0] = '\0';
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);
        agent_trace(w, "recovered session %.8s (%d tokens%s)",
                    sha, w->transcript.len, stripped ? ", rebuilt from text" : "");
        /* Show recent conversation history like agent_worker_switch_session does,
         * but without the worker_is_idle check since we are still initializing. */
        {
            size_t text_len = 0;
            char *text = ds4_kvstore_render_tokens_text(w->engine, &w->transcript,
                                                        &text_len);
            if (text) {
                agent_history_render_text(w, text, text_len, AGENT_HISTORY_DEFAULT_TURNS);
                free(text);
            }
        }
        /* Print the recovery banner in magenta so the user knows which session
         * was restored and can use /switch or /list later. */
        {
            bool color = isatty(STDOUT_FILENO) != 0;
            if (color) printf("\x1b[35m");
            printf("recovered session %.8s (%d tokens%s)\n",
                   sha, w->transcript.len,
                   stripped ? ", rebuilt from text" : "");
            if (color) printf("\x1b[0m");
            fflush(stdout);
        }
    } else {
        ds4_tokens_free(&loaded);
    }
    agent_kv_session_meta_free(&meta);
    free(path);
    return ok;
}

/* Load a saved session KV into the live transcript and optionally replay recent
 * history for the human. */
static bool agent_worker_switch_session(agent_worker *w, const char *prefix,
                                        int history_turns,
                                        char *err, size_t err_len) {
    if (!worker_is_idle(w)) {
        snprintf(err, err_len, "model is busy");
        return false;
    }
    char sha[41];
    char *path = NULL;
    if (!agent_worker_find_session(w, prefix, sha, &path, err, err_len))
        return false;

    bool stripped = false;
    ds4_kvstore_entry entry = {0};
    if (ds4_kvstore_read_entry_file(path, sha, &entry)) {
        stripped = entry.payload_bytes == 0;
        ds4_kvstore_entry_free(&entry);
    }
    if (stripped) {
        printf("rebuilding stripped session %.8s from rendered text...\n", sha);
        fflush(stdout);
    }

    ds4_tokens loaded = {0};
    agent_kv_session_meta meta = {0};
    bool ok = agent_kv_load_path(w, path, sha, NULL, 0, &loaded, &meta,
                                 err, err_len);
    if (ok) {
        ds4_tokens_free(&w->transcript);
        w->transcript = loaded;
        free(w->session_title);
        w->session_title = meta.title ? xstrdup(meta.title) : xstrdup("(no user prompt)");
        w->session_created_at = meta.created_at ? meta.created_at : (uint64_t)time(NULL);
        memcpy(w->session_sha, sha, sizeof(w->session_sha));
        free(w->legacy_session_path_to_delete);
        w->legacy_session_path_to_delete = meta.legacy_identity ? xstrdup(path) : NULL;
        agent_worker_note_system_prompt_seen(w);
        w->datetime_context_injected = true;
        pthread_mutex_lock(&w->mu);
        w->user_activity = true;
        w->session_dirty = false;
        w->status.state = AGENT_WORKER_IDLE;
        w->status.ctx_used = w->transcript.len;
        w->status.ctx_size = w->cfg->gen.ctx_size;
        w->status.prefill_tps = 0.0;
        w->status.greedy_sampling = false;
        w->status.error[0] = '\0';
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);
        printf("switched to session %.8s (%d tokens%s)\n",
               sha, w->transcript.len, stripped ? ", rebuilt from text" : "");
        if (history_turns > 0)
            (void)agent_worker_show_history(w, history_turns, err, err_len);
    } else {
        ds4_tokens_free(&loaded);
    }
    agent_kv_session_meta_free(&meta);
    free(path);
    return ok;
}

/* ============================================================================
 * Tool Argument Parsing And File Tool Helpers
 * ============================================================================
 */

static int agent_parse_timeout(const char *s) {
    if (!s || !s[0]) return 3600;
    char *end = NULL;
    double v = strtod(s, &end);
    if (end == s || v <= 0.0 || !isfinite(v)) return 3600;
    if (v < 1.0) v = 1.0;
    if (v > 24.0 * 3600.0) v = 24.0 * 3600.0;
    return (int)v;
}

static int agent_parse_int_default(const char *s, int def, int min, int max) {
    if (!s || !s[0]) return def;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (end == s) return def;
    while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n') end++;
    if (*end) return def;
    if (v < min) v = min;
    if (v > max) v = max;
    return (int)v;
}

static bool agent_parse_bool_default(const char *s, bool def) {
    if (!s || !s[0]) return def;
    if (!strcasecmp(s, "true") || !strcasecmp(s, "yes") || !strcmp(s, "1"))
        return true;
    if (!strcasecmp(s, "false") || !strcasecmp(s, "no") || !strcmp(s, "0"))
        return false;
    return def;
}

#define AGENT_FILE_MAX_BYTES (16*1024*1024)
#define AGENT_READ_DEFAULT_LINES 500
#define AGENT_TOOL_RESULT_RESERVE_TOKENS 1024
#define AGENT_EDIT_UPTO_MIN_PREFIX_BYTES 64
#define AGENT_EDIT_UPTO_MIN_PREFIX_LINES 2
#define AGENT_COMPACT_SOFT_PERCENT 85
#define AGENT_COMPACT_MIN_FREE_TOKENS 8192
#define AGENT_COMPACT_TAIL_DIVISOR 10
#define AGENT_COMPACT_TAIL_CAP_TOKENS 50000
#define AGENT_COMPACT_SUMMARY_MAX_TOKENS 4096

typedef enum {
    AGENT_PATH_EXISTING,
    AGENT_PATH_PARENT
} agent_path_resolution;

typedef struct {
    size_t start;
    size_t content_end;
    size_t end;
} agent_line_span;

typedef struct {
    agent_line_span *v;
    int len;
    int cap;
} agent_line_spans;

static void agent_line_spans_free(agent_line_spans *spans) {
    free(spans->v);
    memset(spans, 0, sizeof(*spans));
}

static void agent_line_spans_push(agent_line_spans *spans, agent_line_span span) {
    if (spans->len == spans->cap) {
        spans->cap = spans->cap ? spans->cap * 2 : 128;
        spans->v = xrealloc(spans->v, (size_t)spans->cap * sizeof(spans->v[0]));
    }
    spans->v[spans->len++] = span;
}

/* Split a text buffer into line spans.  content_end excludes CR/LF so callers
 * can print or compare line content without newline spelling differences. */
static void agent_split_lines(const char *data, size_t len, agent_line_spans *spans) {
    size_t pos = 0;
    while (pos < len) {
        size_t start = pos;
        while (pos < len && data[pos] != '\n' && data[pos] != '\r') pos++;
        size_t content_end = pos;
        if (pos < len) {
            if (data[pos] == '\r' && pos + 1 < len && data[pos + 1] == '\n')
                pos += 2;
            else
                pos++;
        }
        agent_line_spans_push(spans, (agent_line_span){
            .start = start,
            .content_end = content_end,
            .end = pos,
        });
    }
}

static const agent_path_list *agent_working_directories(const agent_worker *w) {
    if (!w || w->working_directories.len <= 0) return NULL;
    return &w->working_directories;
}

static const char *agent_primary_working_directory(const agent_worker *w) {
    const agent_path_list *roots = agent_working_directories(w);
    return roots && roots->len > 0 ? roots->v[0] : NULL;
}

static bool agent_path_is_under_root(const char *root, const char *path) {
    if (!root || !root[0] || !path || !path[0]) return false;
    size_t n = strlen(root);
    if (!strcmp(root, "/")) return path[0] == '/';
    return !strcmp(root, path) || (!strncmp(root, path, n) && path[n] == '/');
}

static bool agent_path_is_under_any_root(const agent_path_list *roots,
                                         const char *path) {
    if (!roots || !path) return false;
    for (int i = 0; i < roots->len; i++) {
        if (agent_path_is_under_root(roots->v[i], path)) return true;
    }
    return false;
}

static bool agent_path_parent(const char *path, char parent[PATH_MAX]) {
    if (!path || !path[0]) return false;
    snprintf(parent, PATH_MAX, "%s", path);
    size_t len = strlen(parent);
    while (len > 1 && parent[len - 1] == '/') parent[--len] = '\0';
    char *slash = strrchr(parent, '/');
    if (!slash) return false;
    if (slash == parent) slash[1] = '\0';
    else *slash = '\0';
    return true;
}

static bool agent_join_path(char *dst, size_t dst_len,
                            const char *base, const char *path,
                            char *err, size_t err_len) {
    int n;
    if (path && path[0] == '/') {
        n = snprintf(dst, dst_len, "%s", path);
    } else if (!base || !base[0] || !strcmp(base, "/")) {
        n = snprintf(dst, dst_len, "/%s", path ? path : "");
    } else {
        n = snprintf(dst, dst_len, "%s/%s", base, path ? path : "");
    }
    if (n < 0 || (size_t)n >= dst_len) {
        snprintf(err, err_len, "path is too long");
        return false;
    }
    return true;
}

static bool agent_tool_use_docker_filesystem(const agent_worker *w);
static void agent_buf_append_shell_quoted(agent_buf *b, const char *s);
static bool agent_docker_shell_exec(agent_worker *w,
                                    const char *cmd,
                                    bool unbounded_output,
                                    const char *op,
                                    agent_buf *out,
                                    int *exit_status);
static bool agent_docker_shell_exec_argv_stderr(agent_worker *w,
                                                char *const cmd_argv[],
                                                bool unbounded_output,
                                                const char *op,
                                                agent_buf *stdout_out,
                                                agent_buf *stderr_out,
                                                int *exit_status);

static bool agent_resolve_existing_candidate(const char *base, const char *path,
                                             char out[PATH_MAX],
                                             char root_out[PATH_MAX],
                                             char *err, size_t err_len) {
    char candidate[PATH_MAX];
    if (!agent_join_path(candidate, sizeof(candidate), base, path, err, err_len))
        return false;
    if (!realpath(candidate, out)) {
        snprintf(err, err_len, "resolve %s: %s", path, strerror(errno));
        return false;
    }

    struct stat st;
    char dir[PATH_MAX];
    if (stat(out, &st) == 0 && S_ISDIR(st.st_mode)) {
        snprintf(dir, sizeof(dir), "%s", out);
    } else {
        snprintf(dir, sizeof(dir), "%s", out);
        char *slash = strrchr(dir, '/');
        if (slash) {
            if (slash == dir) slash[1] = '\0';
            else *slash = '\0';
        } else {
            snprintf(dir, sizeof(dir), ".");
        }
    }
    if (!realpath(dir, root_out)) {
        snprintf(err, err_len, "resolve directory for %s: %s",
                 path, strerror(errno));
        return false;
    }
    return true;
}

static bool agent_resolve_parent_candidate(const char *base, const char *path,
                                           char out[PATH_MAX],
                                           char root_out[PATH_MAX],
                                           char *err, size_t err_len) {
    char candidate[PATH_MAX];
    if (!agent_join_path(candidate, sizeof(candidate), base, path, err, err_len))
        return false;

    char existing[PATH_MAX];
    if (realpath(candidate, existing)) {
        snprintf(out, PATH_MAX, "%s", existing);
        struct stat st;
        if (stat(existing, &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(root_out, PATH_MAX, "%s", existing);
        } else {
            char parent[PATH_MAX];
            snprintf(parent, sizeof(parent), "%s", existing);
            char *slash = strrchr(parent, '/');
            if (slash) {
                if (slash == parent) slash[1] = '\0';
                else *slash = '\0';
            } else {
                snprintf(parent, sizeof(parent), ".");
            }
            if (!realpath(parent, root_out)) {
                snprintf(err, err_len, "resolve parent for %s: %s",
                         path, strerror(errno));
                return false;
            }
        }
        return true;
    }
    if (errno != ENOENT && errno != ENOTDIR) {
        snprintf(err, err_len, "resolve %s: %s", path, strerror(errno));
        return false;
    }

    char parent[PATH_MAX];
    snprintf(parent, sizeof(parent), "%s", candidate);
    size_t len = strlen(parent);
    while (len > 1 && parent[len - 1] == '/') parent[--len] = '\0';
    char *slash = strrchr(parent, '/');
    const char *leaf = slash ? slash + 1 : parent;
    if (!leaf[0] || !strcmp(leaf, ".") || !strcmp(leaf, "..")) {
        snprintf(err, err_len, "invalid path: %s", path);
        return false;
    }
    if (slash) {
        if (slash == parent) slash[1] = '\0';
        else *slash = '\0';
    } else {
        snprintf(parent, sizeof(parent), ".");
    }

    char resolved_parent[PATH_MAX];
    if (!realpath(parent, resolved_parent)) {
        snprintf(err, err_len, "resolve parent for %s: %s", path, strerror(errno));
        return false;
    }
    snprintf(root_out, PATH_MAX, "%s", resolved_parent);
    if (!strcmp(resolved_parent, "/")) {
        int n = snprintf(out, PATH_MAX, "/%s", leaf);
        if (n < 0 || n >= PATH_MAX) {
            snprintf(err, err_len, "path is too long");
            return false;
        }
    } else {
        int n = snprintf(out, PATH_MAX, "%s/%s", resolved_parent, leaf);
        if (n < 0 || n >= PATH_MAX) {
            snprintf(err, err_len, "path is too long");
            return false;
        }
    }
    return true;
}

static void agent_trim_trailing_whitespace(char *s) {
    if (!s) return;
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1]))
        s[--len] = '\0';
}

static bool agent_copy_line(char *dst, size_t dst_len,
                            const char *start, const char *end,
                            char *err, size_t err_len) {
    if (!dst || dst_len == 0 || !start || !end || end < start) {
        snprintf(err, err_len, "invalid path normalization output");
        return false;
    }
    size_t len = (size_t)(end - start);
    while (len > 0 && (start[len - 1] == '\r' || start[len - 1] == '\n'))
        len--;
    if (len == 0 || len >= dst_len) {
        snprintf(err, err_len, len == 0 ?
                 "invalid path normalization output" : "path is too long");
        return false;
    }
    memcpy(dst, start, len);
    dst[len] = '\0';
    return true;
}

static bool agent_parse_two_line_paths(const agent_buf *out,
                                       char first[PATH_MAX],
                                       char second[PATH_MAX],
                                       char *err, size_t err_len) {
    if (!out || !out->ptr || out->len == 0) {
        snprintf(err, err_len, "docker path normalization produced no output");
        return false;
    }
    const char *p = out->ptr;
    const char *end = out->ptr + out->len;
    const char *nl = memchr(p, '\n', (size_t)(end - p));
    if (!nl) {
        snprintf(err, err_len, "docker path normalization produced one line");
        return false;
    }
    if (!agent_copy_line(first, PATH_MAX, p, nl, err, err_len))
        return false;
    p = nl + 1;
    nl = memchr(p, '\n', (size_t)(end - p));
    if (!nl) nl = end;
    return agent_copy_line(second, PATH_MAX, p, nl, err, err_len);
}

static bool agent_resolve_docker_candidate(agent_worker *w, const char *base,
                                           const char *path,
                                           agent_path_resolution mode,
                                           char out[PATH_MAX],
                                           char root_out[PATH_MAX],
                                           char *err, size_t err_len) {
    char candidate[PATH_MAX];
    if (!agent_join_path(candidate, sizeof(candidate), base, path, err, err_len))
        return false;

    const char *mode_arg = mode == AGENT_PATH_PARENT ? "parent" : "existing";
    const char *script =
        "ds4_resolve_path() { "
        "case \"$mode\" in "
        "existing) "
        "  resolved=$(realpath -- \"$target\" 2>/dev/null) || "
        "    { printf 'resolve %s: No such file or directory\\n' \"$target\" >&2; return 1; }; "
        "  if [ -d \"$resolved\" ]; then root=$resolved; "
        "  else root=$(dirname -- \"$resolved\") || return 1; fi; "
        "  printf '%s\\n%s\\n' \"$resolved\" \"$root\"; "
        "  ;; "
        "parent) "
        "  if resolved=$(realpath -- \"$target\" 2>/dev/null); then "
        "    if [ -d \"$resolved\" ]; then root=$resolved; "
        "    else root=$(dirname -- \"$resolved\") || return 1; fi; "
        "    printf '%s\\n%s\\n' \"$resolved\" \"$root\"; return 0; "
        "  fi; "
        "  leaf=$(basename -- \"$target\") || return 1; "
        "  case \"$leaf\" in ''|'.'|'..') printf 'invalid path: %s\\n' \"$target\" >&2; return 1;; esac; "
        "  parent=$(dirname -- \"$target\") || return 1; "
        "  root=$(realpath -- \"$parent\" 2>/dev/null) || "
        "    { printf 'resolve parent for %s: No such file or directory\\n' \"$target\" >&2; return 1; }; "
        "  if [ \"$root\" = / ]; then resolved=/$leaf; else resolved=$root/$leaf; fi; "
        "  printf '%s\\n%s\\n' \"$resolved\" \"$root\"; "
        "  ;; "
        "*) printf 'invalid path resolution mode\\n' >&2; return 1;; "
        "esac; "
        "}; ds4_resolve_path; ds4_rc=$?; unset -f ds4_resolve_path; "
        "(exit \"$ds4_rc\")";
    agent_buf cmd = {0};
    agent_buf_puts(&cmd, "mode=");
    agent_buf_append_shell_quoted(&cmd, mode_arg);
    agent_buf_puts(&cmd, "; target=");
    agent_buf_append_shell_quoted(&cmd, candidate);
    agent_buf_puts(&cmd, "; ");
    agent_buf_puts(&cmd, script);
    agent_buf stdout_out = {0};
    int status = 0;
    bool ok = agent_docker_shell_exec(w, cmd.ptr, false, "docker path resolve",
                                      &stdout_out, &status);
    free(cmd.ptr);
    if (!ok || status != 0) {
        if (stdout_out.ptr && stdout_out.ptr[0]) {
            agent_trim_trailing_whitespace(stdout_out.ptr);
            snprintf(err, err_len, "%s", stdout_out.ptr);
        } else {
            snprintf(err, err_len, "docker path resolve failed");
        }
        free(stdout_out.ptr);
        return false;
    }
    ok = agent_parse_two_line_paths(&stdout_out, out, root_out, err, err_len);
    free(stdout_out.ptr);
    return ok;
}

static bool agent_request_working_directory(agent_worker *w,
                                            const char *path,
                                            const char *dir,
                                            char *err, size_t err_len) {
    if (!w || !w->cfg || w->cfg->non_interactive) {
        snprintf(err, err_len,
                 "path is outside configured working directories: %s", path);
        return false;
    }

    pthread_mutex_lock(&w->mu);
    w->path_approval_pending = true;
    w->path_approval_answered = false;
    w->path_approval_result = false;
    w->path_approval_option_count = 0;
    w->path_approval_choice[0] = '\0';
    w->path_approval_error[0] = '\0';
    snprintf(w->path_approval_message, sizeof(w->path_approval_message),
             "Tool path %s is outside the current working directories. "
             "Choose a directory to allow, or deny:", path);
    char opt[PATH_MAX];
    snprintf(opt, sizeof(opt), "%s", dir);
    for (int i = 0; i < 3 && opt[0]; i++) {
        bool dup = false;
        for (int j = 0; j < w->path_approval_option_count; j++) {
            if (!strcmp(w->path_approval_options[j], opt)) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            snprintf(w->path_approval_options[w->path_approval_option_count++],
                     PATH_MAX, "%s", opt);
        }
        char parent[PATH_MAX];
        if (!agent_path_parent(opt, parent) || !strcmp(parent, opt)) break;
        snprintf(opt, sizeof(opt), "%s", parent);
    }
    agent_wake_locked(w);
    while (!w->stop && !w->interrupt && !w->path_approval_answered)
        pthread_cond_wait(&w->cond, &w->mu);
    bool ok = w->path_approval_result;
    if (!w->path_approval_answered && (w->stop || w->interrupt)) {
        ok = false;
        w->path_approval_pending = false;
        snprintf(w->path_approval_error, sizeof(w->path_approval_error),
                 "interrupted");
    }
    if (!ok) {
        snprintf(err, err_len, "%s",
                 w->path_approval_error[0] ? w->path_approval_error :
                 "user denied working directory add");
    }
    pthread_mutex_unlock(&w->mu);
    if (!ok) return false;

    const char *chosen = w->path_approval_choice[0] ? w->path_approval_choice : dir;
    /* Tool execution is worker-owned, so root checks and root-list mutation are
     * serialized here.  The UI thread only supplies the selected approval
     * scope or deny result above. */
    if (!agent_path_list_contains(&w->working_directories, chosen))
        agent_path_list_append(&w->working_directories, chosen);
    return true;
}

static char *agent_resolve_tool_path(agent_worker *w, const char *path,
                                     agent_path_resolution mode,
                                     char *err, size_t err_len) {
    if (!path || !path[0]) {
        snprintf(err, err_len, "path is empty");
        return NULL;
    }
    const agent_path_list *roots = agent_working_directories(w);
    if (!roots) return xstrdup(path);
    const char *base = agent_primary_working_directory(w);
    char resolved[PATH_MAX];
    char requested_dir[PATH_MAX];
    bool docker_fs = agent_tool_use_docker_filesystem(w);
    bool ok = false;
    if (docker_fs) {
        ok = agent_resolve_docker_candidate(w, base, path, mode, resolved,
                                            requested_dir, err, err_len);
    } else {
        ok = mode == AGENT_PATH_PARENT ?
            agent_resolve_parent_candidate(base, path, resolved, requested_dir,
                                           err, err_len) :
            agent_resolve_existing_candidate(base, path, resolved, requested_dir,
                                             err, err_len);
    }
    if (!ok) return NULL;
    /* Auto-allowed paths (e.g. files created by the agent's own tools) bypass
     * workspace root checks.  If the file no longer exists on disk the entry
     * is stale — remove it and fall through to normal workspace-root checks. */
    if (agent_path_list_contains(&w->auto_allowed_paths, resolved)) {
        if (docker_fs || access(resolved, F_OK) == 0)
            return xstrdup(resolved);
        /* Stale entry — file was deleted externally.  Remove it. */
        agent_path_list_remove(&w->auto_allowed_paths, resolved, false);
    }
    if (agent_path_is_under_any_root(roots, resolved))
        return xstrdup(resolved);
    if (!agent_request_working_directory(w, path, requested_dir, err, err_len))
        return NULL;
    if (!agent_path_is_under_any_root(agent_working_directories(w), resolved)) {
        snprintf(err, err_len, "path is still outside working directories: %s",
                 path);
        return NULL;
    }
    return xstrdup(resolved);
}

static int agent_read_file_bytes(const char *path, char **data, size_t *len,
                                 char *err, size_t errlen) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        snprintf(err, errlen, "open %s: %s", path, strerror(errno));
        return -1;
    }
    char *buf = NULL;
    size_t used = 0, cap = 0;
    char tmp[8192];
    while (true) {
        size_t n = fread(tmp, 1, sizeof(tmp), fp);
        if (n) {
            if (used + n > AGENT_FILE_MAX_BYTES) {
                fclose(fp);
                free(buf);
                snprintf(err, errlen, "file too large: %s exceeds %d bytes",
                         path, AGENT_FILE_MAX_BYTES);
                return -1;
            }
            if (used + n + 1 > cap) {
                cap = cap ? cap * 2 : 8192;
                while (cap < used + n + 1) cap *= 2;
                buf = xrealloc(buf, cap);
            }
            memcpy(buf + used, tmp, n);
            used += n;
            buf[used] = '\0';
        }
        if (n < sizeof(tmp)) {
            if (ferror(fp)) {
                snprintf(err, errlen, "read %s: %s", path, strerror(errno));
                fclose(fp);
                free(buf);
                return -1;
            }
            break;
        }
    }
    fclose(fp);
    if (!buf) buf = xstrdup("");
    *data = buf;
    *len = used;
    return 0;
}

static int agent_line_for_offset(const agent_line_spans *spans, size_t offset) {
    if (!spans || spans->len <= 0) return 1;
    for (int i = 0; i < spans->len; i++) {
        if (offset < spans->v[i].end) return i + 1;
    }
    return spans->len;
}

static bool agent_old_new_line_effect(const char *old_data, size_t old_len,
                                      const char *new_data, size_t new_len,
                                      size_t edit_offset, size_t replaced_len,
                                      int *start_line, int *end_line,
                                      int *delta) {
    agent_line_spans old_spans = {0};
    agent_line_spans new_spans = {0};
    agent_split_lines(old_data, old_len, &old_spans);
    agent_split_lines(new_data, new_len, &new_spans);
    bool ok = old_spans.len > 0;
    if (ok) {
        size_t old_last = edit_offset;
        if (replaced_len > 0) old_last = edit_offset + replaced_len - 1;
        if (old_last >= old_len) old_last = old_len ? old_len - 1 : 0;
        if (start_line) *start_line = agent_line_for_offset(&old_spans, edit_offset);
        if (end_line) *end_line = agent_line_for_offset(&old_spans, old_last);
        if (delta) *delta = new_spans.len - old_spans.len;
    }
    agent_line_spans_free(&old_spans);
    agent_line_spans_free(&new_spans);
    return ok;
}

static void agent_edit_result_append_context(agent_buf *b,
                                             const char *path,
                                             const char *data, size_t len,
                                             int anchor_start,
                                             int anchor_end);

static char *agent_edit_result(const char *path,
                                       int start_line, int end_line, int delta,
                                       const char *new_data, size_t new_len,
                                       const char *kind) {
    agent_buf b = {0};
    char msg[PATH_MAX + 180];
    snprintf(msg, sizeof(msg), "Edited %s using %s\n", path, kind);
    agent_buf_puts(&b, msg);
    if (start_line > 0 && end_line >= start_line) {
        snprintf(msg, sizeof(msg),
                 "Touched old lines %d-%d; current post-edit context follows.\n",
                 start_line, end_line);
        agent_buf_puts(&b, msg);
        if (delta != 0) {
            snprintf(msg, sizeof(msg),
                     "Line shift: old lines after %d moved by %+d (old line %d is now line %d). Re-read before relying on old line numbers there.\n",
                     end_line, delta, end_line + 1, end_line + 1 + delta);
            agent_buf_puts(&b, msg);
        }
    }
    if (start_line > 0 && end_line >= start_line) {
        int new_anchor_end = end_line + delta;
        if (new_anchor_end < start_line) new_anchor_end = start_line;
        agent_edit_result_append_context(&b, path, new_data, new_len,
                                         start_line, new_anchor_end);
    }
    return agent_buf_take(&b);
}

static void agent_worker_set_more(agent_worker *w, const char *path,
                                  int next_line, bool bare) {
    snprintf(w->more_path, sizeof(w->more_path), "%s", path ? path : "");
    w->more_next_line = next_line;
    w->more_bare = bare;
    w->more_valid = path && path[0] && next_line > 0;
}

/* What: choose the workspace path Docker-backed tools should treat as cwd.
 * Why: worker status snapshots can lag initialization, but the configured
 * working-directory roots are available immediately and are the source of
 * truth for relative tool paths and Docker `-w`.
 * Callers: agent_docker_shell_start(); unit tests cover the primary-root
 * fallback directly. */
static const char *agent_docker_effective_working_directory(
        const agent_worker *w) {
    const char *working_dir = agent_primary_working_directory(w);
    if (working_dir && working_dir[0]) return working_dir;
    if (w && w->status.workspace[0]) return w->status.workspace;
    return NULL;
}

static void agent_buf_trim_one_trailing_newline(agent_buf *b) {
    if (!b || !b->ptr || b->len == 0) return;
    if (b->ptr[b->len - 1] == '\n') {
        b->len--;
        b->ptr[b->len] = '\0';
    }
}

static void agent_tool_append_stderr_warning(agent_buf *b,
                                             const agent_buf *stderr_out) {
    if (!b || !stderr_out || !stderr_out->ptr || stderr_out->len == 0) return;
    if (b->len > 0 && b->ptr[b->len - 1] != '\n') agent_buf_puts(b, "\n");
    agent_buf_puts(b, "Tool stderr (warnings):\n");
    agent_buf_append(b, stderr_out->ptr, stderr_out->len);
    if (b->len == 0 || b->ptr[b->len - 1] != '\n') agent_buf_puts(b, "\n");
}

static char *agent_tool_error_with_stderr(const char *fallback,
                                          const agent_buf *stderr_out) {
    agent_buf b = {0};
    agent_buf_puts(&b, "Tool error: ");
    if (stderr_out && stderr_out->ptr && stderr_out->len > 0)
        agent_buf_append(&b, stderr_out->ptr, stderr_out->len);
    else
        agent_buf_puts(&b, fallback ? fallback : "command failed");
    if (b.len == 0 || b.ptr[b.len - 1] != '\n') agent_buf_puts(&b, "\n");
    return agent_buf_take(&b);
}

static bool agent_tool_use_docker_filesystem(const agent_worker *w);
static bool agent_docker_exec(agent_worker *w,
                               const agent_config *cfg,
                               const char *const argv_tail[],
                               const char *stdin_data,
                               size_t stdin_len,
                               bool unbounded_output,
                               const char *op,
                               agent_buf *out,
                               int *exit_status);
static bool agent_docker_capture(const agent_config *cfg,
                                  const char *docker_command, char *const argv[],
                                  const char *op, agent_buf *out);
static void agent_config_prepare_startup_docker_sandbox_with_streams(agent_config *cfg,
                                                                     FILE *input,
                                                                     FILE *output);
static void agent_config_prepare_startup_docker_sandbox(agent_config *cfg);
static bool agent_docker_refresh_mounts(agent_worker *w,
                                        char *err, size_t err_len);
static void agent_docker_mount_fingerprint(agent_worker *w,
                                           char *out, size_t out_len);
static void agent_bash_publish_observation(agent_worker *w, const char *obs);
static agent_bash_job *agent_bash_start_mode(agent_worker *w, const char *cmd,
                                             int timeout_sec, bool use_docker,
                                             char *err, size_t err_len);
static char *agent_bash_job_tool_result(agent_worker *w, agent_bash_job *job,
                                        bool wait, int refresh_sec,
                                        bool stop, bool remove_if_done);
static char *agent_docker_debug_command_text(char *const argv[]);
static void agent_docker_build_shell_argv(char **argv, int *argc,
                                          const char *docker_command,
                                          const char *container,
                                          const char *working_dir,
                                          const char *temp_dir,
                                          const char *cmd,
                                          char *home_env, size_t home_len,
                                          char *tmpdir_env, size_t tmpdir_len,
                                          char *tmp_env, size_t tmp_len,
                                          char *temp_env, size_t temp_len,
                                          char *xdg_config_env, size_t xdg_config_len,
                                          char *xdg_cache_env, size_t xdg_cache_len);
static bool agent_docker_read_file_bytes(agent_worker *w, const char *path,
                                         char **data, size_t *len,
                                         char *err, size_t err_len,
                                         agent_buf *stderr_out);
static char *agent_docker_read_range(agent_worker *w, const char *path,
                                     int start_line, int max_lines,
                                     bool bare, bool set_more,
                                     agent_buf *stderr_out);
static bool agent_docker_write_file_bytes(agent_worker *w, const char *path,
                                          const char *data, size_t len,
                                          char *err, size_t err_len,
                                          agent_buf *stderr_out);
static bool agent_docker_mkdir_p(agent_worker *w, const char *path,
                                  char *err, size_t err_len);
#ifdef DS4_AGENT_TEST
static bool agent_docker_shell_exec_argv(agent_worker *w,
                                          char *const cmd_argv[],
                                           bool unbounded_output,
                                          const char *op,
                                          agent_buf *out,
                                          int *exit_status);
#endif
static bool agent_docker_shell_exec_argv_stderr(agent_worker *w,
                                                char *const cmd_argv[],
                                                bool unbounded_output,
                                                const char *op,
                                                agent_buf *stdout_out,
                                                agent_buf *stderr_out,
                                                int *exit_status);
static char *agent_tool_edit(agent_worker *w, const agent_tool_call *call);
static char *agent_tool_list(agent_worker *w, const agent_tool_call *call);
static char *agent_tool_search(agent_worker *w, const agent_tool_call *call);
static char *agent_tool_mkdir(agent_worker *w, const agent_tool_call *call);

static bool agent_tool_result_fits_context(agent_worker *w, const char *result,
                                           int reserve_tokens,
                                           int *tokens_out) {
    ds4_tokens tmp = {0};
    ds4_tokens_copy(&tmp, &w->transcript);
    ds4_chat_append_message(w->engine, &tmp, "tool", result ? result : "");
    int tokens = tmp.len;
    ds4_tokens_free(&tmp);
    if (tokens_out) *tokens_out = tokens;
    return tokens + reserve_tokens < w->cfg->gen.ctx_size;
}

/* What: read a numbered line range from a file inside the active Docker
 * sandbox using the persistent shell and return the normal read-tool payload.
 * Why: range reads should happen in the sandbox filesystem without copying the
 * whole file to the host, and the persistent shell avoids process startup cost.
 * Callers: agent_read_range() for non-whole-file Docker reads; the agent test
 * harness exercises it through test_agent_docker_read_range_uses_shell_slice(). */
static char *agent_docker_read_range(agent_worker *w, const char *path,
                                     int start_line, int max_lines,
                                     bool bare, bool set_more,
                                     agent_buf *stderr_out) {
    if (!path || !path[0]) return xstrdup("Tool error: read requires path\n");
    if (start_line < 1) start_line = 1;
    if (max_lines <= 0) max_lines = AGENT_READ_DEFAULT_LINES;

    char start_arg[32];
    char count_arg[32];
    snprintf(start_arg, sizeof(start_arg), "start=%d", start_line);
    snprintf(count_arg, sizeof(count_arg), "count=%d", max_lines);
    char *argv[] = {
        "awk",
        "-v", start_arg,
        "-v", count_arg,
        "BEGIN { end = start + count - 1 } "
        "NR >= start && NR <= end { printf(\"__DS4_LINE__%d\\t%s\\n\", NR, $0) } "
        "END { printf(\"__DS4_TOTAL__%d\\n\", NR) }",
        (char *)path,
        NULL,
    };
    agent_buf shell_out = {0};
    agent_buf shell_err = {0};
    int status = 0;
    if (!agent_docker_shell_exec_argv_stderr(w, argv, true, "docker read",
                                             &shell_out, &shell_err,
                                             &status) || status != 0) {
        char *msg = agent_tool_error_with_stderr("docker read failed",
                                                 &shell_err);
        free(shell_out.ptr);
        free(shell_err.ptr);
        return msg;
    }

    agent_buf lines = {0};
    int total = -1;
    int last_line = 0;
    int emitted = 0;
    agent_line_spans spans = {0};
    agent_split_lines(shell_out.ptr ? shell_out.ptr : "", shell_out.len, &spans);
    for (int i = 0; i < spans.len; i++) {
        agent_line_span sp = spans.v[i];
        const char *row = shell_out.ptr + sp.start;
        size_t row_len = sp.content_end - sp.start;
        const char total_prefix[] = "__DS4_TOTAL__";
        const char line_prefix[] = "__DS4_LINE__";
        if (row_len >= sizeof(total_prefix) - 1 &&
            !memcmp(row, total_prefix, sizeof(total_prefix) - 1)) {
            char tmp[32];
            size_t n = row_len - (sizeof(total_prefix) - 1);
            if (n >= sizeof(tmp)) n = sizeof(tmp) - 1;
            memcpy(tmp, row + sizeof(total_prefix) - 1, n);
            tmp[n] = '\0';
            total = atoi(tmp);
            continue;
        }
        if (row_len < sizeof(line_prefix) ||
            memcmp(row, line_prefix, sizeof(line_prefix) - 1))
            continue;
        const char *num_start = row + sizeof(line_prefix) - 1;
        const char *tab = memchr(num_start, '\t',
                                 (size_t)(row + row_len - num_start));
        if (!tab) continue;
        char tmp[32];
        size_t n = (size_t)(tab - num_start);
        if (n >= sizeof(tmp)) n = sizeof(tmp) - 1;
        memcpy(tmp, num_start, n);
        tmp[n] = '\0';
        int line_no = atoi(tmp);
        const char *content = tab + 1;
        size_t content_len = (size_t)(row + row_len - content);
        last_line = line_no;
        emitted++;
        if (bare) {
            agent_buf_append(&lines, content, content_len);
            agent_buf_puts(&lines, "\n");
        } else {
            char prefix[64];
            snprintf(prefix, sizeof(prefix), "%d ", line_no);
            agent_buf_puts(&lines, prefix);
            agent_buf_append(&lines, content, content_len);
            agent_buf_puts(&lines, "\n");
        }
    }
    agent_line_spans_free(&spans);
    free(shell_out.ptr);

    if (total < 0) {
        free(lines.ptr);
        return xstrdup("Tool error: docker read failed: malformed range output\n");
    }

    int start_idx = start_line - 1;
    if (start_idx > total) start_idx = total;
    int end_idx = start_idx + emitted;
    if (end_idx > total) end_idx = total;
    bool truncated = end_idx < total;

    agent_buf out = {0};
    if (bare) {
        if (lines.ptr) agent_buf_append(&out, lines.ptr, lines.len);
        if (truncated) {
            char note[160];
            snprintf(note, sizeof(note),
                     "[Read truncated at line %d of %d. continue_offset=%d. "
                     "Call more with count=%d to read the next chunk.]\n",
                     end_idx, total, end_idx + 1, max_lines);
            agent_buf_puts(&out, note);
        }
    } else {
        char hdr[PATH_MAX + 160];
        int display_start = total ? start_idx + 1 : 0;
        int display_end = emitted ? last_line : end_idx;
        if (truncated) {
            snprintf(hdr, sizeof(hdr),
                     "%s: lines %d-%d of %d; continue_offset=%d; "
                     "call more with count=%d to read the next chunk\n",
                     path, display_start, display_end, total,
                     end_idx + 1, max_lines);
        } else {
            snprintf(hdr, sizeof(hdr), "%s: lines %d-%d of %d\n",
                     path, display_start, display_end, total);
        }
        agent_buf_puts(&out, hdr);
        if (lines.ptr) agent_buf_append(&out, lines.ptr, lines.len);
    }
    free(lines.ptr);
    if (stderr_out) *stderr_out = shell_err;
    else free(shell_err.ptr);
    if (set_more) {
        if (truncated) agent_worker_set_more(w, path, end_idx + 1, bare);
        else agent_worker_set_more(w, NULL, 0, false);
    }
    return agent_buf_take(&out);
}

/* Read file text for the model.  Normal mode shows plain line numbers.  Raw
 * mode is reserved for cases where line decoration would corrupt the payload
 * being inspected. */
static char *agent_read_range(agent_worker *w, const char *path, int start_line,
                              int max_lines, bool whole_file, bool bare,
                              bool set_more) {
    char err[256];
    char *data = NULL;
    size_t len = 0;
    const char *display_path = path;
    if (!path || !path[0]) return xstrdup("Tool error: read requires path\n");
    char *file_path = NULL;
    agent_buf stderr_out = {0};
    if (agent_tool_use_docker_filesystem(w) && !whole_file) {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        char *result = agent_docker_read_range(w, file_path, start_line,
                                               max_lines, bare, set_more,
                                               &stderr_out);
        if (result && strncmp(result, "Tool error:", 11) != 0)
            agent_temp_files_note_read(w, file_path);
        free(file_path);
        if (stderr_out.ptr && result && strncmp(result, "Tool error:", 11) != 0) {
            agent_buf b = {0};
            agent_buf_puts(&b, result);
            free(result);
            agent_tool_append_stderr_warning(&b, &stderr_out);
            free(stderr_out.ptr);
            return agent_buf_take(&b);
        }
        free(stderr_out.ptr);
        return result;
    } else if (agent_tool_use_docker_filesystem(w)) {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        if (!agent_docker_read_file_bytes(w, file_path, &data, &len, err,
                                          sizeof(err), &stderr_out)) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_read(w, file_path);
    } else {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        if (agent_read_file_bytes(file_path, &data, &len, err, sizeof(err)) != 0) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_read(w, file_path);
    }

    if (!data) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, "read failed");
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }

    agent_line_spans spans = {0};
    agent_split_lines(data, len, &spans);
    if (start_line < 1) start_line = 1;
    int start_idx = start_line - 1;
    if (start_idx > spans.len) start_idx = spans.len;
    if (whole_file) {
        max_lines = spans.len - start_idx;
    } else {
        if (max_lines <= 0) max_lines = AGENT_READ_DEFAULT_LINES;
    }
    int end_idx = start_idx + max_lines;
    if (end_idx > spans.len) end_idx = spans.len;

    agent_buf out = {0};
    if (bare) {
        size_t start = start_idx < spans.len ? spans.v[start_idx].start : len;
        size_t end = end_idx > start_idx ? spans.v[end_idx - 1].end : start;
        agent_buf_append(&out, data + start, end - start);
        if (end > start && out.ptr[out.len - 1] != '\n') agent_buf_puts(&out, "\n");
        if (end_idx < spans.len) {
            char note[160];
            snprintf(note, sizeof(note),
                     "[Read truncated at line %d of %d. continue_offset=%d. "
                     "Call more with count=%d to read the next chunk.]\n",
                     end_idx, spans.len, end_idx + 1,
                     max_lines > 0 ? max_lines : AGENT_READ_DEFAULT_LINES);
            agent_buf_puts(&out, note);
        }
    } else {
        char hdr[PATH_MAX + 160];
        if (end_idx < spans.len) {
            snprintf(hdr, sizeof(hdr),
                     "%s: lines %d-%d of %d; continue_offset=%d; "
                     "call more with count=%d to read the next chunk\n",
                     display_path, spans.len ? start_idx + 1 : 0, end_idx, spans.len,
                     end_idx + 1, max_lines > 0 ? max_lines : AGENT_READ_DEFAULT_LINES);
        } else {
            snprintf(hdr, sizeof(hdr), "%s: lines %d-%d of %d\n",
                     display_path, spans.len ? start_idx + 1 : 0, end_idx, spans.len);
        }
        agent_buf_puts(&out, hdr);
        for (int i = start_idx; i < end_idx; i++) {
            agent_line_span sp = spans.v[i];
            char prefix[64];
            snprintf(prefix, sizeof(prefix), "%d ", i + 1);
            agent_buf_puts(&out, prefix);
            agent_buf_append(&out, data + sp.start, sp.content_end - sp.start);
            agent_buf_puts(&out, "\n");
        }
    }
    if (set_more) {
        if (end_idx < spans.len) agent_worker_set_more(w, display_path, end_idx + 1, bare);
        else agent_worker_set_more(w, NULL, 0, false);
    }
    agent_line_spans_free(&spans);
    free(data);
    free(file_path);
    agent_tool_append_stderr_warning(&out, &stderr_out);
    free(stderr_out.ptr);
    return agent_buf_take(&out);
}

static char *agent_tool_read(agent_worker *w, const agent_tool_call *call) {
    const char *path = agent_tool_arg_value(call, "path");
    bool whole = agent_parse_bool_default(agent_tool_arg_value(call, "whole"), false);
    const char *start_s = agent_tool_arg_value(call, "start_line");
    if (!start_s) start_s = agent_tool_arg_value(call, "offset");
    int start = agent_parse_int_default(start_s, 1, 1, INT_MAX);
    const char *count_s = agent_tool_arg_value(call, "max_lines");
    if (!count_s) count_s = agent_tool_arg_value(call, "count");
    int count = agent_parse_int_default(count_s,
                                        AGENT_READ_DEFAULT_LINES, 1, INT_MAX);
    bool raw = agent_parse_bool_default(agent_tool_arg_value(call, "raw"), false);
    return agent_read_range(w, path, start, count, whole, raw, true);
}

static char *agent_tool_more(agent_worker *w, const agent_tool_call *call) {
    int count = agent_parse_int_default(agent_tool_arg_value(call, "count"),
                                        AGENT_READ_DEFAULT_LINES, 1, INT_MAX);
    if (!w->more_valid) return xstrdup("Tool error: no previous output to continue\n");
    return agent_read_range(w, w->more_path, w->more_next_line, count, false,
                            w->more_bare, true);
}

static char *agent_tool_write(agent_worker *w, const agent_tool_call *call) {
    const char *path = agent_tool_arg_value(call, "path");
    const char *content = agent_tool_arg_value(call, "content");
    if (!path || !path[0]) return xstrdup("Tool error: write requires path\n");
    if (!content) return xstrdup("Tool error: write requires content\n");
    char err[256];
    size_t len = strlen(content);
    char *file_path = NULL;
    const char *display_path = path;
    agent_buf stderr_out = {0};
    if (agent_tool_use_docker_filesystem(w)) {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_PARENT,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        if (!agent_docker_write_file_bytes(w, file_path, content, len, err,
                                           sizeof(err), &stderr_out)) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: write failed: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_write(w, file_path);
    } else {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_PARENT,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        FILE *fp = fopen(file_path, "wb");
        if (!fp) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: open for write failed: ");
            agent_buf_puts(&b, strerror(errno));
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        size_t wr = fwrite(content, 1, len, fp);
        int close_rc = fclose(fp);
        if (wr != len || close_rc != 0) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: write failed: ");
            agent_buf_puts(&b, strerror(errno));
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_write(w, file_path);
    }
    char msg[PATH_MAX + 160];
    snprintf(msg, sizeof(msg), "Wrote %zu bytes to %s\n", len, display_path);
    agent_buf result = {0};
    agent_buf_puts(&result, msg);
    agent_tool_append_stderr_warning(&result, &stderr_out);
    free(stderr_out.ptr);
    free(file_path);
    return agent_buf_take(&result);
}

static char *agent_tool_mkdir(agent_worker *w, const agent_tool_call *call) {
    const char *path = agent_tool_arg_value(call, "path");
    if (!path || !path[0]) return xstrdup("Tool error: mkdir requires path\n");
    char err[256];
    char *resolved = agent_resolve_tool_path(w, path, AGENT_PATH_PARENT,
                                             err, sizeof(err));
    if (!resolved) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    const char *display_path = resolved;
    bool ok = false;
    if (agent_tool_use_docker_filesystem(w)) {
        ok = agent_docker_mkdir_p(w, resolved, err, sizeof(err));
    } else {
        ok = agent_mkdir_p(resolved);
    }
    if (!ok) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: mkdir failed");
        if (!agent_tool_use_docker_filesystem(w)) {
            agent_buf_puts(&b, ": ");
            agent_buf_puts(&b, strerror(errno));
        }
        agent_buf_puts(&b, "\n");
        free(resolved);
        return agent_buf_take(&b);
    }
    if (!agent_path_list_contains(&w->auto_allowed_paths, resolved))
        agent_path_list_append(&w->auto_allowed_paths, resolved);
    char msg[PATH_MAX + 64];
    snprintf(msg, sizeof(msg), "Created directory %s\n", display_path);
    free(resolved);
    return xstrdup(msg);
}

static char *agent_tool_list(agent_worker *w, const agent_tool_call *call) {
    const char *path = agent_tool_arg_value(call, "path");
    if (!path || !path[0]) path = ".";
    if (agent_tool_use_docker_filesystem(w)) {
        char err[256];
        char *dir_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                                 err, sizeof(err));
        if (!dir_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        char *argv[] = {
            "/bin/sh", "-c",
            "[ -e \"$1\" ] || { printf 'No such file or directory'; exit 1; }; "
            "[ -d \"$1\" ] || { printf 'Not a directory'; exit 1; }; "
            "find \"$1\" -mindepth 1 -maxdepth 1 -printf '%y %s %f\\n' | "
            "sort | sed -n '1,300p'",
            "sh", dir_path, NULL
        };
        agent_buf out = {0};
        agent_buf stderr_out = {0};
        int status = 0;
        char hdr[PATH_MAX + 64];
        snprintf(hdr, sizeof(hdr), "%s:\n", dir_path);
        if (!agent_docker_shell_exec_argv_stderr(w, argv, false,
                                                 "docker list", &out,
                                                 &stderr_out, &status) ||
            status != 0) {
            char *msg = agent_tool_error_with_stderr("docker list failed",
                                                     &stderr_out);
            free(out.ptr);
            free(stderr_out.ptr);
            free(dir_path);
            return msg;
        }
        agent_buf result = {0};
        agent_buf_puts(&result, hdr);
        if (out.ptr && out.ptr[0]) agent_buf_append(&result, out.ptr, out.len);
        agent_tool_append_stderr_warning(&result, &stderr_out);
        free(out.ptr);
        free(stderr_out.ptr);
        free(dir_path);
        return agent_buf_take(&result);
    }
    char err[256];
    char *dir_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                             err, sizeof(err));
    if (!dir_path) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    DIR *dir = opendir(dir_path);
    if (!dir) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: opendir failed: ");
        agent_buf_puts(&b, strerror(errno));
        agent_buf_puts(&b, "\n");
        free(dir_path);
        return agent_buf_take(&b);
    }
    agent_buf out = {0};
    char hdr[PATH_MAX + 64];
    snprintf(hdr, sizeof(hdr), "%s:\n", dir_path);
    agent_buf_puts(&out, hdr);
    struct dirent *de;
    int shown = 0;
    while ((de = readdir(dir)) != NULL && shown < 300) {
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dir_path, de->d_name);
        struct stat st;
        if (lstat(full, &st) != 0) continue;
        char type = S_ISDIR(st.st_mode) ? 'd' :
                    S_ISLNK(st.st_mode) ? 'l' :
                    S_ISREG(st.st_mode) ? '-' : '?';
        char line[PATH_MAX + 96];
        snprintf(line, sizeof(line), "%c %10lld %s%s\n", type,
                 (long long)st.st_size, de->d_name, S_ISDIR(st.st_mode) ? "/" : "");
        agent_buf_puts(&out, line);
        shown++;
    }
    if (de) agent_buf_puts(&out, "... more entries omitted ...\n");
    closedir(dir);
    free(dir_path);
    return agent_buf_take(&out);
}

/* ============================================================================
 * Edit And Search Tools
 * ============================================================================
 */

static int agent_write_file_bytes(const char *path, const char *data, size_t len,
                                  char *err, size_t errlen) {
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        snprintf(err, errlen, "open %s: %s", path, strerror(errno));
        return -1;
    }
    size_t wr = fwrite(data, 1, len, fp);
    if (wr != len) {
        snprintf(err, errlen, "write %s: %s", path, strerror(errno));
        fclose(fp);
        return -1;
    }
    if (fclose(fp) != 0) {
        snprintf(err, errlen, "close %s: %s", path, strerror(errno));
        return -1;
    }
    return 0;
}

static void agent_edit_result_append_line(agent_buf *b, const char *data,
                                          const agent_line_span *sp,
                                          int line) {
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "%d ", line);
    agent_buf_puts(b, prefix);
    agent_buf_append(b, data + sp->start, sp->content_end - sp->start);
    agent_buf_puts(b, "\n");
}

/* Successful edits return the nearby post-edit file shape.  This spends cheap
 * prefill tokens to save expensive model retries: the model immediately sees
 * shifted line numbers, braces, semicolons, and accidental duplication. */
static void agent_edit_result_append_context(agent_buf *b,
                                             const char *path,
                                             const char *data, size_t len,
                                             int anchor_start,
                                             int anchor_end) {
    enum {
        CONTEXT_BEFORE = 5,
        CONTEXT_AFTER = 8,
        EDITED_CONTEXT_HEAD = 18,
        EDITED_CONTEXT_TAIL = 18
    };

    agent_line_spans spans = {0};
    agent_split_lines(data, len, &spans);
    if (spans.len <= 0) {
        agent_line_spans_free(&spans);
        return;
    }

    if (anchor_start < 1) anchor_start = 1;
    if (anchor_start > spans.len) anchor_start = spans.len;
    if (anchor_end < anchor_start) anchor_end = anchor_start;
    if (anchor_end > spans.len) anchor_end = spans.len;

    int ctx_start = anchor_start - CONTEXT_BEFORE;
    if (ctx_start < 1) ctx_start = 1;
    int ctx_end = anchor_end + CONTEXT_AFTER;
    if (ctx_end > spans.len) ctx_end = spans.len;

    char hdr[PATH_MAX + 160];
    snprintf(hdr, sizeof(hdr),
             "Current file around edit: %s lines %d-%d of %d\n",
             path, ctx_start, ctx_end, spans.len);
    agent_buf_puts(b, hdr);

    int edited_lines = anchor_end - anchor_start + 1;
    if (edited_lines <= EDITED_CONTEXT_HEAD + EDITED_CONTEXT_TAIL) {
        for (int line = ctx_start; line <= ctx_end; line++)
            agent_edit_result_append_line(b, data, &spans.v[line - 1], line);
    } else {
        int head_end = anchor_start + EDITED_CONTEXT_HEAD - 1;
        int tail_start = anchor_end - EDITED_CONTEXT_TAIL + 1;
        for (int line = ctx_start; line <= head_end; line++)
            agent_edit_result_append_line(b, data, &spans.v[line - 1], line);
        snprintf(hdr, sizeof(hdr),
                 "... %d edited lines omitted ...\n",
                 tail_start - head_end - 1);
        agent_buf_puts(b, hdr);
        for (int line = tail_start; line <= ctx_end; line++)
            agent_edit_result_append_line(b, data, &spans.v[line - 1], line);
    }

    agent_line_spans_free(&spans);
}

static const char *agent_memmem_simple(const char *hay, size_t hay_len,
                                       const char *needle, size_t needle_len) {
    if (!needle_len) return hay;
    if (needle_len > hay_len) return NULL;
    size_t last = hay_len - needle_len;
    for (size_t i = 0; i <= last; i++) {
        if (hay[i] == needle[0] && !memcmp(hay + i, needle, needle_len))
            return hay + i;
    }
    return NULL;
}

static bool agent_find_unique(const char *data, size_t len,
                              const char *needle, size_t needle_len,
                              const char **match, const char *label,
                              char *err, size_t err_len) {
    if (!needle || needle_len == 0) {
        snprintf(err, err_len, "%s anchor is empty", label);
        return false;
    }
    const char *first = agent_memmem_simple(data, len, needle, needle_len);
    if (!first) {
        snprintf(err, err_len, "%s anchor not found", label);
        return false;
    }
    size_t after_first = (size_t)(first - data) + 1;
    const char *second = after_first <= len ?
        agent_memmem_simple(data + after_first, len - after_first,
                            needle, needle_len) : NULL;
    if (second) {
        snprintf(err, err_len, "%s anchor is not unique", label);
        return false;
    }
    *match = first;
    return true;
}

/* Find an anchor only in the suffix after start.
 *
 * Anchored edits use "head [upto] tail": the head fixes the edit start, and
 * the tail should delimit the first unique end point after that start. A tail
 * may legitimately appear earlier in the file, so checking global uniqueness
 * would reject valid edits.
 */
static bool agent_find_unique_after(const char *data, size_t len,
                                    const char *start,
                                    const char *needle, size_t needle_len,
                                    const char **match, const char *label,
                                    char *err, size_t err_len) {
    if (!needle || needle_len == 0) {
        snprintf(err, err_len, "%s anchor is empty", label);
        return false;
    }
    if (start < data || start > data + len) {
        snprintf(err, err_len, "%s search starts outside file", label);
        return false;
    }
    size_t off = (size_t)(start - data);
    const char *first = agent_memmem_simple(data + off, len - off,
                                            needle, needle_len);
    if (!first) {
        snprintf(err, err_len, "%s anchor not found after old head", label);
        return false;
    }
    size_t after_first = (size_t)(first - data) + 1;
    const char *second = after_first <= len ?
        agent_memmem_simple(data + after_first, len - after_first,
                            needle, needle_len) : NULL;
    if (second) {
        snprintf(err, err_len, "%s anchor is not unique after old head", label);
        return false;
    }
    *match = first;
    return true;
}

static bool agent_edit_old_may_be_closing_tag(const char *text, size_t len) {
    size_t i = 0;
    while (i < len && (text[i] == ' ' || text[i] == '\t' ||
                       text[i] == '\r' || text[i] == '\n'))
        i++;
    return i < len && text[i] == '<';
}

static bool agent_edit_old_is_only_space(const char *text, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (text[i] != ' ' && text[i] != '\t' &&
            text[i] != '\r' && text[i] != '\n')
            return false;
    }
    return true;
}

static bool agent_span_has_nonspace(const char *s, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (!isspace((unsigned char)s[i])) return true;
    }
    return false;
}

static bool agent_edit_old_prefix_mature_for_upto(const char *old, size_t old_len) {
    if (old_len < AGENT_EDIT_UPTO_MIN_PREFIX_BYTES) return false;
    int nonempty_lines = 0;
    bool line_has_text = false;
    for (size_t i = 0; i < old_len; i++) {
        if (old[i] == '\n') {
            if (line_has_text) nonempty_lines++;
            line_has_text = false;
        } else if (!isspace((unsigned char)old[i])) {
            line_has_text = true;
        }
    }
    return nonempty_lines >= AGENT_EDIT_UPTO_MIN_PREFIX_LINES;
}

static bool agent_edit_old_ready_for_upto(const char *old, size_t old_len) {
    if (!old_len || strstr(old, "[upto]")) return false;
    if (!agent_edit_old_prefix_mature_for_upto(old, old_len)) return false;
    size_t end = old_len;
    while (end > 0 && (old[end - 1] == ' ' || old[end - 1] == '\t' ||
                       old[end - 1] == '\r'))
        end--;
    return end > 0 && old[end - 1] == '\n';
}

/* While the model streams an edit old=... argument, stop it from retyping a
 * large exact old block once the emitted prefix is already a unique file
 * anchor.  The next sampled token is inspected before eval: if it would keep
 * writing old text rather than close the parameter, the caller evaluates a
 * complete "[upto]" marker line instead. */
static bool agent_edit_upto_forcer_should_replace(agent_worker *w,
                                                  agent_edit_upto_forcer *forcer,
                                                  agent_dsml_parser *p,
                                                  const char *next_text,
                                                  size_t next_len) {
    if (!forcer || !p) return false;
    bool in_edit_old = p->state == AGENT_DSML_PARAM_VALUE &&
        p->current.name && strcmp(p->current.name, "edit") == 0 &&
        p->param_name && strcmp(p->param_name, "old") == 0;
    if (!in_edit_old) {
        forcer->active = false;
        forcer->done = false;
        return false;
    }
    if (!forcer->active) {
        forcer->active = true;
        forcer->done = false;
    }
    if (forcer->done)
        return false;
    if (agent_edit_old_may_be_closing_tag(next_text, next_len) ||
        agent_edit_old_is_only_space(next_text, next_len))
        return false;

    const char *path = agent_tool_arg_value(&p->current, "path");
    if (!path || !path[0] || p->param_value_start > p->raw_len) return false;

    const char *old = p->raw + p->param_value_start;
    size_t old_len = p->raw_len - p->param_value_start;
    if (!agent_edit_old_ready_for_upto(old, old_len)) return false;

    char err[256];
    char *data = NULL;
    size_t len = 0;
    char *file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                              err, sizeof(err));
    if (!file_path)
        return false;
    if (agent_read_file_bytes(file_path, &data, &len, err, sizeof(err)) != 0) {
        free(file_path);
        return false;
    }

    const char *match = NULL;
    bool unique = agent_find_unique(data, len, old, old_len, &match,
                                    "old prefix", err, sizeof(err));
    free(file_path);
    free(data);
    if (unique) {
        forcer->done = true;
        return true;
    }
    return false;
}

static const char *agent_edit_find_upto_marker(const char *text) {
    static const char marker[] = "[upto]";
    size_t marker_len = strlen(marker);
    const char *p = text;
    while ((p = strstr(p, marker)) != NULL) {
        const char *after = p + marker_len;
        const char *line = p;
        while (line > text && line[-1] != '\n' && line[-1] != '\r')
            line--;
        const char *q = line;
        while (q < p && (*q == ' ' || *q == '\t'))
            q++;
        bool left_boundary = q == p;
        q = after;
        while (*q == ' ' || *q == '\t')
            q++;
        bool right_boundary = !*q || *q == '\n' || *q == '\r';
        if (left_boundary && right_boundary)
            return p;
        p = after;
    }
    return NULL;
}

static bool agent_edit_find_old_span(const char *data, size_t len,
                                     const char *old, const char **match,
                                     size_t *match_len, bool *anchored,
                                     agent_edit_anchor_span *anchor_span,
                                     char *err, size_t err_len) {
    static const char marker[] = "[upto]";
    size_t old_len = strlen(old);
    if (anchor_span)
        memset(anchor_span, 0, sizeof(*anchor_span));
    const char *upto = agent_edit_find_upto_marker(old);
    if (!upto) {
        *anchored = false;
        if (!agent_find_unique(data, len, old, old_len, match, "old text",
                               err, err_len))
            return false;
        *match_len = old_len;
        return true;
    }
    if (agent_edit_find_upto_marker(upto + strlen(marker))) {
        snprintf(err, err_len, "old text contains more than one [upto] marker");
        return false;
    }
    size_t head_len = (size_t)(upto - old);
    const char *tail = upto + strlen(marker);
    size_t tail_len = old_len - head_len - strlen(marker);
    /* Strip leading newline/CR from tail before searching.  The head already
     * includes the newline at its end, so the extra \n that follows [upto] in
     * the old text (whether injected by the forcer or written by the model)
     * must not be part of the tail needle -- the file after the head has no
     * duplicate newline. */
    while (tail_len > 0 && (*tail == '\n' || *tail == '\r')) {
        tail++;
        tail_len--;
    }
    if (!agent_span_has_nonspace(tail, tail_len)) {
        snprintf(err, err_len,
                 "old text after [upto] must include a unique tail anchor");
        return false;
    }
    const char *head_pos = NULL;
    const char *tail_pos = NULL;
    if (!agent_find_unique(data, len, old, head_len, &head_pos, "old head",
                           err, err_len))
        return false;
    if (!agent_find_unique_after(data, len, head_pos + head_len,
                                 tail, tail_len, &tail_pos, "old tail",
                                 err, err_len))
        return false;
    *anchored = true;
    *match = head_pos;
    *match_len = (size_t)(tail_pos - head_pos) + tail_len;
    if (anchor_span) {
        anchor_span->active = true;
        anchor_span->head_len = head_len;
        anchor_span->tail_offset = (size_t)(tail_pos - head_pos);
        anchor_span->tail_len = tail_len;
    }
    return true;
}

static char *agent_edit_resolve_new_text(const char *data, size_t len,
                                         const char *match, size_t match_len,
                                         const agent_edit_anchor_span *span,
                                         const char *new_text,
                                         bool *used_upto,
                                         char *err, size_t err_len) {
    static const char marker[] = "[upto]";
    if (used_upto)
        *used_upto = false;
    if (!new_text)
        new_text = "";
    if (!span || !span->active)
        return xstrdup(new_text);

    const char *upto = agent_edit_find_upto_marker(new_text);
    if (!upto)
        return xstrdup(new_text);
    if (agent_edit_find_upto_marker(upto + strlen(marker))) {
        snprintf(err, err_len, "new text contains more than one [upto] marker");
        return NULL;
    }
    if (!data || !match || match < data || match > data + len ||
        match_len > (size_t)((data + len) - match))
    {
        snprintf(err, err_len, "anchored old match is outside file");
        return NULL;
    }
    if (span->head_len > span->tail_offset ||
        span->tail_offset > match_len ||
        span->tail_len > match_len - span->tail_offset)
    {
        snprintf(err, err_len, "anchored old match span is invalid");
        return NULL;
    }

    size_t new_len = strlen(new_text);
    size_t new_head_len = (size_t)(upto - new_text);
    const char *new_tail = upto + strlen(marker);
    size_t new_tail_len = new_len - new_head_len - strlen(marker);
    while (new_tail_len > 0 && (*new_tail == '\n' || *new_tail == '\r')) {
        new_tail++;
        new_tail_len--;
    }

    size_t middle_len = span->tail_offset - span->head_len;
    size_t out_len = new_head_len + middle_len + new_tail_len;
    char *out = xmalloc(out_len + 1);
    memcpy(out, new_text, new_head_len);
    memcpy(out + new_head_len, match + span->head_len, middle_len);
    memcpy(out + new_head_len + middle_len, new_tail, new_tail_len);
    out[out_len] = '\0';
    if (used_upto)
        *used_upto = true;
    return out;
}

/* ============================================================================
 * Persistent Docker Shell (agent_docker_shell)
 *
 * A worker-owned persistent shell session for synchronous Docker filesystem
 * utility commands.  The shell is started once and reused across multiple
 * exec calls, using a sentinel-based protocol to delimit command output.
 * ============================================================================
 */

/* Forward declarations for docker helper functions used by the shell. */
static void agent_docker_exec_add_env(char **argv, int *argc,
                                      char *buf, size_t len,
                                      const char *key, const char *value);
static void agent_docker_exec_add_standard_env(char **argv, int *argc,
                                               char *home_env, size_t home_len,
                                               char *tmpdir_env, size_t tmpdir_len,
                                               char *tmp_env, size_t tmp_len,
                                               char *temp_env, size_t temp_len,
                                               char *xdg_config_env, size_t xdg_config_len,
                                               char *xdg_cache_env, size_t xdg_cache_len,
                                               const char *working_dir,
                                               const char *temp_dir);
static void agent_buf_append_shell_quoted(agent_buf *b, const char *s);

/* What: start and cache a worker-owned `docker exec -i ... /bin/sh` session.
 * Why: file tools issue many small filesystem commands; reusing one shell keeps
 * reads, list/search, and edit helpers fast while preserving sandbox env parity.
 * Callers: agent_worker_init(), agent_command_docker_create(), and
 * agent_command_docker_use(); tests call it directly for startup failure cases. */
static bool agent_docker_shell_start(agent_worker *w) {
    if (!w || !w->cfg || !agent_bash_use_docker_sandbox(w))
        return false;
    if (w->docker_shell.active)
        return true;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    const char *temp_dir = w->cfg->temp_directory[0] ?
        w->cfg->temp_directory : NULL;
    const char *working_dir = agent_docker_effective_working_directory(w);

    char home_env[PATH_MAX + 16];
    char tmpdir_env[PATH_MAX + 16];
    char tmp_env[PATH_MAX + 16];
    char temp_env[PATH_MAX + 16];
    char xdg_config_env[PATH_MAX + 32];
    char xdg_cache_env[PATH_MAX + 32];
    char *argv[48];
    int argc = 0;
    argv[argc++] = (char *)docker_command;
    argv[argc++] = "exec";
    argv[argc++] = "-i";
    if (working_dir && working_dir[0]) {
        argv[argc++] = "-w";
        argv[argc++] = (char *)working_dir;
    }
    agent_docker_exec_add_standard_env(argv, &argc,
                                       home_env, sizeof(home_env),
                                       tmpdir_env, sizeof(tmpdir_env),
                                       tmp_env, sizeof(tmp_env),
                                       temp_env, sizeof(temp_env),
                                       xdg_config_env, sizeof(xdg_config_env),
                                       xdg_cache_env, sizeof(xdg_cache_env),
                                       working_dir, temp_dir);
    argv[argc++] = (char *)w->cfg->docker_container;
    argv[argc++] = "/bin/sh";
    argv[argc] = NULL;

    int stdin_pipe[2];
    int stdout_pipe[2];
    if (pipe(stdin_pipe) != 0) return false;
    if (pipe(stdout_pipe) != 0) {
        close(stdin_pipe[0]);
        close(stdin_pipe[1]);
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        close(stdin_pipe[0]);
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        close(stdout_pipe[1]);
        return false;
    }

    if (pid == 0) {
        /* Child: attach stdin/stdout to pipes and exec docker. */
        close(stdin_pipe[1]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[0]);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdout_pipe[1]);
        execvp(docker_command, argv);
        _exit(127);
    }

    /* Parent: close the child's ends. */
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    w->docker_shell.stdin_fd = stdin_pipe[1];
    w->docker_shell.stdout_fd = stdout_pipe[0];
    w->docker_shell.pid = pid;
    w->docker_shell.seq = 0;
    w->docker_shell.active = true;
    return true;
}

/* What: append a chunk of persistent-shell command output to the caller buffer.
 * Why: the persistent Docker shell is an internal transport for file tools; its
 * stdout often contains parser protocol such as resolved paths or line markers
 * and should not be mirrored by /command_output.
 * Callers: agent_docker_shell_exec() while draining command output before the
 * sentinel line or after EOF without a sentinel. */
static void agent_docker_shell_emit_output(agent_worker *w, agent_buf *out,
                                           const char *s, size_t n,
                                           bool unbounded_output) {
    (void)w;
    if (!s || !n) return;
    if (!out) return;
    if (unbounded_output)
        agent_buf_append_full(out, s, n);
    else
        agent_buf_append(out, s, n);
}

/* What: find a possible sentinel marker only when it appears at a line start.
 * Why: command output may contain sentinel-like text; requiring a line boundary
 * prevents ordinary file content from terminating a Docker shell command.
 * Callers: agent_docker_shell_scan_sentinel() while walking pending output. */
static const char *agent_docker_shell_find_sentinel_candidate(
        const char *data, size_t len,
        const char *sentinel_colon, size_t colon_len) {
    if (!data || !sentinel_colon || !colon_len) return NULL;
    size_t off = 0;
    while (off + colon_len <= len) {
        const char *s = agent_memmem_simple(data + off, len - off,
                                            sentinel_colon, colon_len);
        if (!s) return NULL;
        if (s == data || s[-1] == '\n') return s;
        off = (size_t)(s - data) + 1;
    }
    return NULL;
}

typedef struct {
    const char *complete;
    const char *incomplete;
    int rc;
} agent_docker_shell_sentinel_scan;

/* What: walk pending shell output and classify sentinel candidates.
 * Why: output may contain malformed sentinel-like lines before the actual
 * command terminator; the reader must loop past bogus candidates while holding
 * only a real split sentinel in pending output.
 * Callers: agent_docker_shell_find_complete_sentinel() and
 * agent_docker_shell_exec() when deciding whether to stop, emit, or retain
 * pending bytes. */
static agent_docker_shell_sentinel_scan agent_docker_shell_scan_sentinel(
        const char *data, size_t len,
        const char *sentinel_colon, size_t colon_len) {
    agent_docker_shell_sentinel_scan scan = {0};
    scan.rc = -1;
    if (!data || !sentinel_colon || !colon_len) return scan;

    size_t off = 0;
    while (off + colon_len <= len) {
        const char *slice = data + off;
        const char *s = agent_docker_shell_find_sentinel_candidate(
            slice, len - off, sentinel_colon, colon_len);
        if (!s) return scan;

        size_t start = off + (size_t)(s - slice);
        s = data + start;
        if (start > 0 && data[start - 1] != '\n') {
            off = start + 1;
            continue;
        }

        size_t pos = start + colon_len;
        size_t digits = pos;
        while (digits < len && isdigit((unsigned char)data[digits]))
            digits++;

        if (digits == pos) {
            if (pos == len) {
                scan.incomplete = s;
                return scan;
            }
            off = start + 1;
            continue;
        }

        if (digits == len) {
            scan.incomplete = s;
            return scan;
        }

        if (data[digits] != '\n') {
            off = start + 1;
            continue;
        }

        scan.complete = s;
        scan.rc = atoi(s + colon_len);
        return scan;
    }

    return scan;
}

/* What: parse a complete sentinel line and return the start of that line plus
 * the shell command exit code, optionally reporting a split sentinel candidate.
 * Why: persistent shell output arrives in arbitrary pipe chunks, so the reader
 * must loop through candidates and wait for sentinel, digits, and newline
 * before ending the command.
 * Callers: agent_docker_shell_exec(); the split-sentinel unit test also calls
 * it directly. */
static const char *agent_docker_shell_find_complete_sentinel(
        const char *data, size_t len,
        const char *sentinel_colon, size_t colon_len,
        int *rc_out,
        const char **incomplete_out) {
    agent_docker_shell_sentinel_scan scan =
        agent_docker_shell_scan_sentinel(data, len, sentinel_colon, colon_len);
    if (incomplete_out) *incomplete_out = scan.incomplete;
    if (!scan.complete) return NULL;
    if (rc_out) *rc_out = scan.rc;
    return scan.complete;
}

/* What: return how many trailing bytes might be the prefix of the sentinel.
 * Why: when a read chunk ends mid-sentinel, those bytes must stay pending
 * instead of being emitted as file output; the prefix must begin at a line
 * boundary so ordinary output that merely ends with sentinel text can flush.
 * Callers: agent_docker_shell_exec(); the split-sentinel unit test calls it. */
static size_t agent_docker_shell_sentinel_tail_len(const char *data, size_t len,
                                                   const char *sentinel,
                                                   size_t sentinel_len) {
    if (!data || !len || !sentinel || sentinel_len <= 1) return 0;
    size_t max = len < sentinel_len - 1 ? len : sentinel_len - 1;
    for (size_t n = max; n > 0; n--) {
        const char *start = data + len - n;
        if (start != data && start[-1] != '\n') continue;
        if (!memcmp(start, sentinel, n)) return n;
    }
    return 0;
}

static void agent_buf_discard_prefix(agent_buf *b, size_t n) {
    if (!b || n == 0) return;
    if (n >= b->len) {
        b->len = 0;
        if (b->ptr) b->ptr[0] = '\0';
        return;
    }
    memmove(b->ptr, b->ptr + n, b->len - n);
    b->len -= n;
    b->ptr[b->len] = '\0';
}

/* What: run one shell command through the persistent Docker shell and capture
 * output until this command's unique sentinel line appears.
 * Why: read/list/search/edit need synchronous sandbox commands without starting
 * a fresh Docker process each time, and the sentinel protocol separates command
 * output from shell lifetime.
 * Callers: agent_docker_shell_exec_argv() for argv-shaped commands; tests call
 * it directly for parser and unbounded-output coverage. */
static bool agent_docker_shell_exec(agent_worker *w,
                                    const char *cmd,
                                     bool unbounded_output,
                                    const char *op,
                                    agent_buf *out,
                                    int *exit_status) {
    (void)op;
    if (out) memset(out, 0, sizeof(*out));
    if (exit_status) *exit_status = -1;
    if (!w || !w->docker_shell.active || !cmd || !cmd[0])
        return false;

    if (w->cfg && w->cfg->docker_debug) {
        const char *prefix = "\x1b[96m[docker debug] ";
        const char *suffix = "\x1b[0m\n\n";
        agent_publish(w, prefix, strlen(prefix));
        agent_publish(w, cmd, strlen(cmd));
        agent_publish(w, suffix, strlen(suffix));
    }

    pthread_mutex_lock(&w->docker_shell.mu);

    /* Generate a unique sentinel per call. */
    unsigned long long seq = w->docker_shell.seq++;
    char sentinel[64];
    int sentinel_len = snprintf(sentinel, sizeof(sentinel),
                                "__DS4_DONE_%d_%llu__",
                                (int)w->docker_shell.pid, seq);
    if (sentinel_len < 0) sentinel_len = 0;

    /* Build the sentinel-with-colon pattern: __DS4_DONE_<pid>_<seq>__: */
    char sentinel_colon[80];
    snprintf(sentinel_colon, sizeof(sentinel_colon),
             "__DS4_DONE_%d_%llu__:", (int)w->docker_shell.pid, seq);
    size_t colon_len = strlen(sentinel_colon);

    /* Write: { <cmd>; } 2>&1; rc=$?; printf '\n__DS4_DONE_<sentinel>__:%d\n' "$rc" */
    agent_buf write_buf = {0};
    agent_buf_puts(&write_buf, "{ ");
    agent_buf_puts(&write_buf, cmd);
    agent_buf_puts(&write_buf, "; } 2>&1; rc=$?; printf '\\n");
    agent_buf_puts(&write_buf, sentinel_colon);
    agent_buf_puts(&write_buf, "%d\\n' \"$rc\"\n");
    write_all(w->docker_shell.stdin_fd, write_buf.ptr, write_buf.len);
    free(write_buf.ptr);

    /* Read until we see the sentinel: line.  We search for sentinel_colon
     * and verify it appears at the start of a line (preceded by \n or at
     * position 0 of a chunk after a line boundary). */
    bool saw_sentinel = false;
    int rc = -1;
    agent_buf pending = {0};

    for (;;) {
        char chunk[4096];
        ssize_t n = read(w->docker_shell.stdout_fd, chunk, sizeof(chunk));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        agent_buf_append_full(&pending, chunk, (size_t)n);

        const char *incomplete = NULL;
        const char *s = agent_docker_shell_find_complete_sentinel(
            pending.ptr, pending.len, sentinel_colon, colon_len, &rc,
            &incomplete);
        if (s) {
            size_t before = (size_t)(s - pending.ptr);
            agent_docker_shell_emit_output(w, out, pending.ptr, before,
                                           unbounded_output);
            saw_sentinel = true;
            break;
        }

        size_t keep = incomplete ?
            pending.len - (size_t)(incomplete - pending.ptr) :
            agent_docker_shell_sentinel_tail_len(pending.ptr, pending.len,
                                                 sentinel_colon, colon_len);
        if (pending.len > keep) {
            size_t emit = pending.len - keep;
            agent_docker_shell_emit_output(w, out, pending.ptr, emit,
                                           unbounded_output);
            agent_buf_discard_prefix(&pending, emit);
        }
    }

    if (!saw_sentinel && pending.len > 0)
        agent_docker_shell_emit_output(w, out, pending.ptr, pending.len,
                                       unbounded_output);
    free(pending.ptr);

    /* Strip trailing newline from output if present. */
    if (out && out->len > 0 && out->ptr[out->len - 1] == '\n')
        out->len--;
    if (out && out->len > 0)
        out->ptr[out->len] = '\0';

    if (exit_status) *exit_status = rc;
    pthread_mutex_unlock(&w->docker_shell.mu);
    return saw_sentinel;
}

/* What: shell-quote an argv vector and execute it through the persistent Docker
 * shell.
 * Why: tests still exercise the plain argv-to-shell quoting path directly even
 * though production tools now route through the stderr-capturing wrapper.
 * Callers: DS4_AGENT_TEST-only quoting and debug-output tests. */
#ifdef DS4_AGENT_TEST
static bool agent_docker_shell_exec_argv(agent_worker *w,
                                         char *const cmd_argv[],
                                         bool unbounded_output,
                                         const char *op,
                                         agent_buf *out,
                                         int *exit_status) {
    (void)op;
    if (!cmd_argv || !cmd_argv[0])
        return false;

    /* Build command string by shell-quoting each argv element. */
    agent_buf b = {0};
    for (int i = 0; cmd_argv[i]; i++) {
        if (i > 0) agent_buf_puts(&b, " ");
        agent_buf_append_shell_quoted(&b, cmd_argv[i]);
    }
    bool ok = agent_docker_shell_exec(w, b.ptr, unbounded_output, op, out, exit_status);
    free(b.ptr);
    return ok;
}
#endif

static bool agent_docker_shell_exec_argv_stderr(agent_worker *w,
                                                char *const cmd_argv[],
                                                bool unbounded_output,
                                                const char *op,
                                                agent_buf *stdout_out,
                                                agent_buf *stderr_out,
                                                int *exit_status) {
    if (stdout_out) memset(stdout_out, 0, sizeof(*stdout_out));
    if (stderr_out) memset(stderr_out, 0, sizeof(*stderr_out));
    if (exit_status) *exit_status = -1;
    if (!cmd_argv || !cmd_argv[0]) return false;

    agent_buf cmd = {0};
    for (int i = 0; cmd_argv[i]; i++) {
        if (i > 0) agent_buf_puts(&cmd, " ");
        agent_buf_append_shell_quoted(&cmd, cmd_argv[i]);
    }

    unsigned long long tag_seq =
        (unsigned long long)(w ? w->docker_shell.seq : 0ULL);
    int tag_pid = w ? (int)w->docker_shell.pid : 0;
    char begin_tag[96];
    char end_tag[96];
    snprintf(begin_tag, sizeof(begin_tag), "__DS4_STDERR_BEGIN_%d_%llu__",
             tag_pid, tag_seq);
    snprintf(end_tag, sizeof(end_tag), "__DS4_STDERR_END_%d_%llu__",
             tag_pid, tag_seq);

    char label[64];
    size_t li = 0;
    const char *src = (op && op[0]) ? op : "tool";
    while (*src && li + 1 < sizeof(label)) {
        unsigned char ch = (unsigned char)*src++;
        if (isalnum(ch)) label[li++] = (char)tolower(ch);
        else if (li == 0 || label[li - 1] != '_') label[li++] = '_';
    }
    if (li == 0) label[li++] = 't';
    label[li] = '\0';

    const char *temp_dir =
        (w && w->cfg && w->cfg->temp_directory[0]) ?
        w->cfg->temp_directory : "/tmp";
    char err_path[PATH_MAX];
    snprintf(err_path, sizeof(err_path), "%s/ds4_%s_%d_%llu.err",
             temp_dir, label, tag_pid, tag_seq);

    agent_buf wrapped = {0};
    agent_buf_puts(&wrapped, "err_path=");
    agent_buf_append_shell_quoted(&wrapped, err_path);
    agent_buf_puts(&wrapped, "; rm -f \"$err_path\"; { ");
    agent_buf_append(&wrapped, cmd.ptr, cmd.len);
    agent_buf_puts(&wrapped,
                   "; } 2>\"$err_path\"; cmd_rc=$?; "
                   "if [ -s \"$err_path\" ]; then "
                   "printf '\\n");
    agent_buf_puts(&wrapped, begin_tag);
    agent_buf_puts(&wrapped, "\\n'; "
                   "cat \"$err_path\"; "
                   "printf '\\n");
    agent_buf_puts(&wrapped, end_tag);
    agent_buf_puts(&wrapped, "\\n'; "
                   "fi; rm -f \"$err_path\"; (exit \"$cmd_rc\")");

    agent_buf combined = {0};
    int status = 0;
    bool ok = agent_docker_shell_exec(w, wrapped.ptr, unbounded_output, op,
                                      &combined, &status);
    free(cmd.ptr);
    free(wrapped.ptr);
    if (exit_status) *exit_status = status;
    if (!ok) {
        if (stdout_out) *stdout_out = combined;
        else free(combined.ptr);
        return false;
    }

    const char *begin = combined.ptr ? strstr(combined.ptr, begin_tag) : NULL;
    const char *end = begin && combined.ptr ? strstr(begin, end_tag) : NULL;
    if (!begin || !end) {
        if (stdout_out) *stdout_out = combined;
        else free(combined.ptr);
        return true;
    }

    const char *stdout_end = begin;
    if (stdout_end > combined.ptr && stdout_end[-1] == '\n') stdout_end--;
    if (stdout_out)
        agent_buf_append(stdout_out, combined.ptr,
                         (size_t)(stdout_end - combined.ptr));

    const char *stderr_start = begin + strlen(begin_tag);
    if (*stderr_start == '\n') stderr_start++;
    const char *stderr_end = end;
    if (stderr_end > stderr_start && stderr_end[-1] == '\n') stderr_end--;
    if (stderr_out)
        agent_buf_append(stderr_out, stderr_start,
                         (size_t)(stderr_end - stderr_start));

    agent_buf_trim_one_trailing_newline(stdout_out);
    agent_buf_trim_one_trailing_newline(stderr_out);
    free(combined.ptr);
    return true;
}

/* What: terminate and clear the worker's persistent Docker shell process.
 * Why: switching, stopping, destroying, or freeing a sandbox must not leave a
 * shell connected to the old container or stale pipe descriptors.
 * Callers: agent_command_docker_create(), agent_command_docker_use(),
 * agent_command_docker_stop(), agent_worker_free(), and the idempotence test. */
static void agent_docker_shell_stop(agent_worker *w) {
    if (!w) return;
    if (w->docker_shell.active && w->docker_shell.pid > 0) {
        kill(w->docker_shell.pid, SIGTERM);
        int i;
        for (i = 0; i < 50; i++) {
            if (waitpid(w->docker_shell.pid, NULL, WNOHANG) == w->docker_shell.pid)
                break;
            usleep(10000);
        }
        if (i >= 50) kill(w->docker_shell.pid, SIGKILL);
        waitpid(w->docker_shell.pid, NULL, 0);
    }
    if (w->docker_shell.stdin_fd >= 0) {
        close(w->docker_shell.stdin_fd);
    }
    if (w->docker_shell.stdout_fd >= 0) {
        close(w->docker_shell.stdout_fd);
    }
    w->docker_shell.stdin_fd = -1;
    w->docker_shell.stdout_fd = -1;
    w->docker_shell.pid = -1;
    w->docker_shell.active = false;
}

#ifdef DS4_AGENT_TEST
int agent_test_failures;

typedef struct {
    char docker_path[PATH_MAX];
    char names_path[PATH_MAX];
    char state_path[PATH_MAX];
    char start_fail_path[PATH_MAX];
} agent_fake_docker_env;

static void agent_test_write_text_file(const char *path, const char *text) {
    char err[256];
    AGENT_TEST_ASSERT(agent_write_file_bytes(path, text ? text : "",
                                             strlen(text ? text : ""),
                                             err, sizeof(err)) == 0);
}

static void agent_test_fake_docker_cleanup(agent_fake_docker_env *env) {
    if (!env) return;
    unlink(env->docker_path);
    unlink(env->names_path);
    unlink(env->state_path);
    unlink(env->start_fail_path);
}

static void agent_test_fake_docker_setup(agent_fake_docker_env *env,
                                         const char *names_text,
                                         const char *state_text,
                                         bool fail_start) {
    memset(env, 0, sizeof(*env));
    snprintf(env->docker_path, sizeof(env->docker_path),
             "%s", "/tmp/ds4-agent-fake-docker-XXXXXX");
    snprintf(env->names_path, sizeof(env->names_path),
             "%s", "/tmp/ds4-agent-fake-docker-names-XXXXXX");
    snprintf(env->state_path, sizeof(env->state_path),
             "%s", "/tmp/ds4-agent-fake-docker-state-XXXXXX");
    snprintf(env->start_fail_path, sizeof(env->start_fail_path),
             "%s", "/tmp/ds4-agent-fake-docker-start-fail-XXXXXX");
    int docker_fd = mkstemp(env->docker_path);
    int names_fd = mkstemp(env->names_path);
    int state_fd = mkstemp(env->state_path);
    int fail_fd = mkstemp(env->start_fail_path);
    AGENT_TEST_ASSERT(docker_fd >= 0);
    AGENT_TEST_ASSERT(names_fd >= 0);
    AGENT_TEST_ASSERT(state_fd >= 0);
    AGENT_TEST_ASSERT(fail_fd >= 0);
    close(names_fd);
    close(state_fd);
    close(fail_fd);
    if (!fail_start) unlink(env->start_fail_path);

    agent_test_write_text_file(env->names_path, names_text ? names_text : "");
    agent_test_write_text_file(env->state_path, state_text ? state_text : "running\n");

    char script[4096];
    snprintf(script, sizeof(script),
             "#!/bin/sh\n"
             "names_file=%s\n"
             "state_file=%s\n"
             "start_fail_file=%s\n"
             "cmd=\"$1\"\n"
             "shift\n"
             "case \"$cmd\" in\n"
             "  ps)\n"
             "    cat \"$names_file\"\n"
             "    ;;\n"
             "  inspect)\n"
             "    while [ \"$#\" -gt 1 ]; do shift; done\n"
             "    name=\"$1\"\n"
             "    grep -Fx \"$name\" \"$names_file\" >/dev/null 2>&1 || exit 1\n"
             "    state=$(tr -d '\\r\\n' < \"$state_file\")\n"
             "    printf '/%%s\\t{\"ds4:sandbox\":\"1\"}\\timage\\t%%s\\t172.17.0.2 \\n' \"$name\" \"$state\"\n"
             "    ;;\n"
             "  start)\n"
             "    name=\"$1\"\n"
             "    grep -Fx \"$name\" \"$names_file\" >/dev/null 2>&1 || exit 1\n"
             "    if [ -f \"$start_fail_file\" ]; then\n"
             "      echo start failed >&2\n"
             "      exit 1\n"
             "    fi\n"
             "    printf 'running\\n' > \"$state_file\"\n"
             "    printf '%%s\\n' \"$name\"\n"
             "    ;;\n"
             "  version)\n"
             "    printf 'fake\\n'\n"
             "    ;;\n"
             "  *)\n"
             "    echo unsupported >&2\n"
             "    exit 1\n"
             "    ;;\n"
             "esac\n",
             env->names_path, env->state_path, env->start_fail_path);
    write_all(docker_fd, script, strlen(script));
    close(docker_fd);
    AGENT_TEST_ASSERT(chmod(env->docker_path, 0700) == 0);
}

static bool agent_worker_remove_workspace(agent_worker *w, const char *path,
                                          char *removed, size_t removed_len,
                                          char *err, size_t err_len);
static void worker_set_think_mode(agent_worker *w, ds4_think_mode mode);
static ds4_think_mode worker_cycle_think_mode(agent_worker *w);
static void build_footer_text(const agent_status *st, ds4_agent_subagents *subagents,
                               const agent_prompt_queue *queue, int cols,
                               char *buf, size_t len);
static size_t agent_display_width(const char *s);
static void agent_format_badge(const ds4_agent_subagent_status *item,
                                char *visible, size_t visible_len,
                                char *styled, size_t styled_len);
static bool linenoise_take_queued_sequence(struct linenoiseState *l,
                                           const char *seq, size_t seq_len);
static bool linenoise_take_queued_alt_tab(struct linenoiseState *l);
static bool linenoise_queued_alt_tab_prefix_pending(struct linenoiseState *l);

void agent_test_assert(bool cond, const char *expr,
                       const char *file, int line) {
    if (cond) return;
    fprintf(stderr, "%s:%d: assertion failed: %s\n", file, line, expr);
    agent_test_failures++;
}

static void test_agent_edit_upto_tail_newline_is_not_part_of_anchor(void) {
    const char *data =
        "CFLAGS = -Wall -Wextra -g\n"
        "LDFLAGS =\n"
        "\n"
        "all: bc\n"
        "\n"
        "bc: main.c\n"
        "\t$(CC) $(CFLAGS) -o bc main.c $(LDFLAGS)\n"
        "\n"
        "clean:\n"
        "\trm -f bc\n";
    const char *old =
        "CFLAGS = -Wall -Wextra -g\n"
        "LDFLAGS =\n"
        "\n"
        "all: bc\n"
        "\n"
        "bc: main.c\n"
        "\t$(CC) $(CFLAGS) -o bc main.c $(LDFLAGS)\n"
        "\n"
        "[upto]\n"
        "clean:\n";

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_edit_find_old_span(data, strlen(data), old,
                                              &match, &match_len, &anchored,
                                              NULL, err, sizeof(err)));
    AGENT_TEST_ASSERT(anchored);
    AGENT_TEST_ASSERT(match == data);
    AGENT_TEST_ASSERT(match_len == strlen(data) - strlen("\trm -f bc\n"));
}

static void test_agent_edit_upto_requires_tail_after_newline_strip(void) {
    const char *data = "head\nbody\ntail\n";
    const char *old = "head\n[upto]\n";
    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    char err[128] = {0};

    AGENT_TEST_ASSERT(!agent_edit_find_old_span(data, strlen(data), old,
                                               &match, &match_len, &anchored,
                                               NULL, err, sizeof(err)));
    AGENT_TEST_ASSERT(strstr(err, "must include a unique tail anchor") != NULL);
}

static void test_agent_edit_new_upto_preserves_omitted_middle(void) {
    const char *data =
        "static bool before(void);\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";
    const char *old =
        "static bool before(void);\n"
        "\n"
        "[upto]\n"
        "static bool after(void);\n";
    const char *new_text =
        "static bool before(void);\n"
        "static bool added(void);\n"
        "\n"
        "[upto]\n"
        "static bool after(void);\n";
    const char *expected =
        "static bool before(void);\n"
        "static bool added(void);\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    bool used_upto = false;
    agent_edit_anchor_span span = {0};
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_edit_find_old_span(data, strlen(data), old,
                                              &match, &match_len, &anchored,
                                              &span, err, sizeof(err)));
    AGENT_TEST_ASSERT(anchored);
    AGENT_TEST_ASSERT(span.active);
    char *resolved = agent_edit_resolve_new_text(data, strlen(data),
                                                 match, match_len, &span,
                                                 new_text, &used_upto,
                                                 err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL);
    if (resolved) {
        AGENT_TEST_ASSERT(used_upto);
        AGENT_TEST_ASSERT(strstr(resolved, "[upto]") == NULL);
        AGENT_TEST_ASSERT(strcmp(resolved, expected) == 0);
        free(resolved);
    }
}

static void test_agent_edit_upto_accepts_indented_marker_line(void) {
    const char *data =
        "static void outer(void) {\n"
        "    call_before();\n"
        "    keep_one();\n"
        "    keep_two();\n"
        "    call_after();\n"
        "}\n";
    const char *old =
        "static void outer(void) {\n"
        "    call_before();\n"
        "    [upto]\n"
        "    call_after();\n";
    const char *new_text =
        "static void outer(void) {\n"
        "    call_before();\n"
        "    call_added();\n"
        "    [upto]\n"
        "    call_after();\n";
    const char *expected =
        "static void outer(void) {\n"
        "    call_before();\n"
        "    call_added();\n"
        "    keep_one();\n"
        "    keep_two();\n"
        "    call_after();\n";

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    bool used_upto = false;
    agent_edit_anchor_span span = {0};
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_edit_find_old_span(data, strlen(data), old,
                                              &match, &match_len, &anchored,
                                              &span, err, sizeof(err)));
    AGENT_TEST_ASSERT(anchored);
    char *resolved = agent_edit_resolve_new_text(data, strlen(data),
                                                 match, match_len, &span,
                                                 new_text, &used_upto,
                                                 err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL);
    if (resolved) {
        AGENT_TEST_ASSERT(used_upto);
        AGENT_TEST_ASSERT(strstr(resolved, "[upto]") == NULL);
        AGENT_TEST_ASSERT(strcmp(resolved, expected) == 0);
        free(resolved);
    }
}

static void test_agent_edit_new_literal_upto_is_not_merge_marker(void) {
    const char *data =
        "static bool before(void);\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";
    const char *old =
        "static bool before(void);\n"
        "\n"
        "[upto]\n"
        "static bool after(void);\n";
    const char *new_text =
        "static bool before(void);\n"
        "static const char *marker = \"[upto]\";\n"
        "\n"
        "[upto]\n"
        "static bool after(void);\n";
    const char *expected =
        "static bool before(void);\n"
        "static const char *marker = \"[upto]\";\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    bool used_upto = false;
    agent_edit_anchor_span span = {0};
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_edit_find_old_span(data, strlen(data), old,
                                              &match, &match_len, &anchored,
                                              &span, err, sizeof(err)));
    char *resolved = agent_edit_resolve_new_text(data, strlen(data),
                                                 match, match_len, &span,
                                                 new_text, &used_upto,
                                                 err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL);
    if (resolved) {
        AGENT_TEST_ASSERT(used_upto);
        AGENT_TEST_ASSERT(strstr(resolved, "\"[upto]\"") != NULL);
        AGENT_TEST_ASSERT(strcmp(resolved, expected) == 0);
        free(resolved);
    }
}

static void test_agent_tool_edit_new_upto_merges_omitted_middle(void) {
    char root_tmpl[] = "/tmp/ds4_agent_edit_root_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/sample.c", root);
    const char *initial =
        "static bool before(void);\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";
    const char *expected =
        "static bool before(void);\n"
        "static bool added(void);\n"
        "\n"
        "static bool keep_one(void);\n"
        "static bool keep_two(void);\n"
        "\n"
        "static bool after(void);\n";
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_write_file_bytes(file_path, initial, strlen(initial),
                                             err, sizeof(err)) == 0);

    agent_tool_arg args[] = {
        {.name = "path", .value = "sample.c", .is_string = true},
        {.name = "old", .value =
            "static bool before(void);\n"
            "\n"
            "[upto]\n"
            "static bool after(void);\n", .is_string = true},
        {.name = "new", .value =
            "static bool before(void);\n"
            "static bool added(void);\n"
            "\n"
            "[upto]\n"
            "static bool after(void);\n", .is_string = true},
    };
    agent_tool_call call = {
        .name = "edit",
        .args = args,
        .argc = 3,
    };
    char *result = agent_tool_edit(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    if (result) {
        AGENT_TEST_ASSERT(strstr(result, "anchored old/new merge") != NULL);
        free(result);
    }

    char *data = NULL;
    size_t len = 0;
    AGENT_TEST_ASSERT(agent_read_file_bytes(file_path, &data, &len,
                                            err, sizeof(err)) == 0);
    if (data) {
        AGENT_TEST_ASSERT(strstr(data, "[upto]") == NULL);
        AGENT_TEST_ASSERT(len == strlen(expected));
        AGENT_TEST_ASSERT(strcmp(data, expected) == 0);
        free(data);
    }

    unlink(file_path);
    agent_path_list_free(&w.working_directories);
    rmdir(root);
}

static void test_agent_dsml_file_literal_escape_markup(void) {
    const char *dsml =
        "<｜DSML｜tool_calls>"
        "<｜DSML｜invoke name=\"write\">"
        "<｜DSML｜parameter name=\"path\" string=\"true\">sample.txt</｜DSML｜parameter>"
        "<｜DSML｜parameter name=\"content\" string=\"true\">line one\nliteral \\\\n tail</｜DSML｜parameter>"
        "</｜DSML｜invoke>"
        "</｜DSML｜tool_calls>";
    agent_dsml_parser p = {.state = AGENT_DSML_SEARCH};
    agent_dsml_feed(&p, dsml, strlen(dsml));

    AGENT_TEST_ASSERT(p.state == AGENT_DSML_DONE);
    AGENT_TEST_ASSERT(p.calls.len == 1);
    if (p.calls.len == 1) {
        const agent_tool_call *call = &p.calls.v[0];
        const char *content = agent_tool_arg_value(call, "content");
        AGENT_TEST_ASSERT(content != NULL);
        AGENT_TEST_ASSERT(content && strcmp(content, "line one\nliteral \\n tail") == 0);
        AGENT_TEST_ASSERT(content && strstr(content, "\\\\n") == NULL);
    }
    agent_dsml_parser_free(&p);
}

static void test_agent_file_literal_escape_decode_function_only(void) {
    const char *encoded = "before \\\\n after\nreal newline";
    size_t decoded_len = 0;
    char *decoded = agent_file_tool_decode_literal_escape_markup(
        encoded, strlen(encoded), &decoded_len);
    AGENT_TEST_ASSERT(decoded != NULL);
    if (decoded) {
        AGENT_TEST_ASSERT(decoded_len == strlen("before \\n after\nreal newline"));
        AGENT_TEST_ASSERT(strcmp(decoded, "before \\n after\nreal newline") == 0);
        free(decoded);
    }

    decoded = agent_file_tool_decode_literal_escape_markup(
        "already \\n literal", strlen("already \\n literal"), &decoded_len);
    AGENT_TEST_ASSERT(decoded != NULL);
    if (decoded) {
        AGENT_TEST_ASSERT(decoded_len == strlen("already \\n literal"));
        AGENT_TEST_ASSERT(strcmp(decoded, "already \\n literal") == 0);
        free(decoded);
    }
}

static void test_agent_tool_write_preserves_literal_backslash_n(void) {
    char root_tmpl[] = "/tmp/ds4_agent_literal_write_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    const char *expected = "line one\nliteral \\n tail\n";
    agent_tool_arg write_args[] = {
        {.name = "path", .value = "literal.txt", .is_string = true},
        {.name = "content", .value = (char *)expected, .is_string = true},
    };
    agent_tool_call write_call = {
        .name = "write",
        .args = write_args,
        .argc = 2,
    };
    char *write_result = agent_tool_write(&w, &write_call);
    AGENT_TEST_ASSERT(write_result != NULL);
    AGENT_TEST_ASSERT(write_result && strstr(write_result, "Wrote ") != NULL);
    free(write_result);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/literal.txt", root);
    char err[128] = {0};
    char *data = NULL;
    size_t len = 0;
    AGENT_TEST_ASSERT(agent_read_file_bytes(file_path, &data, &len,
                                            err, sizeof(err)) == 0);
    if (data) {
        AGENT_TEST_ASSERT(len == strlen(expected));
        AGENT_TEST_ASSERT(strcmp(data, expected) == 0);
        AGENT_TEST_ASSERT(strstr(data, "\\n") != NULL);
        free(data);
    }

    unlink(file_path);
    agent_path_list_free(&w.working_directories);
    rmdir(root);
}

static void test_agent_fake_worker_init(agent_worker *w, agent_config *cfg);

static void test_agent_tool_write_survives_worker_free(void) {
    char root_tmpl[] = "/tmp/ds4_agent_write_survives_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w;
    test_agent_fake_worker_init(&w, &cfg);
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    agent_tool_arg write_args[] = {
        {.name = "path", .value = "durable.txt", .is_string = true},
        {.name = "content", .value = "keep me\n", .is_string = true},
    };
    agent_tool_call write_call = {
        .name = "write",
        .args = write_args,
        .argc = 2,
    };
    char *write_result = agent_tool_write(&w, &write_call);
    AGENT_TEST_ASSERT(write_result != NULL);
    AGENT_TEST_ASSERT(write_result && strstr(write_result, "Wrote ") != NULL);
    free(write_result);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/durable.txt", root);
    AGENT_TEST_ASSERT(access(file_path, F_OK) == 0);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 0);
    AGENT_TEST_ASSERT(w.temp_files.len == 0);

    agent_worker_free(&w);
    AGENT_TEST_ASSERT(access(file_path, F_OK) == 0);
    unlink(file_path);
    rmdir(root);
}

static void test_agent_temp_files_cleanup_requires_idle_window(void) {
    char path[] = "/tmp/ds4_agent_temp_idle_XXXXXX";
    int fd = mkstemp(path);
    AGENT_TEST_ASSERT(fd >= 0);
    if (fd < 0) return;
    write_all(fd, "temp\n", 5);
    close(fd);

    agent_config cfg = {.non_interactive = true};
    agent_worker w;
    test_agent_fake_worker_init(&w, &cfg);
    agent_temp_files_track(&w, path);
    AGENT_TEST_ASSERT(w.temp_files.len == 1);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 1);

    uint64_t now = agent_now_sec();
    w.temp_files.v[0].last_write_at = now - (AGENT_TEMP_FILE_IDLE_SECONDS + 1);
    w.temp_files.v[0].last_read_at = now - (AGENT_TEMP_FILE_IDLE_SECONDS - 1);
    agent_temp_files_cleanup(&w, now);
    AGENT_TEST_ASSERT(access(path, F_OK) == 0);
    AGENT_TEST_ASSERT(w.temp_files.len == 1);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 1);

    w.temp_files.v[0].last_read_at = now - (AGENT_TEMP_FILE_IDLE_SECONDS + 1);
    agent_temp_files_cleanup(&w, now);
    AGENT_TEST_ASSERT(access(path, F_OK) != 0 && errno == ENOENT);
    AGENT_TEST_ASSERT(w.temp_files.len == 0);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 0);

    agent_worker_free(&w);
    unlink(path);
}

static void test_agent_tool_edit_preserves_literal_backslash_n(void) {
    char root_tmpl[] = "/tmp/ds4_agent_literal_edit_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/literal.txt", root);
    const char *initial = "alpha \\n beta\nsecond line\n";
    const char *expected = "gamma \\n delta\nsecond line\n";
    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_write_file_bytes(file_path, initial, strlen(initial),
                                             err, sizeof(err)) == 0);

    agent_tool_arg mismatch_args[] = {
        {.name = "path", .value = "literal.txt", .is_string = true},
        {.name = "old", .value = "alpha \n beta", .is_string = true},
        {.name = "new", .value = "should not apply", .is_string = true},
    };
    agent_tool_call mismatch_call = {
        .name = "edit",
        .args = mismatch_args,
        .argc = 3,
    };
    char *mismatch = agent_tool_edit(&w, &mismatch_call);
    AGENT_TEST_ASSERT(mismatch != NULL);
    AGENT_TEST_ASSERT(mismatch && strstr(mismatch, "old text") != NULL);
    free(mismatch);

    agent_tool_arg edit_args[] = {
        {.name = "path", .value = "literal.txt", .is_string = true},
        {.name = "old", .value = "alpha \\n beta", .is_string = true},
        {.name = "new", .value = "gamma \\n delta", .is_string = true},
    };
    agent_tool_call edit_call = {
        .name = "edit",
        .args = edit_args,
        .argc = 3,
    };
    char *result = agent_tool_edit(&w, &edit_call);
    AGENT_TEST_ASSERT(result != NULL);
    AGENT_TEST_ASSERT(result && strstr(result, "old/new replacement") != NULL);
    free(result);

    char *data = NULL;
    size_t len = 0;
    AGENT_TEST_ASSERT(agent_read_file_bytes(file_path, &data, &len,
                                            err, sizeof(err)) == 0);
    if (data) {
        AGENT_TEST_ASSERT(len == strlen(expected));
        AGENT_TEST_ASSERT(strcmp(data, expected) == 0);
        AGENT_TEST_ASSERT(strstr(data, "\\n") != NULL);
        free(data);
    }

    unlink(file_path);
    agent_path_list_free(&w.working_directories);
    rmdir(root);
}

static void test_agent_working_directory_path_resolution(void) {
    char root_tmpl[] = "/tmp/ds4_agent_jail_root_XXXXXX";
    char outside_tmpl[] = "/tmp/ds4_agent_jail_out_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    char *outside_tmp = mkdtemp(outside_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    AGENT_TEST_ASSERT(outside_tmp != NULL);
    if (!root_tmp || !outside_tmp) return;

    char root[PATH_MAX];
    char outside[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    AGENT_TEST_ASSERT(realpath(outside_tmp, outside) != NULL);

    char inside_file[PATH_MAX];
    char outside_file[PATH_MAX];
    snprintf(inside_file, sizeof(inside_file), "%s/file.txt", root);
    snprintf(outside_file, sizeof(outside_file), "%s/file.txt", outside);
    char err[256] = {0};
    AGENT_TEST_ASSERT(agent_write_file_bytes(inside_file, "inside\n", 7,
                                             err, sizeof(err)) == 0);
    AGENT_TEST_ASSERT(agent_write_file_bytes(outside_file, "outside\n", 8,
                                             err, sizeof(err)) == 0);

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    agent_path_list_append(&w.working_directories, root);

    char *resolved = agent_resolve_tool_path(&w, "file.txt",
                                             AGENT_PATH_EXISTING,
                                             err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL && !strcmp(resolved, inside_file));
    free(resolved);
    resolved = agent_resolve_tool_path(&w, inside_file,
                                       AGENT_PATH_EXISTING,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL && !strcmp(resolved, inside_file));
    free(resolved);
    resolved = agent_resolve_tool_path(&w, outside_file,
                                       AGENT_PATH_EXISTING,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved == NULL);
    free(resolved);

    char link_path[PATH_MAX];
    snprintf(link_path, sizeof(link_path), "%s/link-out", root);
    AGENT_TEST_ASSERT(symlink(outside_file, link_path) == 0);
    resolved = agent_resolve_tool_path(&w, "link-out",
                                       AGENT_PATH_EXISTING,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved == NULL);
    free(resolved);

    agent_path_list_append(&w.working_directories, outside);
    resolved = agent_resolve_tool_path(&w, outside_file,
                                       AGENT_PATH_EXISTING,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL && !strcmp(resolved, outside_file));
    free(resolved);

    resolved = agent_resolve_tool_path(&w, "new.txt",
                                       AGENT_PATH_PARENT,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL && agent_path_is_under_root(root, resolved));
    free(resolved);
    char outside_new[PATH_MAX];
    snprintf(outside_new, sizeof(outside_new), "%s/new.txt", outside);
    resolved = agent_resolve_tool_path(&w, outside_new,
                                       AGENT_PATH_PARENT,
                                       err, sizeof(err));
    AGENT_TEST_ASSERT(resolved != NULL && agent_path_is_under_root(outside, resolved));
    free(resolved);

    unlink(link_path);
    unlink(inside_file);
    unlink(outside_file);
    agent_path_list_free(&w.working_directories);
    rmdir(root);
    rmdir(outside);
}

static void test_agent_working_directory_file_tools(void) {
    char root_tmpl[] = "/tmp/ds4_agent_tool_root_XXXXXX";
    char outside_tmpl[] = "/tmp/ds4_agent_tool_out_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    char *outside_tmp = mkdtemp(outside_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    AGENT_TEST_ASSERT(outside_tmp != NULL);
    if (!root_tmp || !outside_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    agent_tool_arg write_args[] = {
        {.name = "path", .value = "created.txt", .is_string = true},
        {.name = "content", .value = "hello\n", .is_string = true},
    };
    agent_tool_call write_call = {
        .name = "write",
        .args = write_args,
        .argc = 2,
    };
    char *write_result = agent_tool_write(&w, &write_call);
    AGENT_TEST_ASSERT(strstr(write_result, "Wrote 6 bytes") != NULL);
    free(write_result);

    agent_tool_arg read_args[] = {
        {.name = "path", .value = "created.txt", .is_string = true},
    };
    agent_tool_call read_call = {
        .name = "read",
        .args = read_args,
        .argc = 1,
    };
    char *read_result = agent_tool_read(&w, &read_call);
    AGENT_TEST_ASSERT(strstr(read_result, "hello") != NULL);
    free(read_result);

    char outside[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(outside_tmp, outside) != NULL);
    char outside_file[PATH_MAX];
    snprintf(outside_file, sizeof(outside_file), "%s/outside.txt", outside);
    char err[256];
    AGENT_TEST_ASSERT(agent_write_file_bytes(outside_file, "nope\n", 5,
                                             err, sizeof(err)) == 0);
    agent_tool_arg outside_read_args[] = {
        {.name = "path", .value = outside_file, .is_string = true},
    };
    agent_tool_call outside_read_call = {
        .name = "read",
        .args = outside_read_args,
        .argc = 1,
    };
    char *outside_result = agent_tool_read(&w, &outside_read_call);
    AGENT_TEST_ASSERT(strstr(outside_result, "outside configured working directories") != NULL);
    free(outside_result);

    char created[PATH_MAX];
    snprintf(created, sizeof(created), "%s/created.txt", root);
    unlink(created);
    unlink(outside_file);
    agent_path_list_free(&w.working_directories);
    rmdir(root);
    rmdir(outside);
}

static void test_agent_default_working_directory_from_launch_cwd(void) {
    char cwd[PATH_MAX];
    AGENT_TEST_ASSERT(getcwd(cwd, sizeof(cwd)) != NULL);

    agent_config cfg = {0};
    snprintf(cfg.launch_working_directory, sizeof(cfg.launch_working_directory),
             "%s", cwd);
    char err[256] = {0};
    AGENT_TEST_ASSERT(agent_config_resolve_working_directory(&cfg, err,
                                                              sizeof(err)));
    AGENT_TEST_ASSERT(cfg.working_directories.len == 1);
    AGENT_TEST_ASSERT(!strcmp(cfg.working_directories.v[0], cwd));

    agent_path_list_free(&cfg.working_directories);
}

static void test_agent_project_instruction_file_priority(void) {
    char root_tmpl[] = "/tmp/ds4_agent_project_instructions_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    char selected[64] = {0};
    char *text = agent_read_project_instruction_file(root, selected,
                                                     sizeof(selected));
    AGENT_TEST_ASSERT(text == NULL);
    free(text);

    char hermes[PATH_MAX], claude[PATH_MAX], agents[PATH_MAX], agent[PATH_MAX];
    snprintf(hermes, sizeof(hermes), "%s/HERMES.md", root);
    snprintf(claude, sizeof(claude), "%s/CLAUDE.md", root);
    snprintf(agents, sizeof(agents), "%s/AGENTS.md", root);
    snprintf(agent, sizeof(agent), "%s/AGENT.md", root);

    char err[128] = {0};
    AGENT_TEST_ASSERT(agent_write_file_bytes(hermes, "hermes\n", 7,
                                             err, sizeof(err)) == 0);
    AGENT_TEST_ASSERT(agent_write_file_bytes(claude, "claude\n", 7,
                                             err, sizeof(err)) == 0);
    selected[0] = '\0';
    text = agent_read_project_instruction_file(root, selected,
                                               sizeof(selected));
    AGENT_TEST_ASSERT(text && !strcmp(text, "claude\n"));
    AGENT_TEST_ASSERT(!strcmp(selected, "CLAUDE.md"));
    free(text);

    AGENT_TEST_ASSERT(agent_write_file_bytes(agents, "agents\n", 7,
                                             err, sizeof(err)) == 0);
    selected[0] = '\0';
    text = agent_read_project_instruction_file(root, selected,
                                               sizeof(selected));
    AGENT_TEST_ASSERT(text && !strcmp(text, "agents\n"));
    AGENT_TEST_ASSERT(!strcmp(selected, "AGENTS.md"));
    free(text);

    AGENT_TEST_ASSERT(agent_write_file_bytes(agent, "agent\n", 6,
                                             err, sizeof(err)) == 0);
    selected[0] = '\0';
    text = agent_read_project_instruction_file(root, selected,
                                               sizeof(selected));
    AGENT_TEST_ASSERT(text && !strcmp(text, "agent\n"));
    AGENT_TEST_ASSERT(!strcmp(selected, "AGENT.md"));
    free(text);

    unlink(agent);
    unlink(agents);
    unlink(claude);
    unlink(hermes);
    rmdir(root);
}

static void test_agent_path_list_remove_prefix(void) {
    agent_path_list list = {0};
    agent_path_list_append(&list, "/root/a.txt");
    agent_path_list_append(&list, "/root/sub/b.txt");
    agent_path_list_append(&list, "/other.txt");
    agent_path_list_append(&list, "/root");
    AGENT_TEST_ASSERT(list.len == 4);

    /* Remove all entries with prefix "/root" (including "/root" itself). */
    bool ok = agent_path_list_remove(&list, "/root", true);
    AGENT_TEST_ASSERT(ok);
    AGENT_TEST_ASSERT(list.len == 1);
    AGENT_TEST_ASSERT(!strcmp(list.v[0], "/other.txt"));

    /* Exact removal still works. */
    ok = agent_path_list_remove(&list, "/other.txt", false);
    AGENT_TEST_ASSERT(ok);
    AGENT_TEST_ASSERT(list.len == 0);

    agent_path_list_free(&list);
}

static void test_agent_remove_workspace_clears_auto_allowed(void) {
    char root1_tmpl[] = "/tmp/ds4_agent_test_auto1_XXXXXX";
    char root2_tmpl[] = "/tmp/ds4_agent_test_auto2_XXXXXX";
    char *root1_tmp = mkdtemp(root1_tmpl);
    char *root2_tmp = mkdtemp(root2_tmpl);
    AGENT_TEST_ASSERT(root1_tmp != NULL);
    AGENT_TEST_ASSERT(root2_tmp != NULL);
    if (!root1_tmp || !root2_tmp) return;

    char root1[PATH_MAX], root2[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root1_tmp, root1) != NULL);
    AGENT_TEST_ASSERT(realpath(root2_tmp, root2) != NULL);

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);
    agent_path_list_append(&w.working_directories, root1);
    agent_path_list_append(&w.working_directories, root2);
    AGENT_TEST_ASSERT(w.working_directories.len == 2);

    /* Add auto-allowed paths under root1. */
    char path1[PATH_MAX], path2[PATH_MAX];
    snprintf(path1, sizeof(path1), "%s/a.txt", root1);
    snprintf(path2, sizeof(path2), "%s/sub/b.txt", root1);
    agent_path_list_append(&w.auto_allowed_paths, path1);
    agent_path_list_append(&w.auto_allowed_paths, path2);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 2);

    /* Remove root1 — auto-allowed paths under it must be cleared. */
    char err[256] = {0};
    char removed[PATH_MAX] = {0};
    bool ok = agent_worker_remove_workspace(&w, root1, removed, sizeof(removed),
                                            err, sizeof(err));
    AGENT_TEST_ASSERT(ok);
    AGENT_TEST_ASSERT(w.working_directories.len == 1);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 0);
    AGENT_TEST_ASSERT(!strcmp(w.working_directories.v[0], root2));

    /* Clean up. */
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_mutex_destroy(&w.mu);
    agent_path_list_free(&w.working_directories);
    agent_path_list_free(&w.auto_allowed_paths);
    rmdir(root1_tmp);
    rmdir(root2_tmp);
}

static void test_agent_remove_auto_allowed_path_directly(void) {
    char root_tmpl[] = "/tmp/ds4_agent_test_auto2_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {.cfg = &cfg};
    agent_path_list_append(&w.working_directories, root);

    /* Add an auto-allowed path that is not a workspace root. */
    char ap[PATH_MAX];
    snprintf(ap, sizeof(ap), "%s/x.txt", root);
    agent_path_list_append(&w.auto_allowed_paths, ap);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 1);

    /* Remove it via /workspace -<path> — it is not in working_directories,
     * so the function falls back to auto_allowed_paths. */
    char err[256] = {0};
    char removed[PATH_MAX] = {0};
    bool ok = agent_worker_remove_workspace(&w, ap, removed, sizeof(removed),
                                            err, sizeof(err));
    AGENT_TEST_ASSERT(ok);
    AGENT_TEST_ASSERT(w.auto_allowed_paths.len == 0);
    AGENT_TEST_ASSERT(w.working_directories.len == 1); /* root still there */

    agent_path_list_free(&w.working_directories);
    agent_path_list_free(&w.auto_allowed_paths);
    rmdir(root_tmp);
}

static void test_agent_executable_exists(void) {
    AGENT_TEST_ASSERT(agent_executable_exists("/bin/sh"));
    AGENT_TEST_ASSERT(!agent_executable_exists("/definitely/not/a/real/executable"));
}

static void test_agent_command_in_path(void) {
    const char *old_path = getenv("PATH");
    char *saved_path = old_path ? xstrdup(old_path) : NULL;
    setenv("PATH", "/bin:/usr/bin", 1);

    AGENT_TEST_ASSERT(agent_command_in_path("sh"));
    AGENT_TEST_ASSERT(!agent_command_in_path("ds4-agent-definitely-missing"));

    if (saved_path) {
        setenv("PATH", saved_path, 1);
        free(saved_path);
    } else {
        unsetenv("PATH");
    }
}

/* --- Footer regression tests (tasks 4.1-4.8) --- */

/* Helper: populate a subagent_status for testing agent_format_badge */
static void test_badge_set_item(ds4_agent_subagent_status *item, uint64_t id,
                                 const char *name, bool active,
                                 ds4_agent_subagent_state state,
                                 bool approval_blocked, bool queued_output,
                                 int prefill_done, int prefill_total,
                                 double prefill_tps, double gen_tps,
                                 const char *perms, bool engine_owner) {
    memset(item, 0, sizeof(*item));
    item->id.value = id;
    snprintf(item->name, sizeof(item->name), "%s", name);
    item->active = active;
    item->state = state;
    item->approval_blocked = approval_blocked;
    item->queued_output = queued_output;
    item->prefill_done = prefill_done;
    item->prefill_total = prefill_total;
    item->prefill_tps = prefill_tps;
    item->gen_tps = gen_tps;
    item->engine_owner = engine_owner;
    if (perms)
        snprintf(item->tool_permissions, sizeof(item->tool_permissions), "%s", perms);
    else
        memset(item->tool_permissions, 0, sizeof(item->tool_permissions));
}

/* 4.1: Manager-backed test proving main + subagents render exactly once
 * in stable order before and after switching focus. */
static void test_agent_footer_main_subagents_stable_order(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    ds4_agent_subagent_id main_id = {.value = 1};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);
    ds4_agent_subagent_id sub_id = {0};
    ds4_agent_subagent_create_request sub_req = {
        .name = "sub1",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &sub_req, &sub_id) == 0);
    AGENT_TEST_ASSERT(sub_id.value == 2);

    /* Build footer with sandbox placeholder */
    agent_status st = {0};
    char buf[4096];
    build_footer_text(&st, mgr, NULL, 200, buf, sizeof(buf));
    AGENT_TEST_ASSERT(strstr(buf, "1:main") != NULL);
    AGENT_TEST_ASSERT(strstr(buf, "2:sub1") != NULL);

    /* After switching focus, both badges remain and order is stable */
    ds4_agent_subagent_switch(mgr, sub_id);
    char buf2[4096];
    build_footer_text(&st, mgr, NULL, 200, buf2, sizeof(buf2));
    AGENT_TEST_ASSERT(strstr(buf2, "1:main") != NULL);
    AGENT_TEST_ASSERT(strstr(buf2, "2:sub1") != NULL);
    /* main should appear before sub1 */
    const char *m = strstr(buf2, "1:main");
    const char *s = strstr(buf2, "2:sub1");
    AGENT_TEST_ASSERT(m != NULL && s != NULL && m < s);

    ds4_agent_subagents_destroy(mgr);
}

/* 4.2: Test that /allow and /disallow policy changes update badges.
 * Uses agent_format_badge with explicit permissions. */
static void test_agent_footer_policy_change_reflects(void) {
    ds4_agent_subagent_status items[2];
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", true);
    test_badge_set_item(&items[1], 2, "sub1", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RW-X", false);
    char v1[256], s1[256];
    agent_format_badge(&items[0], v1, sizeof(v1), s1, sizeof(s1));
    AGENT_TEST_ASSERT(strstr(v1, "RWBX") != NULL);

    char v2[256], s2[256];
    agent_format_badge(&items[1], v2, sizeof(v2), s2, sizeof(s2));
    AGENT_TEST_ASSERT(strstr(v2, "RW-X") != NULL);

    /* Change policy on item 0 */
    snprintf(items[0].tool_permissions, sizeof(items[0].tool_permissions), "%s", "---X");
    agent_format_badge(&items[0], v1, sizeof(v1), s1, sizeof(s1));
    AGENT_TEST_ASSERT(strstr(v1, "---X") != NULL);
    /* Item 1 unchanged */
    agent_format_badge(&items[1], v2, sizeof(v2), s2, sizeof(s2));
    AGENT_TEST_ASSERT(strstr(v2, "RW-X") != NULL);
}

/* 4.3: Full attention/focus matrix test covering active unread, active error,
 * active approval, background attention, and main error states. */
static void test_agent_footer_attention_focus_matrix(void) {
    ds4_agent_subagent_status items[5];
    /* Engine-owner, no attention flags → ▶ */
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", true);
    /* Active with error → ! with magenta brackets */
    test_badge_set_item(&items[2], 3, "active_err", true,
                        DS4_AGENT_SUBAGENT_STATE_ERROR,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Active with approval blocked → ! with magenta brackets */
    test_badge_set_item(&items[3], 4, "active_appr", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        true, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Background with error → ! without brackets (background) */
    test_badge_set_item(&items[4], 5, "bg_err", false,
                        DS4_AGENT_SUBAGENT_STATE_ERROR,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);

    char v[256], s[256];
    /* Item 0: engine-owner no attention → ▶ with magenta brackets */
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "▶") != NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);  /* magenta brackets */

    /* Item 2: active error → ! with magenta brackets */
    agent_format_badge(&items[2], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "!") != NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);

    /* Item 3: active approval → ! with magenta brackets */
    agent_format_badge(&items[3], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "!") != NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);

    /* Item 4: bg error → ! without brackets */
    agent_format_badge(&items[4], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "!") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "5:bg_err") != NULL);
    /* Background badge: no magenta brackets */
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") == NULL);
}

/* 4.4: Replace truncation assertions with exact multi-row wrapping and
 * oversized-badge assertions using agent_display_width. */
static void test_agent_footer_wrapping_oversized(void) {
    /* Test display-width measurement */
    const char *plain = "hello world";
    AGENT_TEST_ASSERT(agent_display_width(plain) == 11);
    const char *ansi = "\x1b[32mhello\x1b[0m world";
    AGENT_TEST_ASSERT(agent_display_width(ansi) == 11);  /* ANSI escapes count 0 */

    /* Test oversized badge splitting via build_footer_text */
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    ds4_agent_subagent_id id1 = {0}, id2 = {0};
    ds4_agent_subagent_create_request r1 = {.name = "main", .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB};
    ds4_agent_subagent_create_request r2 = {.name = "longname_subagent", .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND};
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &r1, &id1) == 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &r2, &id2) == 0);

    agent_status st = {0};
    char buf[4096];
    /* Narrow width forces wrapping */
    build_footer_text(&st, mgr, NULL, 40, buf, sizeof(buf));
    /* Both agents should be present */
    AGENT_TEST_ASSERT(strstr(buf, "1:main") != NULL);
    AGENT_TEST_ASSERT(strstr(buf, "2:longname_subagent") != NULL);
    /* Should contain a newline (wrapping occurred) */
    AGENT_TEST_ASSERT(strchr(buf, '\n') != NULL);

    ds4_agent_subagents_destroy(mgr);
}

/* 4.5: Exact grammar assertions for <id>:<name>, ■, fixed-width metrics,
 * and final-field permissions. */
static void test_agent_footer_grammar_assertions(void) {
    ds4_agent_subagent_status items[2];
    /* Engine-owner agent with prefill metrics */
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 452, 1000, 128.5, 0.0, "RWBX", true);
    /* Background agent (no engine-owner, no attention) */
    test_badge_set_item(&items[1], 2, "bg_agent", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RW-X", false);

    char v[256], s[256];

    /* Item 0: active, prefill active */
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "1:main") != NULL);          /* <id>:<name> */
    AGENT_TEST_ASSERT(strstr(v, "▶") != NULL);               /* engine-owner indicator */
    AGENT_TEST_ASSERT(strstr(v, "pp") != NULL);              /* prefill field */
    AGENT_TEST_ASSERT(strstr(v, "pp  45.2%  128 t/s") != NULL); /* fixed-width metrics */
    AGENT_TEST_ASSERT(strstr(v, "RWBX") != NULL);            /* permissions final field */
    /* Check no "think:" prefix */
    AGENT_TEST_ASSERT(strstr(v, "think:") == NULL);

    /* Item 1: background */
    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "2:bg_agent") != NULL);      /* <id>:<name> */
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);               /* background glyph */
    AGENT_TEST_ASSERT(strstr(v, "---") != NULL);             /* neutral activity */
    AGENT_TEST_ASSERT(strstr(v, "RW-X") != NULL);            /* permissions */
    /* Check no "think:" prefix */
    AGENT_TEST_ASSERT(strstr(v, "think:") == NULL);
}

/* 4.6: Exact mapping tests for 🚫, 🧠, ⚡️; verify badges contain only the
 * icon field and "think:" is absent; cover subagent status use. */
static void test_agent_footer_think_mode_icons(void) {
    /* Test the shared helper directly */
    AGENT_TEST_ASSERT(strcmp(agent_think_mode_icon(DS4_THINK_NONE), "\xf0\x9f\x9a\xab") == 0); /* 🚫 */
    AGENT_TEST_ASSERT(strcmp(agent_think_mode_icon(DS4_THINK_HIGH), "\xf0\x9f\xa7\xa0") == 0); /* 🧠 */
    AGENT_TEST_ASSERT(strcmp(agent_think_mode_icon(DS4_THINK_MAX), "\xe2\x9a\xa1\xef\xb8\x8f") == 0); /* ⚡️ + VS16 */

    /* Test badge formatting for each mode */
    ds4_agent_subagent_status items[3];
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", true);
    items[0].think_mode = DS4_THINK_NONE;
    test_badge_set_item(&items[1], 2, "sub1", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    items[1].think_mode = DS4_THINK_HIGH;
    test_badge_set_item(&items[2], 3, "sub2", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    items[2].think_mode = DS4_THINK_MAX;

    char v[256], s[256];
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "\xf0\x9f\x9a\xab") != NULL);  /* 🚫 */
    AGENT_TEST_ASSERT(strstr(v, "think:") == NULL);

    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "\xf0\x9f\xa7\xa0") != NULL);  /* 🧠 */
    AGENT_TEST_ASSERT(strstr(v, "think:") == NULL);

    agent_format_badge(&items[2], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "\xe2\x9a\xa1\xef\xb8\x8f") != NULL);  /* ⚡️ + VS16 */
    AGENT_TEST_ASSERT(strstr(v, "think:") == NULL);
}

/* 4.7: State-matrix assertions proving prefill badges contain only "pp",
 * generation/compaction badges contain only "gen", and inactive/background
 * badges contain only the neutral activity placeholder. */
static void test_agent_footer_mutually_exclusive_activity(void) {
    ds4_agent_subagent_status items[4];
    /* Prefill engine-owner */
    test_badge_set_item(&items[0], 1, "prefill", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 452, 1000, 128.5, 0.0, "RWBX", true);
    /* Generation active (non-owner) */
    test_badge_set_item(&items[1], 2, "generation", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 24.5, "RWBX", false);
    /* Inactive/background with no live metrics */
    test_badge_set_item(&items[2], 3, "idle", false,
                        DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Error state */
    test_badge_set_item(&items[3], 4, "error", false,
                        DS4_AGENT_SUBAGENT_STATE_ERROR,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);

    char v[256], s[256];

    /* Item 0: prefill → contains "pp", does NOT contain "gen" */
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "pp") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "pp  45.2%  128 t/s") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "gen") == NULL);

    /* Item 1: generation → contains "gen", does NOT contain "pp" */
    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "gen") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "24.5 t/s") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "pp") == NULL);

    /* Completed prefill counters must not make generation flip back to pp. */
    items[1].prefill_done = 1000;
    items[1].prefill_total = 1000;
    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "gen  24.5 t/s") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "pp") == NULL);

    /* Item 2: idle/waiting → contains "---", does NOT contain "pp" or "gen" */
    agent_format_badge(&items[2], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "---") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "pp") == NULL);
    AGENT_TEST_ASSERT(strstr(v, "gen") == NULL);

    /* Item 3: error → contains "---", does NOT contain "pp" or "gen" */
    agent_format_badge(&items[3], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "---") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "pp") == NULL);
    AGENT_TEST_ASSERT(strstr(v, "gen") == NULL);
}

/* 4.8: Manager-backed regression test with multiple concurrently running
 * agents at 80 columns; assert every inactive running agent remains present
 * after ANSI styling, wrapping, and focus switches. */
static void test_agent_footer_multi_agent_80col(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    ds4_agent_subagent_id ids[4];
    const char *names[] = {"main", "alpha", "beta", "gamma"};
    for (int i = 0; i < 4; i++) {
        ds4_agent_subagent_create_request req = {
            .name = names[i],
            .autonomy = i == 0 ? DS4_AGENT_SUBAGENT_AUTONOMY_TAB :
                                 DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND,
        };
        AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &req, &ids[i]) == 0);
    }

    agent_status st = {0};
    char buf[8192];
    build_footer_text(&st, mgr, NULL, 80, buf, sizeof(buf));

    /* All four agents present */
    for (int i = 0; i < 4; i++) {
        char expected[64];
        snprintf(expected, sizeof(expected), "%" PRIu64 ":%s",
                 ids[i].value, names[i]);
        AGENT_TEST_ASSERT(strstr(buf, expected) != NULL);
    }

    /* Switch focus to beta (id=3) */
    ds4_agent_subagent_switch(mgr, ids[2]);  /* beta is index 2, id=3 */
    char buf2[8192];
    build_footer_text(&st, mgr, NULL, 80, buf2, sizeof(buf2));

    /* All four still present after focus switch */
    for (int i = 0; i < 4; i++) {
        char expected[64];
        snprintf(expected, sizeof(expected), "%" PRIu64 ":%s",
                 ids[i].value, names[i]);
        AGENT_TEST_ASSERT(strstr(buf2, expected) != NULL);
    }

    /* Order stable: main (1) before alpha (2) before beta (3) before gamma (4) */
    const char *p1 = strstr(buf2, "1:main");
    const char *p2 = strstr(buf2, "2:alpha");
    const char *p3 = strstr(buf2, "3:beta");
    const char *p4 = strstr(buf2, "4:gamma");
    AGENT_TEST_ASSERT(p1 && p2 && p3 && p4);
    AGENT_TEST_ASSERT(p1 < p2 && p2 < p3 && p3 < p4);

    ds4_agent_subagents_destroy(mgr);
}

/* 4.9: Verify writable workspace/temp paths do not appear in the footer.
 * This is a regression guard — the footer must never leak sandbox paths. */
static void test_agent_footer_no_writable_paths(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);
    ds4_agent_subagent_id main_id = {.value = 1};
    ds4_agent_subagent_create_request main_req = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &main_req, &main_id) == 0);

    agent_status st = {0};
    st.state = AGENT_WORKER_IDLE;
    st.ctx_used = 1000;
    st.ctx_size = 16000;
    snprintf(st.session_name, sizeof(st.session_name), "%s", "test");
    snprintf(st.writable_workspace_paths, sizeof(st.writable_workspace_paths),
             "%s", "/workspace");
    snprintf(st.writable_temp_paths, sizeof(st.writable_temp_paths),
             "%s", "/tmp");
    char buf[4096];
    build_footer_text(&st, mgr, NULL, 200, buf, sizeof(buf));
    AGENT_TEST_ASSERT(strstr(buf, "workspace") == NULL);
    AGENT_TEST_ASSERT(strstr(buf, "writable") == NULL);
    AGENT_TEST_ASSERT(strstr(buf, "/tmp") == NULL);
    ds4_agent_subagents_destroy(mgr);
}

/* 3.1: Badge tests for owner, non-owner, no-owner, focus-different-from-owner,
 * and attention-overrides-owner cases. */
static void test_agent_footer_ownership_badge(void) {
    ds4_agent_subagent_status items[5];

    /* Engine-owner, no attention → ▶ with magenta brackets (active & owner) */
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", true);
    /* Non-owner, no attention → ■ */
    test_badge_set_item(&items[1], 2, "sub1", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RW-X", false);
    /* No owner (engine_owner=false, not active) → ■ */
    test_badge_set_item(&items[2], 3, "sub2", false,
                        DS4_AGENT_SUBAGENT_STATE_IDLE,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Focus differs from owner: active=true but not engine_owner → ■ with magenta brackets */
    test_badge_set_item(&items[3], 4, "focused", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Attention overrides owner: engine_owner=true but needs attention → ! */
    test_badge_set_item(&items[4], 5, "attn_owner", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        true, false, 0, 0, 0.0, 0.0, "RWBX", true);

    char v[256], s[256];

    /* Item 0: engine-owner → ▶ */
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "▶") != NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);  /* active → magenta brackets */

    /* Item 1: non-owner → ■ */
    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "▶") == NULL);
    AGENT_TEST_ASSERT(strstr(v, "!") == NULL);

    /* Item 2: no owner → ■ (no attention, no ownership) */
    agent_format_badge(&items[2], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);

    /* Item 3: focused but not owner → ■ with magenta brackets */
    agent_format_badge(&items[3], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);

    /* Item 4: attention overrides owner → ! */
    agent_format_badge(&items[4], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "!") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "▶") == NULL);
}

/* 3.2: Manager-backed test proving one sampled owner id yields at most one
 * engine_owner item and that owner id 0 yields none. */
static void test_agent_footer_ownership_manager_sampling(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds4_agent_subagent_id ids[3];
    const char *names[] = {"main", "alpha", "beta"};
    for (int i = 0; i < 3; i++) {
        ds4_agent_subagent_create_request req = {
            .name = names[i],
            .autonomy = i == 0 ? DS4_AGENT_SUBAGENT_AUTONOMY_TAB :
                                 DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND,
        };
        AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &req, &ids[i]) == 0);
    }

    size_t n = 0;
    ds4_agent_subagent_status items[4];
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 4, &n) == 0);
    AGENT_TEST_ASSERT(n == 3);

    /* With owner_id still 0, no item should be engine_owner */
    int owner_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (items[i].engine_owner) owner_count++;
    }
    AGENT_TEST_ASSERT(owner_count == 0);

    /* Simulate setting engine_owner_id to alpha's id (ids[1].value) */
    ds4_agent_subagents_set_engine_owner(mgr, ids[1].value);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 4, &n) == 0);
    owner_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (items[i].engine_owner) {
            owner_count++;
            AGENT_TEST_ASSERT(items[i].id.value == ids[1].value);
        }
    }
    AGENT_TEST_ASSERT(owner_count == 1);

    /* Reset to 0, verify no owner again */
    ds4_agent_subagents_set_engine_owner(mgr, 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 4, &n) == 0);
    owner_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (items[i].engine_owner) owner_count++;
    }
    AGENT_TEST_ASSERT(owner_count == 0);

    ds4_agent_subagents_destroy(mgr);
}

/* 3.3: Handoff test proving a later resident-list snapshot reflects the new
 * owner without requiring every intermediate gate transition to render. */
static void test_agent_footer_ownership_handoff(void) {
    ds4_agent_subagents *mgr = NULL;
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, NULL) == 0);

    ds4_agent_subagent_id ids[2];
    ds4_agent_subagent_create_request req_main = {
        .name = "main",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &req_main, &ids[0]) == 0);
    ds4_agent_subagent_create_request req_sub = {
        .name = "sub1",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_BACKGROUND,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &req_sub, &ids[1]) == 0);

    size_t n = 0;
    ds4_agent_subagent_status items[3];

    /* Snapshot 1: main is owner */
    ds4_agent_subagents_set_engine_owner(mgr, ids[0].value);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 3, &n) == 0);
    AGENT_TEST_ASSERT(n == 2);
    AGENT_TEST_ASSERT(items[0].engine_owner);  /* main */
    AGENT_TEST_ASSERT(!items[1].engine_owner); /* sub1 */

    /* Snapshot 2: sub1 is owner (handoff occurred) */
    ds4_agent_subagents_set_engine_owner(mgr, ids[1].value);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 3, &n) == 0);
    AGENT_TEST_ASSERT(!items[0].engine_owner); /* main */
    AGENT_TEST_ASSERT(items[1].engine_owner);  /* sub1 */

    /* Snapshot 3: no owner (gate released) */
    ds4_agent_subagents_set_engine_owner(mgr, 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, items, 3, &n) == 0);
    AGENT_TEST_ASSERT(!items[0].engine_owner);
    AGENT_TEST_ASSERT(!items[1].engine_owner);

    ds4_agent_subagents_destroy(mgr);
}

/* 3.4: Regression assertions that queued output still selects !, focus still
 * controls brackets, and waiting sessions can retain generation metrics
 * independently of ownership. */
static void test_agent_footer_ownership_regression(void) {
    ds4_agent_subagent_status items[4];

    /* Queued output → ! even when engine_owner */
    test_badge_set_item(&items[0], 1, "main", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, true, 0, 0, 0.0, 0.0, "RWBX", true);
    /* Focus controls brackets independently: active=true, not owner */
    test_badge_set_item(&items[1], 2, "focused", true,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);
    /* Waiting session retains gen_tps independently of ownership */
    test_badge_set_item(&items[2], 3, "waiting", false,
                        DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL,
                        false, false, 0, 0, 0.0, 24.5, "RWBX", false);
    /* Non-owner, no attention → ■ */
    test_badge_set_item(&items[3], 4, "bg", false,
                        DS4_AGENT_SUBAGENT_STATE_RUNNING,
                        false, false, 0, 0, 0.0, 0.0, "RWBX", false);

    char v[256], s[256];

    /* Item 0: queued output overrides owner → ! with magenta brackets */
    agent_format_badge(&items[0], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "!") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "▶") == NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);

    /* Item 1: focused non-owner → ■ with magenta brackets */
    agent_format_badge(&items[1], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "▶") == NULL);
    AGENT_TEST_ASSERT(strstr(v, "!") == NULL);
    AGENT_TEST_ASSERT(strstr(s, "\x1b[35m[") != NULL);

    /* Item 2: waiting with gen_tps → gen field, no ownership influence */
    agent_format_badge(&items[2], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "gen") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);  /* non-owner indicator */

    /* Item 3: plain non-owner → ■ */
    agent_format_badge(&items[3], v, sizeof(v), s, sizeof(s));
    AGENT_TEST_ASSERT(strstr(v, "■") != NULL);
    AGENT_TEST_ASSERT(strstr(v, "▶") == NULL);
    AGENT_TEST_ASSERT(strstr(v, "!") == NULL);
}

static void test_agent_alt_tab_sequence_is_consumed(void) {
    static const char alt_tab[] = "\x1b\t";
    struct linenoiseState l = {0};

    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, "\t", 1) == 0);
    AGENT_TEST_ASSERT(!linenoise_take_queued_sequence(&l, alt_tab,
                                                      sizeof(alt_tab) - 1));
    AGENT_TEST_ASSERT(l.queued_input_len == 1);
    AGENT_TEST_ASSERT(l.queued_input[0] == '\t');
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char mixed[] = {'a', '\x1b', '\t', 'b'};
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, mixed, sizeof(mixed)) == 0);
    AGENT_TEST_ASSERT(linenoise_take_queued_alt_tab(&l));
    AGENT_TEST_ASSERT(l.queued_input_len == 2);
    AGENT_TEST_ASSERT(l.queued_input[0] == 'a');
    AGENT_TEST_ASSERT(l.queued_input[1] == 'b');
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char backtab[] = "\x1b[Z";
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, backtab,
                                              sizeof(backtab) - 1) == 0);
    AGENT_TEST_ASSERT(linenoise_take_queued_alt_tab(&l));
    AGENT_TEST_ASSERT(l.queued_input_len == 0);
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char kitty_alt_tab[] = "\x1b[9;3u";
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, kitty_alt_tab,
                                              sizeof(kitty_alt_tab) - 1) == 0);
    AGENT_TEST_ASSERT(linenoise_take_queued_alt_tab(&l));
    AGENT_TEST_ASSERT(l.queued_input_len == 0);
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char xterm_alt_tab[] = "\x1b[27;3;9~";
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, xterm_alt_tab,
                                              sizeof(xterm_alt_tab) - 1) == 0);
    AGENT_TEST_ASSERT(linenoise_take_queued_alt_tab(&l));
    AGENT_TEST_ASSERT(l.queued_input_len == 0);
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char partial_alt_tab[] = "\x1b[27;3";
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, partial_alt_tab,
                                              sizeof(partial_alt_tab) - 1) == 0);
    AGENT_TEST_ASSERT(linenoise_queued_alt_tab_prefix_pending(&l));
    AGENT_TEST_ASSERT(!linenoise_take_queued_alt_tab(&l));
    free(l.queued_input);
    memset(&l, 0, sizeof(l));

    const char unrelated_escape[] = "\x1bX";
    AGENT_TEST_ASSERT(linenoiseEditQueueInput(&l, unrelated_escape,
                                              sizeof(unrelated_escape) - 1) == 0);
    AGENT_TEST_ASSERT(!linenoise_queued_alt_tab_prefix_pending(&l));
    AGENT_TEST_ASSERT(!linenoise_take_queued_alt_tab(&l));
    free(l.queued_input);
}

/* What: verify agent_docker_exec() rejects null or empty command inputs.
 * Why: Docker helpers should fail without forking when callers pass invalid
 * argv/config.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_exec_rejects_invalid_input(void) {
    agent_config cfg = {0};
    const char *const empty_argv[] = {NULL};
    const char *const nonempty_argv[] = {"ps", NULL};
    agent_buf out = {0};
    int status = 0;

    /* NULL cfg returns false */
    AGENT_TEST_ASSERT(!agent_docker_exec(NULL, NULL, nonempty_argv,
                                         NULL, 0, false, "test", &out, &status));
    /* NULL argv returns false */
    AGENT_TEST_ASSERT(!agent_docker_exec(NULL, &cfg, NULL,
                                         NULL, 0, false, "test", &out, &status));
    /* Empty argv (first element NULL) returns false */
    AGENT_TEST_ASSERT(!agent_docker_exec(NULL, &cfg, empty_argv,
                                         NULL, 0, false, "test", &out, &status));
    /* No fork happened - out is still zeroed */
    AGENT_TEST_ASSERT(out.ptr == NULL);
    AGENT_TEST_ASSERT(out.len == 0);
    free(out.ptr);
}

/* What: verify agent_docker_exec() handles out == NULL on an unavailable CLI.
 * Why: some command paths do not capture output and must still fail cleanly.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_exec_no_capture(void) {
    agent_config cfg = {0};
    cfg.docker_command = "/nonexistent/docker";
    const char *const argv[] = {"ps", NULL};
    int status = 0;

    /* out == NULL, command does not exist - should return false without crashing */
    AGENT_TEST_ASSERT(!agent_docker_exec(NULL, &cfg, argv,
                                         NULL, 0, false, "test", NULL, &status));
}

/* What: verify Docker debug logging prints the command when no worker exists.
 * Why: startup/config helpers use stdout debug logging instead of agent_publish.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_exec_debug_stdout(void) {
    /* Capture stdout to check debug output */
    int pipe_fds[2];
    AGENT_TEST_ASSERT(pipe(pipe_fds) == 0);
    int saved_stdout = dup(STDOUT_FILENO);
    AGENT_TEST_ASSERT(saved_stdout >= 0);
    dup2(pipe_fds[1], STDOUT_FILENO);
    close(pipe_fds[1]);

    agent_config cfg = {0};
    cfg.docker_command = "/nonexistent/docker";
    cfg.docker_debug = true;
    const char *const argv[] = {"ps", NULL};
    int status = 0;

    /* w == NULL, debug enabled - should print to stdout before failing */
    agent_docker_exec(NULL, &cfg, argv, NULL, 0, false, "test", NULL, &status);

    /* Restore stdout */
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);

    /* Read captured output */
    char buf[256] = {0};
    ssize_t n = read(pipe_fds[0], buf, sizeof(buf) - 1);
    close(pipe_fds[0]);
    AGENT_TEST_ASSERT(n > 0);
    AGENT_TEST_ASSERT(strstr(buf, "[docker debug]") != NULL);
    AGENT_TEST_ASSERT(strstr(buf, "docker") != NULL);
    AGENT_TEST_ASSERT(strstr(buf, "ps") != NULL);
}

/* What: verify agent_docker_exec() accepts non-NULL stdin with length zero.
 * Why: Docker file writes need to create empty files without treating stdin as
 * absent.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_exec_zero_length_stdin(void) {
    /* Use a simple command that reads stdin and exits.  Since docker is not
     * available, we use /bin/sh -c "cat > /dev/null" as a fake docker command
     * that accepts stdin and succeeds. */
    agent_config cfg = {0};
    cfg.docker_command = "/bin/sh";
    const char *const argv[] = {"-c", "cat > /dev/null", NULL};
    agent_buf out = {0};
    int status = 0;

    /* stdin_data is non-NULL with zero length - should succeed */
    const char *empty = "";
    AGENT_TEST_ASSERT(agent_docker_exec(NULL, &cfg, argv,
                                        empty, 0, false, "test", &out, &status));
    free(out.ptr);
}

/* What: verify large stdin is streamed while stdout is drained.
 * Why: pipe-heavy Docker commands must not deadlock when stdout fills before
 * stdin is fully written.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_exec_large_stdin_poll_loop(void) {
    agent_config cfg = {0};
    cfg.docker_command = "/bin/sh";
    const char *const argv[] = {
        "-c", "n=$(wc -c); printf '%s' \"$n\"", NULL
    };
    size_t data_len = 192 * 1024;
    char *data = xmalloc(data_len);
    memset(data, 'x', data_len);
    agent_buf out = {0};
    int status = 0;

    AGENT_TEST_ASSERT(agent_docker_exec(NULL, &cfg, argv, data, data_len,
                                        false, "test", &out, &status));
    AGENT_TEST_ASSERT(out.ptr && strstr(out.ptr, "196608") != NULL);
    free(out.ptr);
    free(data);
}

/* What: verify agent_docker_capture() rejects a null config.
 * Why: wrapper validation should fail before delegating to agent_docker_exec().
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_capture_wrapper_rejects_null_cfg(void) {
    char *argv[] = {(char *)"docker", "ps", NULL};
    agent_buf out = {0};
    AGENT_TEST_ASSERT(!agent_docker_capture(NULL, "docker", argv, "test", &out));
    AGENT_TEST_ASSERT(out.ptr == NULL);
}

/* What: verify agent_docker_capture() rejects a null Docker command.
 * Why: management helpers pass the command explicitly and need predictable
 * validation.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_capture_wrapper_rejects_null_docker_command(void) {
    agent_config cfg = {0};
    char *argv[] = {(char *)"docker", "ps", NULL};
    agent_buf out = {0};
    AGENT_TEST_ASSERT(!agent_docker_capture(&cfg, NULL, argv, "test", &out));
    AGENT_TEST_ASSERT(out.ptr == NULL);
}

/* What: verify agent_docker_capture() rejects a null argv.
 * Why: malformed management command construction should not fork.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_capture_wrapper_rejects_null_argv(void) {
    agent_config cfg = {0};
    agent_buf out = {0};
    AGENT_TEST_ASSERT(!agent_docker_capture(&cfg, "docker", NULL, "test", &out));
    AGENT_TEST_ASSERT(out.ptr == NULL);
}

/* What: verify agent_docker_capture() rejects an empty argv.
 * Why: the wrapper cannot derive argv_tail when argv[0] is absent.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_capture_wrapper_rejects_empty_argv(void) {
    agent_config cfg = {0};
    char *argv[] = {NULL};
    agent_buf out = {0};
    AGENT_TEST_ASSERT(!agent_docker_capture(&cfg, "docker", argv, "test", &out));
    AGENT_TEST_ASSERT(out.ptr == NULL);
}

/* --- agent_docker_shell tests --- */

/* What: verify persistent-shell startup fails without a selected container.
 * Why: filesystem tools must not silently fall back to host paths when Docker
 * state is incomplete.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_start_rejects_missing_container(void) {
    agent_worker w = {0};
    agent_config cfg = {0};
    w.cfg = &cfg;
    /* No container set */
    AGENT_TEST_ASSERT(!agent_docker_shell_start(&w));
    AGENT_TEST_ASSERT(!w.docker_shell.active);
}

/* What: verify persistent-shell startup fails when Docker is unavailable.
 * Why: sandbox commands should honor docker_available before forking.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_start_rejects_unavailable_docker(void) {
    agent_worker w = {0};
    agent_config cfg = {0};
    cfg.docker_available = false;
    cfg.docker_container = "test";
    w.cfg = &cfg;
    AGENT_TEST_ASSERT(!agent_docker_shell_start(&w));
    AGENT_TEST_ASSERT(!w.docker_shell.active);
}

static char *test_agent_startup_docker_capture_output(agent_config *cfg,
                                                      const char *input_text) {
    FILE *input = tmpfile();
    FILE *output = tmpfile();
    AGENT_TEST_ASSERT(output != NULL);
    if (input_text) {
        AGENT_TEST_ASSERT(input != NULL);
        AGENT_TEST_ASSERT(fputs(input_text, input) >= 0);
        rewind(input);
    }

    agent_config_prepare_startup_docker_sandbox_with_streams(cfg, input, output);

    if (input) fclose(input);
    fflush(output);
    rewind(output);

    agent_buf captured = {0};
    char chunk[256];
    size_t nread = 0;
    while ((nread = fread(chunk, 1, sizeof(chunk), output)) > 0)
        agent_buf_append_full(&captured, chunk, nread);
    fclose(output);
    return agent_buf_take(&captured);
}

static void test_agent_startup_docker_prompt_skip_leaves_no_active_sandbox(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\nbeta\n", "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, "n\n");

    AGENT_TEST_ASSERT(cfg.docker_container == NULL);
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Select docker sandbox for this launch:") != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Choose 1-2 or n/skip:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_noninteractive_autoloads_first_sandbox(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\nbeta\n", "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.non_interactive = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, NULL);

    AGENT_TEST_ASSERT(cfg.docker_container != NULL);
    AGENT_TEST_ASSERT(!strcmp(cfg.docker_container, "alpha"));
    AGENT_TEST_ASSERT(captured == NULL || strstr(captured, "Select docker sandbox for this launch:") == NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_prompt_selects_running_sandbox(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\n", "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, "1\n");

    AGENT_TEST_ASSERT(cfg.docker_container != NULL);
    AGENT_TEST_ASSERT(!strcmp(cfg.docker_container, "alpha"));
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Select docker sandbox for this launch:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_prompt_starts_stopped_sandbox(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\n", "exited\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, "1\n");

    AGENT_TEST_ASSERT(cfg.docker_container != NULL);
    AGENT_TEST_ASSERT(!strcmp(cfg.docker_container, "alpha"));
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Select docker sandbox for this launch:") != NULL);
    FILE *fp = fopen(env.state_path, "r");
    AGENT_TEST_ASSERT(fp != NULL);
    if (fp) {
        char buf[32] = {0};
        AGENT_TEST_ASSERT(fgets(buf, sizeof(buf), fp) != NULL);
        AGENT_TEST_ASSERT(!strcmp(buf, "running\n"));
        fclose(fp);
    }
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_activation_failure_leaves_no_sandbox(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\n", "exited\n", true);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, "1\n");

    AGENT_TEST_ASSERT(cfg.docker_container == NULL);
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "startup docker sandbox activation failed:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_prompt_selects_two_digit_choice(void) {
    /* Create ten sandbox names so that entry 10 is valid */
    agent_fake_docker_env env;
    char names[256] = {0};
    for (int i = 0; i < 10; i++) {
        char tag[16];
        snprintf(tag, sizeof(tag), "sb%d\n", i + 1);
        if (i > 0) strlcat(names, tag, sizeof(names));
        else strlcpy(names, tag, sizeof(names));
    }
    agent_test_fake_docker_setup(&env, names, "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    char *captured = test_agent_startup_docker_capture_output(&cfg, "10\n");

    AGENT_TEST_ASSERT(cfg.docker_container != NULL);
    AGENT_TEST_ASSERT(!strcmp(cfg.docker_container, "sb10"));
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Select docker sandbox for this launch:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_prompt_rejects_partial_numeric_token(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\nbeta\n", "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    /* "10abc" should be rejected and the prompt loops */
    char *captured = test_agent_startup_docker_capture_output(&cfg, "10abc\nn\n");

    AGENT_TEST_ASSERT(cfg.docker_container == NULL);
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Choose 1-2 or n/skip:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

static void test_agent_startup_docker_prompt_rejects_out_of_range_number(void) {
    agent_fake_docker_env env;
    agent_test_fake_docker_setup(&env, "alpha\nbeta\n", "running\n", false);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_auto = true;
    cfg.docker_command = env.docker_path;

    /* "0" (below range) and "3" (above count 2) both rejected, then skip */
    char *captured = test_agent_startup_docker_capture_output(&cfg, "0\n3\nn\n");

    AGENT_TEST_ASSERT(cfg.docker_container == NULL);
    AGENT_TEST_ASSERT(captured != NULL);
    AGENT_TEST_ASSERT(strstr(captured, "Choose 1-2 or n/skip:") != NULL);
    free(captured);
    agent_test_fake_docker_cleanup(&env);
}

/* What: verify persistent-shell exec rejects inactive shell state.
 * Why: callers should get false instead of blocking on invalid pipe fds.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_exec_rejects_inactive_shell(void) {
    agent_worker w = {0};
    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(!agent_docker_shell_exec(&w, "echo hello", false, "test", &out, &status));
    AGENT_TEST_ASSERT(out.ptr == NULL);
    free(out.ptr);
}

/* What: verify persistent-shell exec captures output and parses exit status.
 * Why: sentinel parsing is the contract that makes the reused shell safe.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_parses_output_and_exit_code(void) {
    /* Use a real /bin/sh to test the sentinel protocol.  We fork a shell,
     * then send it a command via agent_docker_shell_exec and verify the
     * output and exit code. */
    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        /* Start a real shell */
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_worker w = {0};
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    /* Test: echo hello, should output "hello" and exit 0 */
    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(&w, "echo hello", false, "test", &out, &status));
    AGENT_TEST_ASSERT(out.ptr != NULL);
    AGENT_TEST_ASSERT(out.len > 0);
    AGENT_TEST_ASSERT(strstr(out.ptr, "hello") != NULL);
    AGENT_TEST_ASSERT(status == 0);

    /* Test: return a non-zero exit code */
    agent_buf out2 = {0};
    int status2 = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(&w, "false", false, "test", &out2, &status2));
    AGENT_TEST_ASSERT(status2 != 0);

    free(out.ptr);
    free(out2.ptr);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

/* What: verify persistent Docker shell output is captured but not mirrored.
 * Why: file-tool helpers use the persistent shell for path resolution and
 * parsing protocols; /command_output should not display those internals.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_command_output_skips_blank_only(void) {
    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.command_output = true};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(&w, "printf '\\n\\n'",
                                              false, "test", &out, &status));
    AGENT_TEST_ASSERT(status == 0);
    AGENT_TEST_ASSERT(w.out == NULL);

    agent_buf path_out = {0};
    int path_status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(&w,
                                              "printf '/tmp/file\\n/tmp\\n'",
                                              false, "test", &path_out,
                                              &path_status));
    AGENT_TEST_ASSERT(path_status == 0);
    AGENT_TEST_ASSERT(path_out.ptr != NULL);
    AGENT_TEST_ASSERT(strstr(path_out.ptr, "/tmp/file") != NULL);
    AGENT_TEST_ASSERT(w.out == NULL);

    free(out.ptr);
    free(path_out.ptr);
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
    pthread_mutex_destroy(&w.mu);
}

/* What: verify split sentinel prefixes stay pending across reads.
 * Why: a sentinel split across pipe chunks must not be emitted as output and
 * cause the next read to hang.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_sentinel_tail_survives_split(void) {
    const char *sentinel = "__DS4_DONE_17_4__:";
    agent_buf pending = {0};
    agent_buf out = {0};
    agent_buf_append_full(&pending, "payload\n__DS4_DO",
                          strlen("payload\n__DS4_DO"));

    const char *incomplete = NULL;
    const char *complete = agent_docker_shell_find_complete_sentinel(
        pending.ptr, pending.len, sentinel, strlen(sentinel), NULL,
        &incomplete);
    AGENT_TEST_ASSERT(complete == NULL);
    AGENT_TEST_ASSERT(incomplete == NULL);
    size_t keep = agent_docker_shell_sentinel_tail_len(
        pending.ptr, pending.len, sentinel, strlen(sentinel));
    AGENT_TEST_ASSERT(keep == strlen("__DS4_DO"));
    agent_buf_append_full(&out, pending.ptr, pending.len - keep);
    agent_buf_discard_prefix(&pending, pending.len - keep);
    AGENT_TEST_ASSERT(!strcmp(pending.ptr, "__DS4_DO"));

    agent_buf not_line_start = {0};
    agent_buf_append_full(&not_line_start, "payload__DS4_DO",
                          strlen("payload__DS4_DO"));
    keep = agent_docker_shell_sentinel_tail_len(
        not_line_start.ptr, not_line_start.len, sentinel, strlen(sentinel));
    AGENT_TEST_ASSERT(keep == 0);
    free(not_line_start.ptr);

    agent_buf_append_full(&pending, "NE_17_4__:7\n", strlen("NE_17_4__:7\n"));
    int rc = -1;
    complete = agent_docker_shell_find_complete_sentinel(
        pending.ptr, pending.len, sentinel, strlen(sentinel), &rc,
        &incomplete);
    AGENT_TEST_ASSERT(complete == pending.ptr);
    AGENT_TEST_ASSERT(incomplete == NULL);
    AGENT_TEST_ASSERT(rc == 7);
    AGENT_TEST_ASSERT(out.ptr && !strcmp(out.ptr, "payload\n"));

    free(pending.ptr);
    free(out.ptr);
}

/* What: verify sentinel parsing loops through malformed candidates.
 * Why: a bogus line-start marker should not pin pending output until the real
 * command terminator arrives.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_sentinel_skips_invalid_candidates(void) {
    const char *sentinel = "__DS4_DONE_17_4__:";
    const char *data =
        "payload\n"
        "__DS4_DONE_17_4__:oops\n"
        "middle\n"
        "__DS4_DONE_17_4__:42\n";
    const char *expected = strstr(data, "__DS4_DONE_17_4__:42\n");
    int rc = -1;

    const char *complete = agent_docker_shell_find_complete_sentinel(
        data, strlen(data), sentinel, strlen(sentinel), &rc, NULL);
    AGENT_TEST_ASSERT(complete == expected);
    AGENT_TEST_ASSERT(rc == 42);

    const char *incomplete_data =
        "payload\n"
        "__DS4_DONE_17_4__:4";
    const char *incomplete = NULL;
    complete = agent_docker_shell_find_complete_sentinel(
        incomplete_data, strlen(incomplete_data), sentinel, strlen(sentinel),
        NULL, &incomplete);
    AGENT_TEST_ASSERT(complete == NULL);
    AGENT_TEST_ASSERT(incomplete == incomplete_data + strlen("payload\n"));
}

/* What: verify argv values are shell-quoted before persistent-shell execution.
 * Why: paths and arguments with spaces or metacharacters must stay literal in
 * Docker filesystem commands.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_argv_conversion(void) {
    /* Test that agent_docker_shell_exec_argv properly shell-quotes arguments
     * with spaces, quotes, dollar signs, and semicolons, using a real shell. */
    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        /* Start a real shell */
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_worker w = {0};
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    /* Arguments with spaces, quotes, dollar signs, and semicolons */
    char *const argv[] = {
        (char *)"echo",
        (char *)"hello world",
        (char *)"it's fine",
        (char *)"$VAR",
        (char *)"a;b",
        NULL
    };
    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec_argv(&w, argv, false, "test", &out, &status));
    AGENT_TEST_ASSERT(out.ptr != NULL);
    AGENT_TEST_ASSERT(out.len > 0);
    AGENT_TEST_ASSERT(status == 0);
    /* The output should contain the shell-quoted arguments */
    AGENT_TEST_ASSERT(strstr(out.ptr, "hello world") != NULL);
    AGENT_TEST_ASSERT(strstr(out.ptr, "it's fine") != NULL);
    AGENT_TEST_ASSERT(strstr(out.ptr, "$VAR") != NULL);
    AGENT_TEST_ASSERT(strstr(out.ptr, "a;b") != NULL);

    agent_buf no_nl = {0};
    int no_nl_status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(&w, "printf hello",
                                              false, "test", &no_nl,
                                              &no_nl_status));
    AGENT_TEST_ASSERT(no_nl_status == 0);
    AGENT_TEST_ASSERT(no_nl.ptr && !strcmp(no_nl.ptr, "hello"));

    free(out.ptr);
    free(no_nl.ptr);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

/* What: verify argv-based Docker shell commands publish debug text on failure.
 * Why: `/docker debug` must explain the emitted command even when command
 * execution fails.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_exec_argv_debug_publish(void) {
    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.docker_debug = true};
    agent_worker w = {0};
     w.cfg = &cfg;
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cond, NULL);
    pthread_mutex_init(&w.docker_shell.mu, NULL);
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);

    char *const argv[] = {
        (char *)"cat",
        (char *)"/tmp/example file.txt",
        NULL
    };
    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec_argv(&w, argv, false,
                                                  "test", &out, &status));
    AGENT_TEST_ASSERT(status != 0);
    AGENT_TEST_ASSERT(w.out != NULL);
    AGENT_TEST_ASSERT(strstr(w.out, "[docker debug]") != NULL);
    AGENT_TEST_ASSERT(strstr(w.out, "cat '/tmp/example file.txt'") != NULL);

    free(out.ptr);
    free(w.out);
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
    pthread_cond_destroy(&w.cond);
    pthread_mutex_destroy(&w.mu);
}

/* What: verify unbounded persistent-shell output is not capped.
 * Why: full-file Docker reads need exact bytes beyond the normal display cap.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_unbounded_output(void) {
    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_worker w = {0};
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    agent_buf out = {0};
    int status = 0;
    AGENT_TEST_ASSERT(agent_docker_shell_exec(
        &w,
        "dd if=/dev/zero bs=1024 count=160 2>/dev/null | tr '\\000' x",
        true, "test", &out, &status));
    AGENT_TEST_ASSERT(status == 0);
    AGENT_TEST_ASSERT(out.len == 160 * 1024);
    AGENT_TEST_ASSERT(!out.truncated);

    free(out.ptr);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

/* What: verify Docker range reads slice inside the persistent shell.
 * Why: ranged reads should return line metadata and set `more` state without
 * copying the whole file.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_read_range_uses_shell_slice(void) {
    char path[] = "/tmp/ds4-agent-read-range-XXXXXX";
    int fd = mkstemp(path);
    AGENT_TEST_ASSERT(fd >= 0);
    FILE *f = fdopen(fd, "w");
    AGENT_TEST_ASSERT(f != NULL);
    for (int i = 1; i <= 1200; i++)
        fprintf(f, "line%04d\n", i);
    fclose(f);

    int stdin_pipe[2];
    int stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    char *result = agent_read_range(&w, path, 501, 5, false, false, true);
    AGENT_TEST_ASSERT(result != NULL);
    AGENT_TEST_ASSERT(strstr(result, "lines 501-505 of 1200") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "501 line0501") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "505 line0505") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "500 line0500") == NULL);
    AGENT_TEST_ASSERT(w.more_valid);
    AGENT_TEST_ASSERT(w.more_next_line == 506);
    free(result);

    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
    unlink(path);
}

/* What: verify mount refresh skips work when the fingerprint is unchanged.
 * Why: refreshing mounts can recreate containers, so the fast path must be
 * cheap and non-destructive.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_refresh_mounts_skips_unchanged_fingerprint(void) {
    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_command = "/definitely/not/docker";
    cfg.docker_container = "ds4-test";
    snprintf(cfg.temp_directory, sizeof(cfg.temp_directory), "%s", "/tmp");

    agent_worker w = {0};
    w.cfg = &cfg;
    agent_path_list_append(&w.working_directories, "/tmp");
    agent_docker_mount_fingerprint(&w, w.docker_mount_fingerprint,
                                   sizeof(w.docker_mount_fingerprint));

    char err[256] = {0};
    AGENT_TEST_ASSERT(agent_docker_refresh_mounts(&w, err, sizeof(err)));
    AGENT_TEST_ASSERT(err[0] == '\0');
    agent_path_list_free(&w.working_directories);
}

static void test_agent_bash_publish_observation_command_output_on(void) {
    agent_config cfg = {.command_output = true};
    agent_worker w = {0};
    w.cfg = &cfg;
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);

    const char *obs =
        "bash job=1 pid=42 status=done elapsed_sec=0.1 timed_out=0\n"
        "exit_status=0\n"
        "<output>\n"
        "</output>\n";
    agent_bash_publish_observation(&w, obs);
    AGENT_TEST_ASSERT(w.out != NULL);
    AGENT_TEST_ASSERT(strstr(w.out, "\x1b[90m[bash completed with no output]\n\x1b[0m") != NULL);

    free(w.out);
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_mutex_destroy(&w.mu);
}

static void test_agent_bash_publish_observation_command_output_off(void) {
    agent_config cfg = {.command_output = false};
    agent_worker w = {0};
    w.cfg = &cfg;
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);

    const char *obs =
        "bash job=1 pid=42 status=done elapsed_sec=0.1 timed_out=0\n"
        "exit_status=0\n"
        "<output>\n"
        "hidden\n"
        "</output>\n";
    agent_bash_publish_observation(&w, obs);
    AGENT_TEST_ASSERT(w.out == NULL);

    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_mutex_destroy(&w.mu);
}

static void test_agent_bash_publish_observation_skips_nonempty_body(void) {
    agent_config cfg = {.command_output = true};
    agent_worker w = {0};
    w.cfg = &cfg;
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);

    const char *obs =
        "bash job=1 pid=42 status=running elapsed_sec=0.1 timeout_sec=5\n"
        "output_path=/tmp/ds4_agent_output_test (12 bytes, 1 lines)\n"
        "<tail -4 /tmp/ds4_agent_output_test>\n"
        "old-output\n"
        "</tail>\n";
    agent_bash_publish_observation(&w, obs);
    AGENT_TEST_ASSERT(w.out == NULL);

    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_mutex_destroy(&w.mu);
}

/* What: verify Docker-backed bash jobs mirror command output when enabled.
 * Why: bash-in-Docker uses a direct exec path and must preserve terminal output
 * behavior shared with local bash.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_bash_publishes_command_output(void) {
    char docker_path[] = "/tmp/ds4-agent-fake-docker-XXXXXX";
    int fd = mkstemp(docker_path);
    AGENT_TEST_ASSERT(fd >= 0);
    const char *script =
        "#!/bin/sh\n"
        "while [ \"$#\" -gt 0 ] && [ \"$1\" != \"/bin/sh\" ]; do shift; done\n"
        "if [ \"$1\" != \"/bin/sh\" ]; then echo missing shell >&2; exit 127; fi\n"
        "shift\n"
        "if [ \"$1\" = \"-c\" ]; then shift; exec /bin/sh -c \"$1\"; fi\n"
        "exec /bin/sh \"$@\"\n";
    write_all(fd, script, strlen(script));
    close(fd);
    AGENT_TEST_ASSERT(chmod(docker_path, 0700) == 0);

    agent_config cfg = {0};
    cfg.docker_available = true;
    cfg.docker_container = "fake-container";
    cfg.docker_command = docker_path;
    cfg.command_output = true;
    snprintf(cfg.temp_directory, sizeof(cfg.temp_directory), "%s", "/tmp");

    agent_worker w = {0};
    w.cfg = &cfg;
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    pthread_mutex_init(&w.mu, NULL);
    char err[160] = {0};
    agent_bash_job *job = agent_bash_start_mode(&w, "printf docker-bash-output",
                                                5, true, err, sizeof(err));
    AGENT_TEST_ASSERT(job != NULL);
    if (job) {
        char *obs = agent_bash_job_tool_result(&w, job, true, 2, false, true);
        AGENT_TEST_ASSERT(obs != NULL);
        AGENT_TEST_ASSERT(strstr(obs, "docker-bash-output") != NULL);
        AGENT_TEST_ASSERT(w.out != NULL);
        AGENT_TEST_ASSERT(strstr(w.out, "\x1b[90mdocker-bash-output\x1b[0m\n") != NULL);
        free(obs);
    }

    free(w.out);
    agent_bash_jobs_free(&w);
    agent_temp_files_cleanup(&w, agent_now_sec() + AGENT_TEMP_FILE_IDLE_SECONDS + 1);
    agent_temp_files_free(&w.temp_files);
    agent_path_list_free(&w.auto_allowed_paths);
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_mutex_destroy(&w.mu);
    unlink(docker_path);
}

/* What: verify debug formatting shows the full `docker exec` command.
 * Why: Docker debug output is used to diagnose env, workdir, and shell-command
 * parity issues.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_debug_argv_includes_docker_exec(void) {
    char home_env[PATH_MAX + 16];
    char tmpdir_env[PATH_MAX + 16];
    char tmp_env[PATH_MAX + 16];
    char temp_env[PATH_MAX + 16];
    char xdg_config_env[PATH_MAX + 32];
    char xdg_cache_env[PATH_MAX + 32];
    char *argv[48];
    int argc = 0;
    agent_docker_build_shell_argv(argv, &argc, "docker", "ds4_c",
                                  "/work", "/tmp", "echo hi",
                                  home_env, sizeof(home_env),
                                  tmpdir_env, sizeof(tmpdir_env),
                                  tmp_env, sizeof(tmp_env),
                                  temp_env, sizeof(temp_env),
                                  xdg_config_env, sizeof(xdg_config_env),
                                  xdg_cache_env, sizeof(xdg_cache_env));
    AGENT_TEST_ASSERT(argc > 0);
    char *cmd = agent_docker_debug_command_text(argv);
    AGENT_TEST_ASSERT(cmd != NULL);
    if (cmd) {
        AGENT_TEST_ASSERT(strstr(cmd, "docker exec -i") != NULL);
        AGENT_TEST_ASSERT(strstr(cmd, "-w /work") != NULL);
        AGENT_TEST_ASSERT(strstr(cmd, "ds4_c /bin/sh -c 'echo hi'") != NULL);
        free(cmd);
    }
}

/* What: verify stopping the persistent shell twice is harmless.
 * Why: cleanup paths may stop shells after failed starts, sandbox switches, or
 * worker teardown.
 * Callers: ds4_agent_unit_tests_run(). */
static void test_agent_docker_shell_stop_is_idempotent(void) {
    agent_worker w = {0};
    w.docker_shell.stdin_fd = 3;
    w.docker_shell.stdout_fd = 4;
    w.docker_shell.pid = 12345;
    w.docker_shell.active = true;

    agent_docker_shell_stop(&w);
    AGENT_TEST_ASSERT(w.docker_shell.stdin_fd == -1);
    AGENT_TEST_ASSERT(w.docker_shell.stdout_fd == -1);
    AGENT_TEST_ASSERT(!w.docker_shell.active);

    /* Second call should be harmless */
    agent_docker_shell_stop(&w);
    AGENT_TEST_ASSERT(w.docker_shell.stdin_fd == -1);
    AGENT_TEST_ASSERT(w.docker_shell.stdout_fd == -1);
}

/* --- agent_tool_list / agent_tool_search persistent shell tests --- */

static void test_agent_tool_list_with_fake_shell(void) {
    char root_tmpl[] = "/tmp/ds4_agent_list_root_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;
    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    char child_file[PATH_MAX];
    snprintf(child_file, sizeof(child_file), "%s/visible.txt", root);
    char err[256];
    AGENT_TEST_ASSERT(agent_write_file_bytes(child_file, "hello\n", 6,
                                             err, sizeof(err)) == 0);

    int stdin_pipe[2], stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    agent_path_list_append(&w.working_directories, root);
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    agent_tool_arg args[] = {
        {.name = "path", .value = ".", .is_string = true},
    };
    agent_tool_call call = {
        .name = "list",
        .args = args,
        .argc = 1,
    };
    char *result = agent_tool_list(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    AGENT_TEST_ASSERT(strstr(result, root) != NULL);
    free(result);

    agent_tool_arg missing_args[] = {
        {.name = "path", .value = "missing", .is_string = true},
    };
    agent_tool_call missing_call = {
        .name = "list",
        .args = missing_args,
        .argc = 1,
    };
    char *missing_result = agent_tool_list(&w, &missing_call);
    AGENT_TEST_ASSERT(missing_result != NULL);
    AGENT_TEST_ASSERT(strstr(missing_result, "Tool error:") != NULL);
    free(missing_result);

    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
    agent_path_list_free(&w.working_directories);
    unlink(child_file);
    rmdir(root);
}

static void test_agent_tool_search_with_fake_shell(void) {
    int stdin_pipe[2], stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    agent_tool_arg args[] = {
        {.name = "query", .value = "hello", .is_string = true},
        {.name = "path", .value = ".", .is_string = true},
    };
    agent_tool_call call = {
        .name = "search",
        .args = args,
        .argc = 2,
    };
    char *result = agent_tool_search(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    /* Should get "No matches" since there is no file with "hello" in /tmp */
    AGENT_TEST_ASSERT(strstr(result, "No matches") != NULL);
    free(result);

    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

static void test_agent_tool_list_fails_when_shell_inactive(void) {
    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.active = false;

    agent_tool_arg args[] = {
        {.name = "path", .value = ".", .is_string = true},
    };
    agent_tool_call call = {
        .name = "list",
        .args = args,
        .argc = 1,
    };
    char *result = agent_tool_list(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    AGENT_TEST_ASSERT(strstr(result, "Tool error") != NULL);
    free(result);
}

static void test_agent_tool_search_fails_when_shell_inactive(void) {
    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.active = false;

    agent_tool_arg args[] = {
        {.name = "query", .value = "hello", .is_string = true},
        {.name = "path", .value = ".", .is_string = true},
    };
    agent_tool_call call = {
        .name = "search",
        .args = args,
        .argc = 2,
    };
    char *result = agent_tool_search(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    AGENT_TEST_ASSERT(strstr(result, "Tool error") != NULL);
    free(result);
}

static void test_agent_tool_list_shell_argv_quoting(void) {
    int stdin_pipe[2], stdout_pipe[2];
    AGENT_TEST_ASSERT(pipe(stdin_pipe) == 0);
    AGENT_TEST_ASSERT(pipe(stdout_pipe) == 0);

    pid_t child = fork();
    if (child == 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        dup2(stdin_pipe[0], STDIN_FILENO);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        execl("/bin/sh", "sh", NULL);
        _exit(127);
    }
    AGENT_TEST_ASSERT(child > 0);
    close(stdin_pipe[0]);
    close(stdout_pipe[1]);

    agent_config cfg = {.docker_available = true, .docker_container = "test"};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.docker_shell.stdin_fd = stdin_pipe[1];
    w.docker_shell.stdout_fd = stdout_pipe[0];
    w.docker_shell.pid = child;
    w.docker_shell.seq = 0;
    w.docker_shell.active = true;
    pthread_mutex_init(&w.docker_shell.mu, NULL);

    /* Path with spaces, quotes, $, backticks, semicolons */
    agent_tool_arg args[] = {
        {.name = "path", .value = "/tmp/test dir'\"$`;", .is_string = true},
    };
    agent_tool_call call = {
        .name = "list",
        .args = args,
        .argc = 1,
    };
    char *result = agent_tool_list(&w, &call);
    AGENT_TEST_ASSERT(result != NULL);
    free(result);

    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

static void test_agent_docker_effective_working_directory_prefers_primary_root(void) {
    agent_worker w = {0};
    agent_path_list_append(&w.working_directories, "/tmp/primary-root");
    snprintf(w.status.workspace, sizeof(w.status.workspace), "%s",
             "/tmp/status-root");

    const char *working_dir = agent_docker_effective_working_directory(&w);
    AGENT_TEST_ASSERT(working_dir != NULL);
    AGENT_TEST_ASSERT(!strcmp(working_dir, "/tmp/primary-root"));

    agent_path_list_free(&w.working_directories);
}

static void test_agent_subagent_api_lifecycle(void) {
    ds4_agent_subagents *mgr = NULL;
    ds4_agent_subagent_options opt = {
        .default_context_size = 4096,
        .default_round_budget = 7,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagents_create(&mgr, NULL, &opt) == 0);

    ds4_agent_subagent_id alpha = {0};
    ds4_agent_subagent_create_request alpha_req = {
        .name = "alpha",
        .prompt = "inspect the tests",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS,
        .think_mode = DS4_THINK_MAX,
        .think_mode_set = true,
        .round_budget = 3,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &alpha_req, &alpha) == 0);
    AGENT_TEST_ASSERT(alpha.value == 1);

    ds4_agent_subagent_id beta = {0};
    ds4_agent_subagent_create_request beta_req = {
        .name = "beta",
        .autonomy = DS4_AGENT_SUBAGENT_AUTONOMY_TAB,
    };
    AGENT_TEST_ASSERT(ds4_agent_subagent_create(mgr, &beta_req, &beta) == 0);
    AGENT_TEST_ASSERT(beta.value == 2);

    ds4_agent_subagent_status st[4];
    size_t n = 0;
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, st, 4, &n) == 0);
    AGENT_TEST_ASSERT(n == 2);
    AGENT_TEST_ASSERT(!strcmp(st[0].name, "alpha"));
    AGENT_TEST_ASSERT(st[0].active);
    AGENT_TEST_ASSERT(st[0].autonomy == DS4_AGENT_SUBAGENT_AUTONOMY_AUTONOMOUS);
    AGENT_TEST_ASSERT(st[0].budget_limit == 3);
    AGENT_TEST_ASSERT(st[0].think_mode == DS4_THINK_MAX);
    AGENT_TEST_ASSERT(st[1].think_mode == DS4_THINK_MAX);

    AGENT_TEST_ASSERT(ds4_agent_subagent_switch(mgr, beta) == 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_send(mgr, beta, "queued prompt") == 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_stop(mgr, alpha) == 0);

    char report[256];
    AGENT_TEST_ASSERT(ds4_agent_subagent_report(mgr, alpha,
                                               report, sizeof(report)) == 0);
    AGENT_TEST_ASSERT(strstr(report, "No subagent report") != NULL);
    AGENT_TEST_ASSERT(ds4_agent_subagent_import_report(mgr, alpha, beta) == 0);

    ds4_agent_subagent_event ev;
    AGENT_TEST_ASSERT(ds4_agent_subagent_poll_event(mgr, &ev) == 1);
    AGENT_TEST_ASSERT(ev.type == DS4_AGENT_SUBAGENT_EVENT_CREATED);
    AGENT_TEST_ASSERT(!strcmp(ev.name, "alpha"));

    AGENT_TEST_ASSERT(ds4_agent_subagent_close(mgr, alpha) == 0);
    AGENT_TEST_ASSERT(ds4_agent_subagent_list(mgr, st, 4, &n) == 0);
    AGENT_TEST_ASSERT(n == 1);
    AGENT_TEST_ASSERT(!strcmp(st[0].name, "beta"));
    AGENT_TEST_ASSERT(st[0].active);
    ds4_agent_subagents_destroy(mgr);
}

static void test_agent_subagent_slash_command_recognition(void) {
    AGENT_TEST_ASSERT(agent_slash_command_known("/subagent"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/subagent new tests run"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/subagent new --thinking off tests run"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/subagent report tests"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/allow read,bash"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/disallow web"));
    AGENT_TEST_ASSERT(!agent_slash_command_known("/subagentry"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/save"));
}

#endif

/* --- Tool-access policy implementation ----------------------------------- */

/* Parse a comma-separated list of tool names / meta-tools into a normalized
 * policy.  Returns 0 on success, -1 on unknown tool name. */
static const char *agent_tool_name_for_index(int idx) {
    return idx >= 0 && idx < AGENT_TOOL_COUNT ? agent_tool_names[idx] : NULL;
}

static int agent_tool_index_for_name(const char *name) {
    if (!name) return -1;
    for (int i = 0; i < AGENT_TOOL_COUNT; i++) {
        const char *tool = agent_tool_name_for_index(i);
        if (tool && !strcmp(name, tool)) return i;
    }
    return -1;
}

static void ds4_agent_tool_policy_set_none(ds4_agent_tool_policy *pol) {
    if (!pol) return;
    memset(pol, 0, sizeof(*pol));
    pol->allow_none = true;
}

static void ds4_agent_tool_policy_set_all(ds4_agent_tool_policy *pol) {
    if (!pol) return;
    memset(pol, 0, sizeof(*pol));
    pol->allow_all = true;
}

static bool ds4_agent_tool_policy_any_allowed(const ds4_agent_tool_policy *pol) {
    if (!pol || pol->allow_none) return false;
    if (pol->allow_all) return true;
    for (int i = 0; i < AGENT_TOOL_COUNT; i++)
        if (pol->allowed[i]) return true;
    return false;
}

static void ds4_agent_tool_policy_allow_class(ds4_agent_tool_policy *pol,
                                              agent_tools_class cls) {
    if (!pol || cls < 0 || cls >= AGENT_TOOLS_CLASS_COUNT) return;
    int count = agent_tools_class_indices[cls][0];
    for (int j = 1; j <= count; j++) {
        int idx = agent_tools_class_indices[cls][j];
        if (idx >= 0 && idx < AGENT_TOOL_COUNT)
            pol->allowed[idx] = true;
    }
}

static bool ds4_agent_tool_policy_class_allowed(const ds4_agent_tool_policy *pol,
                                                agent_tools_class cls) {
    if (!pol || pol->allow_none || cls < 0 || cls >= AGENT_TOOLS_CLASS_COUNT)
        return false;
    if (pol->allow_all) return true;
    int count = agent_tools_class_indices[cls][0];
    for (int j = 1; j <= count; j++) {
        int idx = agent_tools_class_indices[cls][j];
        if (idx < 0 || idx >= AGENT_TOOL_COUNT ||
            !pol->allowed[idx])
            return false;
    }
    return true;
}

int ds4_agent_tool_policy_parse(const char *input, ds4_agent_tool_policy *out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    if (!input || !input[0]) { out->allow_none = true; return 0; }

    /* Handle single-word aliases. */
    if (!strcmp(input, "all"))  { out->allow_all = true; return 0; }
    if (!strcmp(input, "none") || !strcmp(input, "off")) { out->allow_none = true; return 0; }

    /* Tokenize comma-separated list. */
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s", input);
    char *save = NULL;
    for (char *tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        /* Strip leading/trailing whitespace. */
        while (*tok == ' ' || *tok == '\t') tok++;
        char *end = tok + strlen(tok) - 1;
        while (end > tok && (*end == ' ' || *end == '\t')) end--;
        end[1] = '\0';
        if (!*tok) continue;

        if (!strcmp(tok, "read")) {
            ds4_agent_tool_policy_allow_class(out, AGENT_TOOLS_CLASS_READ);
        } else if (!strcmp(tok, "write")) {
            ds4_agent_tool_policy_allow_class(out, AGENT_TOOLS_CLASS_WRITE);
        } else if (!strcmp(tok, "web")) {
            ds4_agent_tool_policy_allow_class(out, AGENT_TOOLS_CLASS_WEB);
        } else if (!strcmp(tok, "bash")) {
            ds4_agent_tool_policy_allow_class(out, AGENT_TOOLS_CLASS_BASH);
        } else {
            int idx = agent_tool_index_for_name(tok);
            if (idx < 0) return -1;
            out->allowed[idx] = true;
        }
    }
    if (!ds4_agent_tool_policy_any_allowed(out))
        out->allow_none = true;
    return 0;
}

bool ds4_agent_tool_policy_allows(const ds4_agent_tool_policy *pol, const char *tool_name) {
    if (!tool_name) return false;
    if (!pol) return false;
    if (pol->allow_all)  return true;
    if (pol->allow_none) return false;
    int idx = agent_tool_index_for_name(tool_name);
    if (idx < 0 || idx >= AGENT_TOOL_COUNT) return false;
    return pol->allowed[idx];
}

void ds4_agent_tool_policy_format(const ds4_agent_tool_policy *pol, char *buf, size_t len) {
    if (!buf || len == 0) return;
    if (!pol) { buf[0] = '\0'; return; }
    if (pol->allow_all)  { snprintf(buf, len, "all");  return; }
    if (pol->allow_none) { snprintf(buf, len, "none"); return; }
    bool covered[AGENT_TOOL_COUNT] = {0};
    char tmp[256] = "";
    int first = 1;

    const struct {
        agent_tools_class cls;
        const char *name;
    } classes[] = {
        {AGENT_TOOLS_CLASS_WRITE, "write"},
        {AGENT_TOOLS_CLASS_READ,  "read"},
        {AGENT_TOOLS_CLASS_WEB,   "web"},
        {AGENT_TOOLS_CLASS_BASH,  "bash"},
    };
    for (size_t k = 0; k < sizeof(classes) / sizeof(classes[0]); k++) {
        agent_tools_class cls = classes[k].cls;
        if (!ds4_agent_tool_policy_class_allowed(pol, cls)) continue;
        bool all_covered = true;
        int count = agent_tools_class_indices[cls][0];
        for (int j = 1; j <= count; j++) {
            int idx = agent_tools_class_indices[cls][j];
            if (idx < 0 || idx >= AGENT_TOOL_COUNT ||
                !covered[idx]) {
                all_covered = false;
                break;
            }
        }
        if (all_covered) continue;
        if (!first) strcat(tmp, ", ");
        first = 0;
        strcat(tmp, classes[k].name);
        for (int j = 1; j <= count; j++) {
            int idx = agent_tools_class_indices[cls][j];
            if (idx >= 0 && idx < AGENT_TOOL_COUNT)
                covered[idx] = true;
        }
    }

    for (int i = 0; i < AGENT_TOOL_COUNT; i++) {
        if (covered[i] || !pol->allowed[i]) continue;
        const char *tool = agent_tool_name_for_index(i);
        if (tool) {
            if (!first) strcat(tmp, ", ");
            first = 0;
            strcat(tmp, tool);
        }
    }
    if (tmp[0] == '\0') strcpy(tmp, "none");
    snprintf(buf, len, "%s", tmp);
}

static void ds4_agent_tool_policy_grant(ds4_agent_tool_policy *dst,
                                        const ds4_agent_tool_policy *grant) {
    if (!dst || !grant) return;
    if (grant->allow_all) {
        ds4_agent_tool_policy_set_all(dst);
        return;
    }
    if (grant->allow_none) {
        ds4_agent_tool_policy_set_none(dst);
        return;
    }
    if (dst->allow_all) return;
    dst->allow_none = false;
    for (int i = 0; i < AGENT_TOOL_COUNT; i++)
        if (grant->allowed[i]) dst->allowed[i] = true;
    if (!ds4_agent_tool_policy_any_allowed(dst))
        ds4_agent_tool_policy_set_none(dst);
}

static void ds4_agent_tool_policy_revoke(ds4_agent_tool_policy *dst,
                                         const ds4_agent_tool_policy *remove) {
    if (!dst || !remove) return;
    if (remove->allow_all || remove->allow_none) {
        ds4_agent_tool_policy_set_none(dst);
        return;
    }
    if (dst->allow_all) {
        dst->allow_all = false;
        dst->allow_none = false;
        for (int i = 0; i < AGENT_TOOL_COUNT; i++)
            dst->allowed[i] = !remove->allowed[i];
    } else if (!dst->allow_none) {
        for (int i = 0; i < AGENT_TOOL_COUNT; i++)
            if (remove->allowed[i]) dst->allowed[i] = false;
    }
    if (!ds4_agent_tool_policy_any_allowed(dst))
        ds4_agent_tool_policy_set_none(dst);
}

static bool agent_worker_apply_tool_policy_command(agent_worker *w,
                                                   bool grant,
                                                   const char *arg,
                                                   char *err,
                                                   size_t err_len) {
    if (err && err_len) err[0] = '\0';
    if (!w || !arg || !arg[0]) {
        if (err && err_len) snprintf(err, err_len, "missing tool policy");
        return false;
    }
    ds4_agent_tool_policy parsed;
    if (ds4_agent_tool_policy_parse(arg, &parsed) != 0) {
        if (err && err_len) snprintf(err, err_len, "invalid tool policy '%s'", arg);
        return false;
    }
    if (grant)
        ds4_agent_tool_policy_grant(&w->tool_policy, &parsed);
    else
        ds4_agent_tool_policy_revoke(&w->tool_policy, &parsed);
    return true;
}

#ifdef DS4_AGENT_TEST
static char *agent_tool_skill_list(agent_worker *w,
                                   const agent_tool_call *call);
static char *agent_expand_skill_references(agent_worker *w, const char *text);
static void agent_json_escape(agent_buf *out, const char *text);
static void agent_json_escape_n(agent_buf *out, const char *text,
                                size_t text_len);

static void test_agent_tool_policy_parse(void) {
    ds4_agent_tool_policy pol;
    char buf[256];
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("all", &pol) == 0);
    AGENT_TEST_ASSERT(pol.allow_all);
    AGENT_TEST_ASSERT(!pol.allow_none);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("none", &pol) == 0);
    AGENT_TEST_ASSERT(pol.allow_none);
    AGENT_TEST_ASSERT(!pol.allow_all);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("off", &pol) == 0);
    AGENT_TEST_ASSERT(pol.allow_none);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("", &pol) == 0);
    AGENT_TEST_ASSERT(pol.allow_none);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse(NULL, &pol) == 0);
    AGENT_TEST_ASSERT(pol.allow_none);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("read", &pol) == 0);
    AGENT_TEST_ASSERT(!pol.allow_all && !pol.allow_none);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "read"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "bash"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("read,bash", &pol) == 0);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "bash"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "write"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("read", &pol) == 0);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "more"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "list"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "search"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "write"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("write", &pol) == 0);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "write"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "edit"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "bash"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("bash", &pol) == 0);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "bash"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "bash_status"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "bash_stop"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "read"));
    /* meta-tool: web */
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("web", &pol) == 0);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "web_browse"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&pol, "web_fetch"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "read"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "bash"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&pol, "ask_question"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("ask_question", &pol) == -1);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_parse("nonexistent_tool", &pol) == -1);
    ds4_agent_tool_policy_parse("all", &pol);
    ds4_agent_tool_policy_format(&pol, buf, sizeof(buf));
    AGENT_TEST_ASSERT(!strcmp(buf, "all"));
    ds4_agent_tool_policy_parse("none", &pol);
    ds4_agent_tool_policy_format(&pol, buf, sizeof(buf));
    AGENT_TEST_ASSERT(!strcmp(buf, "none"));
    ds4_agent_tool_policy_parse("read,bash", &pol);
    ds4_agent_tool_policy_format(&pol, buf, sizeof(buf));
    AGENT_TEST_ASSERT(strstr(buf, "read") != NULL);
    AGENT_TEST_ASSERT(strstr(buf, "bash") != NULL);
    ds4_agent_tool_policy_parse("write", &pol);
    ds4_agent_tool_policy_format(&pol, buf, sizeof(buf));
    AGENT_TEST_ASSERT(!strcmp(buf, "write"));
}

static void test_agent_tool_policy_prompt_building(void) {
    ds4_agent_tool_policy pol;
    char *prompt;
    ds4_agent_tool_policy_parse("none", &pol);
    prompt = agent_build_filtered_tools_prompt(&pol);
    AGENT_TEST_ASSERT(prompt != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "## Tools") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Available tools: ask_question") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "\"name\": \"ask_question\"") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "\"choices\": {\"type\": \"array\"") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "## Editing files") == NULL);
    free(prompt);
    ds4_agent_tool_policy_parse("web", &pol);
    prompt = agent_build_filtered_tools_prompt(&pol);
    AGENT_TEST_ASSERT(prompt != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Available tools: ask_question, web_browse, web_fetch") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Use web_browse") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "## Editing files") == NULL);
    free(prompt);
    ds4_agent_tool_policy_parse("bash", &pol);
    prompt = agent_build_filtered_tools_prompt(&pol);
    AGENT_TEST_ASSERT(prompt != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Available tools: ask_question, bash, bash_status, bash_stop") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Run a shell command") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "## Editing files") == NULL);
    free(prompt);
    ds4_agent_tool_policy_parse("write", &pol);
    prompt = agent_build_filtered_tools_prompt(&pol);
    AGENT_TEST_ASSERT(prompt != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Available tools: ask_question, read, more, write, list, edit, search") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "## Editing files") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Use web_browse") == NULL);
    free(prompt);

    ds4_agent_tool_policy_parse("all", &pol);
    prompt = agent_build_filtered_tools_prompt(&pol);
    AGENT_TEST_ASSERT(prompt != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Available tools: ask_question, read, more, write, list, edit, search, web_browse, web_fetch, bash, bash_status, bash_stop, mkdir") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Use web_browse") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt, "Run a shell command") != NULL);
    free(prompt);
}

static void test_agent_tool_policy_session_mutation(void) {
    agent_config cfg = {0};
    agent_worker a = {0};
    agent_worker b = {0};
    char err[256];
    ds4_agent_tool_policy_set_none(&a.tool_policy);
    ds4_agent_tool_policy_set_none(&b.tool_policy);

    a.cfg = &cfg;
    b.cfg = &cfg;
    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, true, "read", err, sizeof(err)));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&a.tool_policy, "read"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&b.tool_policy, "read"));

    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, true, "web", err, sizeof(err)));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&a.tool_policy, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&a.tool_policy, "web_browse"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&b.tool_policy, "web_browse"));

    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, false, "read", err, sizeof(err)));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&a.tool_policy, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&a.tool_policy, "web_browse"));

    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, true, "all", err, sizeof(err)));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&a.tool_policy, "bash"));
    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, false, "all", err, sizeof(err)));
    AGENT_TEST_ASSERT(a.tool_policy.allow_none);
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&a.tool_policy, "bash"));

    AGENT_TEST_ASSERT(agent_worker_apply_tool_policy_command(
        &a, true, "off", err, sizeof(err)));
    AGENT_TEST_ASSERT(a.tool_policy.allow_none);
    AGENT_TEST_ASSERT(!agent_worker_apply_tool_policy_command(
        &a, true, "not_a_tool", err, sizeof(err)));
    AGENT_TEST_ASSERT(strstr(err, "invalid tool policy") != NULL);
}

static void test_agent_tool_policy_subagent_persistence(void) {
    ds4_agent_tool_policy main_pol, sub_pol, saved;
    ds4_agent_tool_policy_parse("read,bash", &main_pol);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&main_pol, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&main_pol, "bash"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&main_pol, "web_browse"));
    ds4_agent_tool_policy_parse("web", &sub_pol);
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&sub_pol, "web_browse"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&sub_pol, "read"));
    saved = sub_pol;
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&saved, "web_browse"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&saved, "read"));
    AGENT_TEST_ASSERT(ds4_agent_tool_policy_allows(&main_pol, "read"));
    AGENT_TEST_ASSERT(!ds4_agent_tool_policy_allows(&main_pol, "web_browse"));
}

static void test_agent_tool_policy_dispatch_enforcement(void) {
    char root_tmpl[] = "/tmp/ds4_agent_policy_dispatch_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    if (!root_tmp) return;

    agent_config cfg = {.non_interactive = true};
    agent_worker w = {0};
    w.cfg = &cfg;
    w.wake_fd[0] = -1;
    w.wake_fd[1] = -1;
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cond, NULL);
    pthread_mutex_init(&w.docker_shell.mu, NULL);
    AGENT_TEST_ASSERT(pipe(w.wake_fd) == 0);
    ds4_agent_tool_policy_set_none(&w.tool_policy);

    agent_tool_arg empty_question_args[] = {
        {.name = "choices", .value = "[\"A\",\"B\"]", .is_string = true},
    };
    agent_tool_call empty_question_call = {
        .name = "ask_question",
        .args = empty_question_args,
        .argc = 1,
    };
    char *empty_question = agent_execute_tool_call(&w, &empty_question_call);
    AGENT_TEST_ASSERT(strstr(empty_question, "ask_question requires question") != NULL);
    free(empty_question);

    agent_tool_arg question_args[] = {
        {.name = "question", .value = "Choose?", .is_string = true},
        {.name = "choices", .value = "[\"A\",\"B\"]", .is_string = true},
    };
    agent_tool_call question_call = {
        .name = "ask_question",
        .args = question_args,
        .argc = 2,
    };
    char *noninteractive_question = agent_execute_tool_call(&w, &question_call);
    AGENT_TEST_ASSERT(strstr(noninteractive_question,
                             "requires an interactive user") != NULL);
    AGENT_TEST_ASSERT(strstr(noninteractive_question, "not allowed") == NULL);
    free(noninteractive_question);

    char root[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    agent_path_list_append(&w.working_directories, root);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/policy.txt", root);
    char err[256];
    AGENT_TEST_ASSERT(agent_write_file_bytes(file_path, "policy ok\n", 10,
                                             err, sizeof(err)) == 0);

    agent_tool_arg read_args[] = {
        {.name = "path", .value = "policy.txt", .is_string = true},
    };
    agent_tool_call read_call = {
        .name = "read",
        .args = read_args,
        .argc = 1,
    };

    char *blocked = agent_execute_tool_call(&w, &read_call);
    AGENT_TEST_ASSERT(strstr(blocked, "is not allowed") != NULL);
    AGENT_TEST_ASSERT(w.out && strstr(w.out, "blocked by tool-access policy") != NULL);
    free(blocked);

    agent_tool_call grep_call = {
        .name = "grep",
        .args = read_args,
        .argc = 1,
    };
    free(w.out);
    w.out = NULL;
    w.out_len = 0;
    w.out_cap = 0;
    ds4_agent_tool_policy_parse("read", &w.tool_policy);
    char *unknown = agent_execute_tool_call(&w, &grep_call);
    AGENT_TEST_ASSERT(strstr(unknown, "Tool error: unknown tool: grep") != NULL);
    AGENT_TEST_ASSERT(strstr(unknown, "Available tools:") != NULL);
    AGENT_TEST_ASSERT(strstr(unknown, "search") != NULL);
    AGENT_TEST_ASSERT(strstr(unknown, "bash") == NULL);
    AGENT_TEST_ASSERT(w.out && strstr(w.out, "[tool:grep] unknown tool") != NULL);
    free(unknown);

    char *allowed = agent_execute_tool_call(&w, &read_call);
    AGENT_TEST_ASSERT(strstr(allowed, "policy ok") != NULL);
    free(allowed);

    unlink(file_path);
    rmdir(root);
    agent_path_list_free(&w.working_directories);
    free(w.out);
    close(w.wake_fd[0]);
    close(w.wake_fd[1]);
    pthread_cond_destroy(&w.cond);
    pthread_mutex_destroy(&w.mu);
    pthread_mutex_destroy(&w.docker_shell.mu);
}

static char *test_agent_thinking_stream(const char *text, bool replay,
                                        agent_dsml_parser *parser,
                                        agent_stream_renderer *stream) {
    agent_tail_capture *capture = xmalloc(sizeof(*capture));
    memset(capture, 0, sizeof(*capture));
    capture->cap = 16384;
    agent_token_renderer *renderer = xmalloc(sizeof(*renderer));
    memset(renderer, 0, sizeof(*renderer));
    renderer->format_thinking = true;
    renderer->in_think = true;
    renderer->last_output_newline = true;
    renderer->capture = capture;
    memset(parser, 0, sizeof(*parser));
    parser->state = AGENT_DSML_SEARCH;
    memset(stream, 0, sizeof(*stream));
    stream->renderer = renderer;
    stream->parser = parser;
    stream->in_think = true;
    stream->replay = replay;
    size_t split = strlen(text) > 13 ? 13 : strlen(text);
    agent_stream_text(stream, text, split, false);
    agent_stream_text(stream, text + split, strlen(text) - split, true);
    size_t out_len = 0;
    char *out = agent_tail_capture_take(capture, &out_len);
    (void)out_len;
    free(capture);
    renderer->capture = NULL;
    return out;
}

typedef struct {
    agent_worker *worker;
    bool path;
    const char *requested_path;
    const char *directory;
    bool ok;
    char err[160];
} test_agent_thinking_approval_ctx;

static void *test_agent_thinking_approval_thread(void *arg) {
    test_agent_thinking_approval_ctx *ctx = arg;
    if (ctx->path) {
        ctx->ok = agent_request_working_directory(
            ctx->worker, ctx->requested_path, ctx->directory,
            ctx->err, sizeof(ctx->err));
    } else {
        ctx->ok = agent_web_confirm(ctx->worker, "Start test browser?",
                                    ctx->err, sizeof(ctx->err)) != 0;
    }
    return NULL;
}

static void test_agent_wait_for_approval(agent_worker *worker, bool path) {
    for (int i = 0; i < 1000; i++) {
        pthread_mutex_lock(&worker->mu);
        bool pending = path ? worker->path_approval_pending :
                              worker->web_approval_pending;
        pthread_mutex_unlock(&worker->mu);
        if (pending) return;
        usleep(1000);
    }
    AGENT_TEST_ASSERT(false);
}

static void test_agent_thinking_tool_access(void) {
    static const char *eligible[] = {
        "read", "more", "list", "search", "web_browse", "web_fetch",
        "skill_list",
    };
    static const char *forbidden[] = {
        "write", "edit", "mkdir", "bash", "bash_status", "bash_stop",
        "ask_question", "subagent", "future_observer", NULL,
    };
    AGENT_TEST_ASSERT(strstr(agent_tools_prompt_intro,
                             "Inside <think></think>, only read, more, list") != NULL);
    AGENT_TEST_ASSERT(strstr(agent_tools_prompt_intro,
                             "skill_list is always available") != NULL);
    AGENT_TEST_ASSERT(strstr(agent_tools_prompt_intro,
                             "Tool calls are not allowed inside") == NULL);
    for (size_t i = 0; i < sizeof(eligible) / sizeof(eligible[0]); i++)
        AGENT_TEST_ASSERT(agent_tool_phase_eligible_during_thinking(eligible[i]));
    for (size_t i = 0; i < sizeof(forbidden) / sizeof(forbidden[0]); i++)
        AGENT_TEST_ASSERT(!agent_tool_phase_eligible_during_thinking(forbidden[i]));

    const char *allowed =
        "<｜DSML｜tool_calls>"
        "<｜DSML｜invoke name=\"read\">"
        "<｜DSML｜parameter name=\"path\" string=\"true\">a.c</｜DSML｜parameter>"
        "</｜DSML｜invoke>"
        "<｜DSML｜invoke name=\"skill_list\"></｜DSML｜invoke>"
        "</｜DSML｜tool_calls>";
    agent_dsml_parser parser;
    agent_stream_renderer stream;
    char *out = test_agent_thinking_stream(allowed, false, &parser, &stream);
    AGENT_TEST_ASSERT(parser.state == AGENT_DSML_DONE);
    AGENT_TEST_ASSERT(stream.dsml_origin_in_think);
    AGENT_TEST_ASSERT(parser.calls.len == 2);
    char validation[192];
    AGENT_TEST_ASSERT(agent_thinking_tool_block_validate(
        &parser, validation, sizeof(validation)));
    AGENT_TEST_ASSERT(strstr(out, "Reading a.c") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "skill_list") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "DSML") == NULL);
    AGENT_TEST_ASSERT(strstr(out, "tool calling is not allowed") == NULL);
    free(out);
    free(stream.renderer);
    agent_dsml_parser_free(&parser);

    const char *mixed =
        "<｜DSML｜tool_calls>"
        "<｜DSML｜invoke name=\"read\"></｜DSML｜invoke>"
        "<｜DSML｜invoke name=\"edit\">"
        "<｜DSML｜parameter name=\"path\" string=\"true\">never-read.c</｜DSML｜parameter>"
        "<｜DSML｜parameter name=\"old\" string=\"true\">old</｜DSML｜parameter>"
        "</｜DSML｜invoke>"
        "</｜DSML｜tool_calls>";
    out = test_agent_thinking_stream(mixed, false, &parser, &stream);
    AGENT_TEST_ASSERT(!agent_thinking_tool_block_validate(
        &parser, validation, sizeof(validation)));
    AGENT_TEST_ASSERT(strstr(validation, "edit is not allowed") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "tool call rejected") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "Reading") == NULL);
    AGENT_TEST_ASSERT(strstr(out, "DSML") == NULL);
    AGENT_TEST_ASSERT(!stream.tool_preflight_error);
    free(out);
    free(stream.renderer);
    agent_dsml_parser_free(&parser);

    const char *empty =
        "<｜DSML｜tool_calls></｜DSML｜tool_calls>";
    out = test_agent_thinking_stream(empty, false, &parser, &stream);
    AGENT_TEST_ASSERT(!agent_thinking_tool_block_validate(
        &parser, validation, sizeof(validation)));
    AGENT_TEST_ASSERT(strstr(validation, "empty") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "tool call rejected") != NULL);
    free(out);
    free(stream.renderer);
    agent_dsml_parser_free(&parser);

    const char *incomplete =
        "<｜DSML｜tool_calls><｜DSML｜invoke name=\"read\">";
    out = test_agent_thinking_stream(incomplete, false, &parser, &stream);
    AGENT_TEST_ASSERT(!agent_thinking_tool_block_validate(
        &parser, validation, sizeof(validation)));
    AGENT_TEST_ASSERT(strstr(validation, "incomplete") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "tool call rejected") != NULL);
    free(out);
    free(stream.renderer);
    agent_dsml_parser_free(&parser);

    /* Replay classifies/render calls but never dispatches forbidden work. */
    out = test_agent_thinking_stream(mixed, true, &parser, &stream);
    AGENT_TEST_ASSERT(strstr(out, "tool call rejected") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "Reading") == NULL);
    free(out);
    free(stream.renderer);
    agent_dsml_parser_free(&parser);

    /* Same block outside thinking keeps existing immediate semantic rendering. */
    agent_tail_capture outside_capture = {.cap = 4096};
    agent_token_renderer outside_renderer = {
        .last_output_newline = true,
        .capture = &outside_capture,
    };
    agent_dsml_parser outside_parser = {.state = AGENT_DSML_SEARCH};
    agent_stream_renderer outside_stream = {
        .renderer = &outside_renderer,
        .parser = &outside_parser,
    };
    agent_stream_text(&outside_stream, allowed, strlen(allowed), true);
    size_t outside_out_len = 0;
    char *outside_out = agent_tail_capture_take(&outside_capture,
                                                &outside_out_len);
    (void)outside_out_len;
    AGENT_TEST_ASSERT(outside_parser.state == AGENT_DSML_DONE);
    AGENT_TEST_ASSERT(!outside_stream.dsml_origin_in_think);
    AGENT_TEST_ASSERT(strstr(outside_out, "Reading a.c") != NULL);
    free(outside_out);
    agent_dsml_parser_free(&outside_parser);

    agent_config cfg = {.non_interactive = true};
    agent_worker worker;
    test_agent_fake_worker_init(&worker, &cfg);
    ds4_agent_tool_policy_set_none(&worker.tool_policy);
    agent_tool_arg read_args[] = {
        {.name = "path", .value = "missing", .is_string = true},
    };
    agent_tool_call calls_array[] = {
        {.name = "read", .args = read_args, .argc = 1},
        {.name = "skill_list"},
    };
    agent_tool_calls calls = {.v = calls_array, .len = 2, .cap = 2};
    bool interrupted = false;
    char *result = agent_execute_tool_calls_ordered(
        &worker, &calls, true, &interrupted);
    AGENT_TEST_ASSERT(!interrupted);
    AGENT_TEST_ASSERT(strstr(result, "read is not allowed") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "Tool result 2 (skill_list)") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "\"total_count\":0") != NULL ||
                      strstr(result, "\"total_count\": 0") != NULL);
    free(result);

    agent_tool_call web_call = {.name = "web_browse"};
    result = agent_execute_tool_call(&worker, &web_call);
    AGENT_TEST_ASSERT(strstr(result, "is not allowed") != NULL);
    free(result);

    char root_tmpl[] = "/tmp/ds4_agent_thinking_root_XXXXXX";
    char outside_tmpl[] = "/tmp/ds4_agent_thinking_outside_XXXXXX";
    char *root = mkdtemp(root_tmpl);
    char *outside_root = mkdtemp(outside_tmpl);
    AGENT_TEST_ASSERT(root != NULL && outside_root != NULL);
    if (root && outside_root) {
        agent_path_list_append(&worker.working_directories, root);
        char outside_path[PATH_MAX];
        snprintf(outside_path, sizeof(outside_path), "%s/evidence.txt",
                 outside_root);
        char write_err[128];
        AGENT_TEST_ASSERT(agent_write_file_bytes(
            outside_path, "outside\n", 8, write_err, sizeof(write_err)) == 0);
        read_args[0].value = outside_path;
        ds4_agent_tool_policy_parse("read", &worker.tool_policy);
        result = agent_execute_tool_call(&worker, &calls_array[0]);
        AGENT_TEST_ASSERT(strstr(result, "outside configured working directories") != NULL);
        AGENT_TEST_ASSERT(worker.working_directories.len == 1);
        free(result);
        unlink(outside_path);
        rmdir(outside_root);
        rmdir(root);
    }

    pthread_mutex_lock(&worker.mu);
    worker.interrupt = true;
    pthread_mutex_unlock(&worker.mu);
    result = agent_execute_tool_calls_ordered(
        &worker, &calls, true, &interrupted);
    AGENT_TEST_ASSERT(interrupted);
    AGENT_TEST_ASSERT(strstr(result, "interrupted by user") != NULL);
    AGENT_TEST_ASSERT(strstr(result, "Tool result 1") == NULL);
    free(result);
    worker_clear_interrupt(&worker);
    agent_worker_free(&worker);

    agent_config interactive_cfg = {0};
    agent_worker approval_worker, sibling_worker;
    test_agent_fake_worker_init(&approval_worker, &interactive_cfg);
    test_agent_fake_worker_init(&sibling_worker, &interactive_cfg);
    pthread_t approval_thread;
    test_agent_thinking_approval_ctx approval = {
        .worker = &approval_worker,
    };

    AGENT_TEST_ASSERT(pthread_create(&approval_thread, NULL,
                                     test_agent_thinking_approval_thread,
                                     &approval) == 0);
    test_agent_wait_for_approval(&approval_worker, false);
    char approval_message[256];
    AGENT_TEST_ASSERT(worker_take_web_approval_request(
        &approval_worker, approval_message, sizeof(approval_message)));
    AGENT_TEST_ASSERT(!worker_take_web_approval_request(
        &sibling_worker, approval_message, sizeof(approval_message)));
    worker_answer_web_approval(&approval_worker, false, "web denied for test");
    pthread_join(approval_thread, NULL);
    AGENT_TEST_ASSERT(!approval.ok);
    AGENT_TEST_ASSERT(strstr(approval.err, "web denied") != NULL);

    memset(&approval, 0, sizeof(approval));
    approval.worker = &approval_worker;
    AGENT_TEST_ASSERT(pthread_create(&approval_thread, NULL,
                                     test_agent_thinking_approval_thread,
                                     &approval) == 0);
    test_agent_wait_for_approval(&approval_worker, false);
    pthread_mutex_lock(&approval_worker.mu);
    approval_worker.interrupt = true;
    pthread_cond_broadcast(&approval_worker.cond);
    pthread_mutex_unlock(&approval_worker.mu);
    pthread_join(approval_thread, NULL);
    AGENT_TEST_ASSERT(!approval.ok);
    AGENT_TEST_ASSERT(!strcmp(approval.err, "interrupted"));
    worker_clear_interrupt(&approval_worker);

    char approved_tmpl[] = "/tmp/ds4_agent_thinking_approved_XXXXXX";
    char denied_tmpl[] = "/tmp/ds4_agent_thinking_denied_XXXXXX";
    char *approved_dir = mkdtemp(approved_tmpl);
    char *denied_dir = mkdtemp(denied_tmpl);
    AGENT_TEST_ASSERT(approved_dir != NULL && denied_dir != NULL);
    if (approved_dir && denied_dir) {
        memset(&approval, 0, sizeof(approval));
        approval.worker = &approval_worker;
        approval.path = true;
        approval.requested_path = "/tmp/requested-evidence.txt";
        approval.directory = approved_dir;
        AGENT_TEST_ASSERT(pthread_create(&approval_thread, NULL,
                                         test_agent_thinking_approval_thread,
                                         &approval) == 0);
        test_agent_wait_for_approval(&approval_worker, true);
        char path_message[PATH_MAX + 256];
        char options[3][PATH_MAX];
        int option_count = 0;
        AGENT_TEST_ASSERT(worker_take_path_approval_request(
            &approval_worker, path_message, sizeof(path_message),
            options, &option_count));
        AGENT_TEST_ASSERT(option_count > 0);
        AGENT_TEST_ASSERT(!worker_take_path_approval_request(
            &sibling_worker, path_message, sizeof(path_message),
            options, &option_count));
        worker_answer_path_approval(&approval_worker, true, approved_dir, NULL);
        pthread_join(approval_thread, NULL);
        AGENT_TEST_ASSERT(approval.ok);
        AGENT_TEST_ASSERT(agent_path_list_contains(
            &approval_worker.working_directories, approved_dir));
        AGENT_TEST_ASSERT(!agent_path_list_contains(
            &sibling_worker.working_directories, approved_dir));

        int roots_before = approval_worker.working_directories.len;
        memset(&approval, 0, sizeof(approval));
        approval.worker = &approval_worker;
        approval.path = true;
        approval.requested_path = "/tmp/denied-evidence.txt";
        approval.directory = denied_dir;
        AGENT_TEST_ASSERT(pthread_create(&approval_thread, NULL,
                                         test_agent_thinking_approval_thread,
                                         &approval) == 0);
        test_agent_wait_for_approval(&approval_worker, true);
        worker_answer_path_approval(&approval_worker, false, NULL,
                                    "path denied for test");
        pthread_join(approval_thread, NULL);
        AGENT_TEST_ASSERT(!approval.ok);
        AGENT_TEST_ASSERT(strstr(approval.err, "path denied") != NULL);
        AGENT_TEST_ASSERT(approval_worker.working_directories.len == roots_before);

        memset(&approval, 0, sizeof(approval));
        approval.worker = &approval_worker;
        approval.path = true;
        approval.requested_path = "/tmp/interrupted-evidence.txt";
        approval.directory = denied_dir;
        AGENT_TEST_ASSERT(pthread_create(&approval_thread, NULL,
                                         test_agent_thinking_approval_thread,
                                         &approval) == 0);
        test_agent_wait_for_approval(&approval_worker, true);
        pthread_mutex_lock(&approval_worker.mu);
        approval_worker.interrupt = true;
        pthread_cond_broadcast(&approval_worker.cond);
        pthread_mutex_unlock(&approval_worker.mu);
        pthread_join(approval_thread, NULL);
        AGENT_TEST_ASSERT(!approval.ok);
        AGENT_TEST_ASSERT(!strcmp(approval.err, "interrupted"));
        AGENT_TEST_ASSERT(approval_worker.working_directories.len == roots_before);
        worker_clear_interrupt(&approval_worker);
        rmdir(denied_dir);
        rmdir(approved_dir);
    }
    agent_worker_free(&sibling_worker);
    agent_worker_free(&approval_worker);
}

static void test_agent_fake_worker_init(agent_worker *w, agent_config *cfg);

typedef struct {
    agent_worker *worker;
    agent_tool_call call;
    char *result;
} agent_question_test_ctx;

static void *test_agent_question_thread(void *arg) {
    agent_question_test_ctx *ctx = arg;
    ctx->result = agent_execute_tool_call(ctx->worker, &ctx->call);
    return NULL;
}

static void test_agent_ask_question_stream_preview(void) {
    const char dsml[] =
        "<｜DSML｜tool_calls>\n"
        "<｜DSML｜invoke name=\"ask_question\">\n"
        "<｜DSML｜parameter name=\"question\" string=\"true\">\xef\xbf\xbd\xef\xbf\xbd Should Fix #4: Path validation against allowed directories\n"
        "\n"
        "The body should be reserved for the interactive prompt.</｜DSML｜parameter>\n"
        "<｜DSML｜parameter name=\"choices\" string=\"true\">[\"Yes - validate paths\", \"No - skip validation\"]</｜DSML｜parameter>\n"
        "</｜DSML｜invoke>\n"
        "</｜DSML｜tool_calls>\n";
    agent_tail_capture capture = {.cap = 4096};
    agent_token_renderer renderer = {
        .last_output_newline = true,
        .capture = &capture,
    };
    agent_dsml_parser dsml_parser = {.state = AGENT_DSML_SEARCH};
    agent_stream_renderer stream = {
        .renderer = &renderer,
        .parser = &dsml_parser,
    };

    agent_stream_text(&stream, dsml, strlen(dsml), true);
    size_t out_len = 0;
    char *out = agent_tail_capture_take(&capture, &out_len);
    (void)out_len;

    AGENT_TEST_ASSERT(strstr(out, "ask_question  question=Should Fix #4: Path validation against allowed directories\n") != NULL);
    AGENT_TEST_ASSERT(strstr(out, "\xef\xbf\xbd") == NULL);
    AGENT_TEST_ASSERT(strstr(out, "The body should be reserved") == NULL);
    AGENT_TEST_ASSERT(strstr(out, "validate paths") == NULL);
    free(out);
    agent_dsml_parser_free(&dsml_parser);
}

static void test_agent_ask_question_interactive_flow(void) {
    agent_config cfg = {0};
    agent_worker w;
    test_agent_fake_worker_init(&w, &cfg);
    ds4_agent_tool_policy_set_none(&w.tool_policy);

    agent_tool_arg args[] = {
        {.name = "question", .value = "Which path?", .is_string = true},
        {.name = "choices",
         .value = "[\"Yes - \\\\@name escapes replacement, backslash is removed, @name passes through literally\",\"No - no escape needed, user can avoid conflicts\"]",
         .is_string = true},
    };
    agent_question_test_ctx ctx = {
        .worker = &w,
        .call = {
            .name = "ask_question",
            .args = args,
            .argc = 2,
        },
    };
    pthread_t thread;
    AGENT_TEST_ASSERT(pthread_create(&thread, NULL,
                                     test_agent_question_thread, &ctx) == 0);

    char message[AGENT_ASK_QUESTION_TEXT_MAX];
    char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX];
    int choice_count = 0;
    bool pending = false;
    for (int i = 0; i < 100; i++) {
        if (worker_take_question_request(&w, message, sizeof(message),
                                         choices, &choice_count)) {
            pending = true;
            break;
        }
        usleep(1000);
    }
    AGENT_TEST_ASSERT(pending);
    AGENT_TEST_ASSERT(!strcmp(message, "Which path?"));
    AGENT_TEST_ASSERT(choice_count == 2);
    AGENT_TEST_ASSERT(!strcmp(choices[0], "Yes - \\@name escapes replacement, backslash is removed, @name passes through literally"));
    AGENT_TEST_ASSERT(!strcmp(choices[1], "No - no escape needed, user can avoid conflicts"));
    worker_answer_question(&w, false, choices[0], NULL);
    pthread_join(thread, NULL);
    AGENT_TEST_ASSERT(ctx.result && strstr(ctx.result, "Answer: Yes - \\@name escapes replacement, backslash is removed") != NULL);
    free(ctx.result);
    agent_worker_free(&w);
}

static void test_agent_ask_question_prompt_options(void) {
    char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX] = {0};
    snprintf(choices[0], sizeof(choices[0]), "A");

    int saved_stdin = dup(STDIN_FILENO);
    AGENT_TEST_ASSERT(saved_stdin >= 0);
    int saved_stdout = dup(STDOUT_FILENO);
    AGENT_TEST_ASSERT(saved_stdout >= 0);
    int devnull = open("/dev/null", O_WRONLY);
    AGENT_TEST_ASSERT(devnull >= 0);
    AGENT_TEST_ASSERT(dup2(devnull, STDOUT_FILENO) >= 0);
    close(devnull);

    int input_pipe[2];
    AGENT_TEST_ASSERT(pipe(input_pipe) == 0);
    const char custom_input[] = "3\nCustom answer\n";
    AGENT_TEST_ASSERT(write(input_pipe[1], custom_input,
                            sizeof(custom_input) - 1) ==
                      (ssize_t)(sizeof(custom_input) - 1));
    close(input_pipe[1]);
    AGENT_TEST_ASSERT(dup2(input_pipe[0], STDIN_FILENO) >= 0);
    close(input_pipe[0]);
    char answer[AGENT_ASK_QUESTION_ANSWER_MAX];
    bool interrupted = false;
    AGENT_TEST_ASSERT(agent_prompt_ask_question("Question?", choices, 1,
                                                answer, &interrupted));
    AGENT_TEST_ASSERT(!interrupted);
    AGENT_TEST_ASSERT(!strcmp(answer, "Custom answer"));

    AGENT_TEST_ASSERT(pipe(input_pipe) == 0);
    const char interrupt_input[] = "2\n";
    AGENT_TEST_ASSERT(write(input_pipe[1], interrupt_input,
                            sizeof(interrupt_input) - 1) ==
                      (ssize_t)(sizeof(interrupt_input) - 1));
    close(input_pipe[1]);
    AGENT_TEST_ASSERT(dup2(input_pipe[0], STDIN_FILENO) >= 0);
    close(input_pipe[0]);
    interrupted = false;
    AGENT_TEST_ASSERT(!agent_prompt_ask_question("Question?", choices, 1,
                                                 answer, &interrupted));
    AGENT_TEST_ASSERT(interrupted);

    AGENT_TEST_ASSERT(dup2(saved_stdin, STDIN_FILENO) >= 0);
    close(saved_stdin);
    AGENT_TEST_ASSERT(dup2(saved_stdout, STDOUT_FILENO) >= 0);
    close(saved_stdout);
}

static void test_agent_skill_write(const char *path, const void *data, size_t len) {
    char err[160] = {0};
    AGENT_TEST_ASSERT(agent_write_file_bytes(path, data, len, err, sizeof(err)) == 0);
}

static void test_agent_skill_add_direct(agent_skill_registry *registry,
                                        const char *name,
                                        const char *description,
                                        const char *path) {
    agent_skill *skill = xmalloc(sizeof(*skill));
    memset(skill, 0, sizeof(*skill));
    snprintf(skill->name, sizeof(skill->name), "%s", name);
    snprintf(skill->description, sizeof(skill->description), "%s", description);
    snprintf(skill->path, sizeof(skill->path), "%s", path);
    pthread_mutex_lock(&registry->mu);
    skill->next = registry->head;
    registry->head = skill;
    registry->count++;
    registry->generation++;
    pthread_mutex_unlock(&registry->mu);
}

typedef struct {
    agent_worker *worker;
    int iterations;
} test_agent_skill_race_ctx;

static void *test_agent_skill_delete_add_thread(void *arg) {
    test_agent_skill_race_ctx *ctx = arg;
    for (int i = 0; i < ctx->iterations; i++) {
        agent_skill_delete(ctx->worker, "race");
        test_agent_skill_add_direct(ctx->worker->skill_registry, "race", "race",
                                    "/tmp/race-skill.md");
    }
    return NULL;
}

static int test_agent_count_substring(const char *text, const char *needle) {
    int count = 0;
    size_t len = strlen(needle);
    for (const char *p = text; p && (p = strstr(p, needle)); p += len) count++;
    return count;
}

static void test_agent_skills(void) {
    char root_tmpl[] = "/tmp/ds4_agent_skills_XXXXXX";
    char outside_tmpl[] = "/tmp/ds4_agent_skills_outside_XXXXXX";
    char *root_tmp = mkdtemp(root_tmpl);
    char *outside_tmp = mkdtemp(outside_tmpl);
    AGENT_TEST_ASSERT(root_tmp != NULL);
    AGENT_TEST_ASSERT(outside_tmp != NULL);
    if (!root_tmp || !outside_tmp) return;

    char root[PATH_MAX], outside[PATH_MAX];
    AGENT_TEST_ASSERT(realpath(root_tmp, root) != NULL);
    AGENT_TEST_ASSERT(realpath(outside_tmp, outside) != NULL);
    agent_config cfg = {0};
    agent_worker a = {.cfg = &cfg};
    agent_worker b = {.cfg = &cfg};
    a.skill_registry = agent_skill_registry_create();
    b.skill_registry = a.skill_registry;
    agent_skill_registry_acquire(b.skill_registry);
    agent_path_list_append(&a.working_directories, root);
    agent_path_list_append(&b.working_directories, root);

    agent_buf escaped_controls = {0};
    char controls[0x20];
    for (size_t i = 0; i < sizeof(controls); i++) controls[i] = (char)i;
    agent_json_escape_n(&escaped_controls, controls, sizeof(controls));
    char *escaped_control_text = agent_buf_take(&escaped_controls);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\u0000") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\u0001") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\b") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\t") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\n") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\f") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\r") != NULL);
    AGENT_TEST_ASSERT(strstr(escaped_control_text, "\\u001f") != NULL);
    for (const unsigned char *p = (const unsigned char *)escaped_control_text;
         *p; p++)
        AGENT_TEST_ASSERT(*p >= 0x20);
    free(escaped_control_text);

    char valid[PATH_MAX];
    snprintf(valid, sizeof(valid), "%s/review.md", root);
    const char valid_text[] =
        "---\nname: review\ndescription: Review changes \"carefully\"\n---\nBody\n";
    test_agent_skill_write(valid, valid_text, strlen(valid_text));
    char name[AGENT_SKILL_NAME_MAX], desc[AGENT_SKILL_DESC_MAX];
    AGENT_TEST_ASSERT(agent_skill_parse_file(valid, name, sizeof(name),
                                             desc, sizeof(desc)) == 0);
    AGENT_TEST_ASSERT(!strcmp(name, "review"));
    AGENT_TEST_ASSERT(strstr(desc, "carefully") != NULL);
    AGENT_TEST_ASSERT(agent_skill_register(&a, valid) == 0);
    AGENT_TEST_ASSERT(agent_skill_register(&a, valid) == AGENT_SKILL_ERR_DUPLICATE);

    agent_worker c = {.cfg = &cfg, .skill_registry = a.skill_registry};
    agent_skill_registry_acquire(c.skill_registry);
    agent_path_list_append(&c.working_directories, root);

    char *prompt_before = agent_build_skills_prompt(&b);
    AGENT_TEST_ASSERT(strstr(prompt_before, "**review**") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt_before, "When the user includes") != NULL);
    char *new_session_prompt = agent_build_skills_prompt(&c);
    AGENT_TEST_ASSERT(strstr(new_session_prompt, "**review**") != NULL);
    free(new_session_prompt);

    char *expanded = agent_expand_skill_references(
        &b, "Use @review then @unknown and \\@review; finish");
    char expected_link[PATH_MAX + 64];
    snprintf(expected_link, sizeof(expected_link), "[skill:review](%s)", valid);
    AGENT_TEST_ASSERT(strstr(expanded, expected_link) != NULL);
    AGENT_TEST_ASSERT(strstr(expanded, "@unknown") != NULL);
    AGENT_TEST_ASSERT(strstr(expanded, "and @review; finish") != NULL);
    free(expanded);

    char long_path[AGENT_SKILL_PATH_MAX];
    memset(long_path, 'p', sizeof(long_path) - 1);
    long_path[0] = '/';
    long_path[sizeof(long_path) - 1] = '\0';
    test_agent_skill_add_direct(a.skill_registry, "x", "long", long_path);
    expanded = agent_expand_skill_references(&a, "@x trailing text");
    AGENT_TEST_ASSERT(strlen(expanded) > strlen(long_path));
    AGENT_TEST_ASSERT(strstr(expanded, " trailing text") != NULL);
    free(expanded);

    test_agent_skill_add_direct(a.skill_registry, "race", "race",
                                "/tmp/race-skill.md");
    test_agent_skill_race_ctx race_ctx = {.worker = &a, .iterations = 1000};
    pthread_t race_thread;
    AGENT_TEST_ASSERT(pthread_create(&race_thread, NULL,
                                     test_agent_skill_delete_add_thread,
                                     &race_ctx) == 0);
    agent_buf race_input = {0};
    for (int i = 0; i < 1000; i++) agent_buf_puts(&race_input, "@race ");
    agent_buf_puts(&race_input, "trailing-marker");
    char *race_text = agent_buf_take(&race_input);
    expanded = agent_expand_skill_references(&a, race_text);
    AGENT_TEST_ASSERT(strstr(expanded, "trailing-marker") != NULL);
    AGENT_TEST_ASSERT(pthread_join(race_thread, NULL) == 0);
    free(expanded);
    free(race_text);
    agent_skill_delete(&a, "race");

    agent_tool_call list_call = {.name = "skill_list"};
    char *json = agent_tool_skill_list(&a, &list_call);
    AGENT_TEST_ASSERT(strstr(json, "\\\"carefully\\\"") != NULL);
    AGENT_TEST_ASSERT(strstr(json, "\"total_count\": 2") != NULL);
    free(json);
    agent_tool_arg query_arg = {.name = "query", .value = "REV", .is_string = true};
    list_call.args = &query_arg;
    list_call.argc = 1;
    json = agent_tool_skill_list(&a, &list_call);
    AGENT_TEST_ASSERT(strstr(json, "\"total_count\": 1") != NULL);
    AGENT_TEST_ASSERT(strstr(json, "\"name\": \"review\"") != NULL);
    free(json);

    test_agent_skill_add_direct(a.skill_registry, "escaped",
                                "line\ncolumn\tcontrol\x01", "/tmp/a\\b");
    for (int i = 0; i < 11; i++) {
        char bulk_name[32];
        snprintf(bulk_name, sizeof(bulk_name), "bulk-%02d", i);
        test_agent_skill_add_direct(a.skill_registry, bulk_name, "bulk", "/tmp/bulk");
    }
    list_call.args = NULL;
    list_call.argc = 0;
    json = agent_tool_skill_list(&a, &list_call);
    AGENT_TEST_ASSERT(strstr(json, "\"total_count\": 14") != NULL);
    AGENT_TEST_ASSERT(strstr(json, "\"has_more\": true") != NULL);
    AGENT_TEST_ASSERT(test_agent_count_substring(json, "\"name\":") == 10);
    free(json);
    query_arg.value = "escaped";
    list_call.args = &query_arg;
    list_call.argc = 1;
    json = agent_tool_skill_list(&a, &list_call);
    AGENT_TEST_ASSERT(strstr(json, "line\\ncolumn\\tcontrol\\u0001") != NULL);
    AGENT_TEST_ASSERT(strstr(json, "/tmp/a\\\\b") != NULL);
    free(json);

    ds4_agent_tool_policy none;
    ds4_agent_tool_policy_parse("none", &none);
    char *tools = agent_build_filtered_tools_prompt(&none);
    AGENT_TEST_ASSERT(strstr(tools, "Available tools: ask_question, skill_list") != NULL);
    AGENT_TEST_ASSERT(test_agent_count_substring(tools, "\"name\": \"skill_list\"") == 1);
    free(tools);
    AGENT_TEST_ASSERT(agent_slash_command_known("/skills add foo"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/skills list"));
    AGENT_TEST_ASSERT(agent_slash_command_known("/skills del foo"));

    char missing_desc[PATH_MAX];
    snprintf(missing_desc, sizeof(missing_desc), "%s/missing.md", root);
    const char missing_text[] = "---\nname: missing\n---\n";
    test_agent_skill_write(missing_desc, missing_text, strlen(missing_text));
    AGENT_TEST_ASSERT(agent_skill_parse_file(missing_desc, name, sizeof(name),
                                             desc, sizeof(desc)) == AGENT_SKILL_ERR_FRONTMATTER);

    char invalid_name[PATH_MAX];
    snprintf(invalid_name, sizeof(invalid_name), "%s/invalid-name.md", root);
    const char invalid_name_text[] =
        "---\nname: bad name\ndescription: invalid\n---\n";
    test_agent_skill_write(invalid_name, invalid_name_text, strlen(invalid_name_text));
    AGENT_TEST_ASSERT(agent_skill_parse_file(invalid_name, name, sizeof(name),
                                             desc, sizeof(desc)) == AGENT_SKILL_ERR_NAME);

    char invalid_utf8[PATH_MAX];
    snprintf(invalid_utf8, sizeof(invalid_utf8), "%s/utf8.md", root);
    const unsigned char invalid_utf8_text[] =
        "---\nname: utf8\ndescription: \xc0\xaf\n---\n";
    test_agent_skill_write(invalid_utf8, invalid_utf8_text,
                           sizeof(invalid_utf8_text) - 1);
    AGENT_TEST_ASSERT(agent_skill_parse_file(invalid_utf8, name, sizeof(name),
                                             desc, sizeof(desc)) == AGENT_SKILL_ERR_UTF8);
    const unsigned char valid_emoji[] = {0xf0, 0x9f, 0x98, 0x80};
    const unsigned char surrogate[] = {0xed, 0xa0, 0x80};
    const unsigned char above_unicode[] = {0xf4, 0x90, 0x80, 0x80};
    AGENT_TEST_ASSERT(agent_utf8_valid(valid_emoji, sizeof(valid_emoji)));
    AGENT_TEST_ASSERT(!agent_utf8_valid(surrogate, sizeof(surrogate)));
    AGENT_TEST_ASSERT(!agent_utf8_valid(above_unicode, sizeof(above_unicode)));

    char overlong[PATH_MAX];
    snprintf(overlong, sizeof(overlong), "%s/overlong.md", root);
    char overlong_text[700];
    memset(overlong_text, 'a', sizeof(overlong_text));
    memcpy(overlong_text, "---\nname: ", 10);
    overlong_text[sizeof(overlong_text) - 1] = '\n';
    test_agent_skill_write(overlong, overlong_text, sizeof(overlong_text));
    AGENT_TEST_ASSERT(agent_skill_parse_file(overlong, name, sizeof(name),
                                             desc, sizeof(desc)) == AGENT_SKILL_ERR_LINE_TOO_LONG);

    char too_many_lines[PATH_MAX];
    snprintf(too_many_lines, sizeof(too_many_lines), "%s/lines.md", root);
    agent_buf many = {0};
    agent_buf_puts(&many, "---\nname: lines\ndescription: lines\n");
    for (int i = 0; i < 18; i++) agent_buf_puts(&many, "extra: value\n");
    agent_buf_puts(&many, "---\n");
    char *many_text = agent_buf_take(&many);
    test_agent_skill_write(too_many_lines, many_text, strlen(many_text));
    free(many_text);
    AGENT_TEST_ASSERT(agent_skill_parse_file(too_many_lines, name, sizeof(name),
                                             desc, sizeof(desc)) == AGENT_SKILL_ERR_FRONTMATTER);

    char symlink_path[PATH_MAX];
    snprintf(symlink_path, sizeof(symlink_path), "%s/loop", root);
    AGENT_TEST_ASSERT(symlink(root, symlink_path) == 0);
    int warning_pipe[2];
    AGENT_TEST_ASSERT(pipe(warning_pipe) == 0);
    int saved_stderr = dup(STDERR_FILENO);
    AGENT_TEST_ASSERT(saved_stderr >= 0);
    fflush(stderr);
    AGENT_TEST_ASSERT(dup2(warning_pipe[1], STDERR_FILENO) >= 0);
    close(warning_pipe[1]);
    AGENT_TEST_ASSERT(agent_skill_register_dir(&a, root) == 0);
    fflush(stderr);
    AGENT_TEST_ASSERT(dup2(saved_stderr, STDERR_FILENO) >= 0);
    close(saved_stderr);
    char warnings[8192];
    ssize_t warning_len = read(warning_pipe[0], warnings, sizeof(warnings) - 1);
    close(warning_pipe[0]);
    AGENT_TEST_ASSERT(warning_len > 0);
    if (warning_len > 0) {
        warnings[warning_len] = '\0';
        AGENT_TEST_ASSERT(strstr(warnings, missing_desc) != NULL);
        AGENT_TEST_ASSERT(strstr(warnings, "invalid or incomplete frontmatter") != NULL);
        AGENT_TEST_ASSERT(strstr(warnings, "skipping symlink") != NULL);
    }

    char outside_file[PATH_MAX];
    snprintf(outside_file, sizeof(outside_file), "%s/outside.md", outside);
    const char outside_text[] =
        "---\nname: outside\ndescription: Outside\n---\n";
    test_agent_skill_write(outside_file, outside_text, strlen(outside_text));
    AGENT_TEST_ASSERT(agent_skill_register(&a, outside_file) == AGENT_SKILL_ERR_WORKSPACE);
    AGENT_TEST_ASSERT(!agent_skill_path_allowed(&a, outside));
    AGENT_TEST_ASSERT(agent_skill_register_dir(&a, outside) == 0);

    int diagnostic_pipe[2];
    AGENT_TEST_ASSERT(pipe(diagnostic_pipe) == 0);
    saved_stderr = dup(STDERR_FILENO);
    AGENT_TEST_ASSERT(saved_stderr >= 0);
    fflush(stderr);
    AGENT_TEST_ASSERT(dup2(diagnostic_pipe[1], STDERR_FILENO) >= 0);
    close(diagnostic_pipe[1]);
    char missing_path[PATH_MAX];
    snprintf(missing_path, sizeof(missing_path), "%s/does-not-exist.md", root);
    AGENT_TEST_ASSERT(agent_skill_register(&a, missing_path) == AGENT_SKILL_ERR_IO);
    char short_path[8];
    AGENT_TEST_ASSERT(!agent_skill_join_path(short_path, sizeof(short_path),
                                             "/123456", "overflow.md"));
    fflush(stderr);
    AGENT_TEST_ASSERT(dup2(saved_stderr, STDERR_FILENO) >= 0);
    close(saved_stderr);
    char diagnostics[2048];
    ssize_t diagnostic_len = read(diagnostic_pipe[0], diagnostics,
                                  sizeof(diagnostics) - 1);
    close(diagnostic_pipe[0]);
    AGENT_TEST_ASSERT(diagnostic_len > 0);
    if (diagnostic_len > 0) {
        diagnostics[diagnostic_len] = '\0';
        AGENT_TEST_ASSERT(strstr(diagnostics, missing_path) != NULL);
        AGENT_TEST_ASSERT(strstr(diagnostics, strerror(ENOENT)) != NULL);
        AGENT_TEST_ASSERT(strstr(diagnostics, "skill path too long") != NULL);
    }

    agent_skill_delete(&a, "review");
    char *prompt_after = agent_build_skills_prompt(&b);
    AGENT_TEST_ASSERT(strstr(prompt_before, "**review**") != NULL);
    AGENT_TEST_ASSERT(strstr(prompt_after, "**review**") == NULL);
    free(prompt_before);
    free(prompt_after);
    agent_skill_delete(&a, "x");
    agent_skill_delete(&a, "escaped");
    for (int i = 0; i < 11; i++) {
        char bulk_name[32];
        snprintf(bulk_name, sizeof(bulk_name), "bulk-%02d", i);
        agent_skill_delete(&a, bulk_name);
    }
    char *empty_prompt = agent_build_skills_prompt(&a);
    AGENT_TEST_ASSERT(empty_prompt[0] == '\0');
    free(empty_prompt);
    json = agent_tool_skill_list(&a, &(agent_tool_call){.name = "skill_list"});
    AGENT_TEST_ASSERT(strstr(json, "\"skills\": [\n\n  ]") != NULL);
    AGENT_TEST_ASSERT(strstr(json, "\"total_count\": 0") != NULL);
    free(json);

    agent_config startup_cfg = {0};
    agent_worker startup = {.cfg = &startup_cfg};
    startup.skill_registry = agent_skill_registry_create();
    agent_path_list_append(&startup_cfg.skill_dirs, outside);
    AGENT_TEST_ASSERT(agent_skills_init(&startup) == 1);
    AGENT_TEST_ASSERT(agent_path_list_contains(&startup.working_directories, outside));
    AGENT_TEST_ASSERT(agent_path_list_contains(&startup_cfg.working_directories, outside));
    agent_path_list_free(&startup_cfg.skill_dirs);
    agent_path_list_free(&startup_cfg.working_directories);
    agent_path_list_free(&startup.working_directories);
    agent_skill_registry_release(startup.skill_registry);

    unlink(symlink_path);
    unlink(valid);
    unlink(missing_desc);
    unlink(invalid_name);
    unlink(invalid_utf8);
    unlink(overlong);
    unlink(too_many_lines);
    unlink(outside_file);
    agent_path_list_free(&a.working_directories);
    agent_path_list_free(&b.working_directories);
    agent_path_list_free(&c.working_directories);
    agent_skill_registry_release(c.skill_registry);
    agent_skill_registry_release(b.skill_registry);
    agent_skill_registry_release(a.skill_registry);
    rmdir(root);
    rmdir(outside);
}

static void test_agent_worker_model_gate_serializes(void);

static void ds4_agent_unit_tests_run(void) {
    test_agent_edit_upto_tail_newline_is_not_part_of_anchor();
    test_agent_edit_upto_requires_tail_after_newline_strip();
    test_agent_edit_new_upto_preserves_omitted_middle();
    test_agent_edit_upto_accepts_indented_marker_line();
    test_agent_edit_new_literal_upto_is_not_merge_marker();
    test_agent_tool_edit_new_upto_merges_omitted_middle();
    test_agent_dsml_file_literal_escape_markup();
    test_agent_file_literal_escape_decode_function_only();
    test_agent_tool_write_preserves_literal_backslash_n();
    test_agent_tool_write_survives_worker_free();
    test_agent_temp_files_cleanup_requires_idle_window();
    test_agent_tool_edit_preserves_literal_backslash_n();
    test_agent_working_directory_path_resolution();
    test_agent_working_directory_file_tools();
    test_agent_default_working_directory_from_launch_cwd();
    test_agent_project_instruction_file_priority();
    test_agent_path_list_remove_prefix();
    test_agent_remove_workspace_clears_auto_allowed();
    test_agent_remove_auto_allowed_path_directly();
    test_agent_executable_exists();
    test_agent_command_in_path();
    test_agent_footer_main_subagents_stable_order();
    test_agent_footer_policy_change_reflects();
    test_agent_footer_attention_focus_matrix();
    test_agent_footer_wrapping_oversized();
    test_agent_footer_grammar_assertions();
    test_agent_footer_think_mode_icons();
    test_agent_footer_mutually_exclusive_activity();
    test_agent_footer_multi_agent_80col();
    test_agent_footer_no_writable_paths();
    test_agent_footer_ownership_badge();
    test_agent_footer_ownership_manager_sampling();
    test_agent_footer_ownership_handoff();
    test_agent_footer_ownership_regression();
    test_agent_subagent_api_lifecycle();
    test_agent_subagent_slash_command_recognition();
    test_agent_tool_policy_parse();
    test_agent_tool_policy_prompt_building();
    test_agent_tool_policy_session_mutation();
    test_agent_tool_policy_subagent_persistence();
    test_agent_tool_policy_dispatch_enforcement();
    test_agent_thinking_tool_access();
    test_agent_ask_question_stream_preview();
    test_agent_ask_question_interactive_flow();
    test_agent_ask_question_prompt_options();
    test_agent_skills();
    ds4_agent_subagent_unit_tests_run();
    test_agent_worker_model_gate_serializes();
}
#endif

static bool agent_preflight_edit_old(agent_worker *w, const agent_tool_call *call,
                                     char *err, size_t err_len) {
    const char *path = agent_tool_arg_value(call, "path");
    if (!path || !path[0]) return true; /* Cannot preflight until path is known. */

    const char *old = agent_tool_arg_value(call, "old");
    if (!old || !old[0]) {
        snprintf(err, err_len, "edit requires non-empty old text");
        return false;
    }

    char *data = NULL;
    size_t len = 0;
    char *file_path = NULL;
    if (agent_tool_use_docker_filesystem(w)) {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, err_len);
        if (!file_path)
            return false;
        if (!agent_docker_read_file_bytes(w, file_path, &data, &len, err,
                                          err_len, NULL)) {
            free(file_path);
            return false;
        }
    } else {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, err_len);
        if (!file_path)
            return false;
        if (agent_read_file_bytes(file_path, &data, &len, err, err_len) != 0) {
            free(file_path);
            return false;
        }
    }

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    bool ok = agent_edit_find_old_span(data, len, old, &match, &match_len,
                                       &anchored, NULL, err, err_len);
    free(file_path);
    free(data);
    return ok;
}

static char *agent_apply_file_splice(agent_worker *w, const char *path,
                                     const char *data, size_t len,
                                     size_t offset, size_t remove_len,
                                     const char *insert, const char *kind,
                                     const agent_buf *stderr_out) {
    char err[256];
    agent_buf write_stderr = {0};
    if (!insert) insert = "";
    size_t insert_len = strlen(insert);
    size_t out_len = offset + insert_len + (len - offset - remove_len);
    char *out = xmalloc(out_len + 1);
    memcpy(out, data, offset);
    memcpy(out + offset, insert, insert_len);
    memcpy(out + offset + insert_len, data + offset + remove_len,
           len - offset - remove_len);
    out[out_len] = '\0';

    int rc = 0;
    if (agent_tool_use_docker_filesystem(w))
        rc = agent_docker_write_file_bytes(w, path, out, out_len, err,
                                           sizeof(err), &write_stderr) ? 0 : -1;
    else
        rc = agent_write_file_bytes(path, out, out_len, err, sizeof(err));
    if (rc != 0) {
        free(out);
        free(write_stderr.ptr);
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    agent_temp_files_note_write(w, path);

    int start_line = 0, end_line = 0, delta = 0;
    agent_old_new_line_effect(data, len, out, out_len, offset, remove_len,
                              &start_line, &end_line, &delta);
    char *result = agent_edit_result(path, start_line, end_line, delta,
                                     out, out_len, kind);
    if (stderr_out && stderr_out->ptr && stderr_out->len > 0 && result) {
        agent_buf b = {0};
        agent_buf_puts(&b, result);
        free(result);
        agent_tool_append_stderr_warning(&b, stderr_out);
        result = agent_buf_take(&b);
    }
    if (write_stderr.ptr && write_stderr.len > 0 && result) {
        agent_buf b = {0};
        agent_buf_puts(&b, result);
        free(result);
        agent_tool_append_stderr_warning(&b, &write_stderr);
        result = agent_buf_take(&b);
    }
    free(write_stderr.ptr);
    free(out);
    return result;
}

/* Old/new editing is intentionally conservative: exact old text must be unique.
 * For large replacements, old may contain one [upto] marker: the head must be
 * unique, and the tail must be unique after that head before the whole span is
 * replaced.  When old is anchored, new may contain one [upto] marker to keep
 * the omitted original middle. */
static char *agent_tool_edit(agent_worker *w, const agent_tool_call *call) {
    const char *path = agent_tool_arg_value(call, "path");
    if (!path || !path[0]) return xstrdup("Tool error: edit requires path\n");
    const char *old = agent_tool_arg_value(call, "old");
    const char *new_text = agent_tool_arg_value(call, "new");
    if (!old || !old[0]) return xstrdup("Tool error: edit requires non-empty old text\n");
    if (!new_text) return xstrdup("Tool error: edit requires new text\n");

    char err[256];
    char *data = NULL;
    size_t len = 0;
    char *file_path = NULL;
    const char *display_path = path;
    if (agent_tool_use_docker_filesystem(w)) {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        if (!agent_docker_read_file_bytes(w, file_path, &data, &len, err,
                                          sizeof(err), NULL)) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_read(w, file_path);
    } else {
        file_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                            err, sizeof(err));
        if (!file_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        display_path = file_path;
        if (agent_read_file_bytes(file_path, &data, &len, err, sizeof(err)) != 0) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            free(file_path);
            return agent_buf_take(&b);
        }
        agent_temp_files_note_read(w, file_path);
    }

    const char *match = NULL;
    size_t match_len = 0;
    bool anchored = false;
    bool new_used_upto = false;
    agent_edit_anchor_span anchor_span = {0};
    if (!agent_edit_find_old_span(data, len, old, &match, &match_len,
                                  &anchored, &anchor_span, err, sizeof(err)))
    {
        free(data);
        free(file_path);
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }

    char *resolved_new = agent_edit_resolve_new_text(data, len, match, match_len,
                                                     &anchor_span, new_text,
                                                     &new_used_upto,
                                                     err, sizeof(err));
    if (!resolved_new) {
        free(data);
        free(file_path);
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }

    char *result = agent_apply_file_splice(w, display_path, data, len,
                                           (size_t)(match - data), match_len,
                                           resolved_new,
                                           new_used_upto ? "anchored old/new merge"
                                           : anchored ? "anchored old/new replacement"
                                                      : "old/new replacement",
                                           NULL);
    free(resolved_new);
    free(file_path);
    free(data);
    return result;
}

typedef struct {
    const char *query;
    const char *glob;
    regex_t regex;
    bool use_regex;
    bool regex_ready;
    bool case_sensitive;
    int context;
    int max_results;
    int results;
    agent_buf out;
} agent_search_ctx;

static bool agent_literal_match(const char *s, size_t n, const char *q,
                                bool case_sensitive) {
    size_t qn = strlen(q);
    if (!qn) return true;
    if (qn > n) return false;
    for (size_t i = 0; i + qn <= n; i++) {
        bool ok = true;
        for (size_t j = 0; j < qn; j++) {
            unsigned char a = (unsigned char)s[i + j];
            unsigned char b = (unsigned char)q[j];
            if (!case_sensitive) {
                a = (unsigned char)tolower(a);
                b = (unsigned char)tolower(b);
            }
            if (a != b) {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }
    return false;
}

static bool agent_search_line_matches(agent_search_ctx *ctx, const char *s, size_t n) {
    if (ctx->use_regex) {
        char *line = xstrndup(s, n);
        int rc = regexec(&ctx->regex, line, 0, NULL, 0);
        free(line);
        return rc == 0;
    }
    return agent_literal_match(s, n, ctx->query, ctx->case_sensitive);
}

static void agent_search_emit_line(agent_search_ctx *ctx, const char *data,
                                   agent_line_span sp, int line_no) {
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "  %d ", line_no);
    agent_buf_puts(&ctx->out, prefix);
    agent_buf_append(&ctx->out, data + sp.start, sp.content_end - sp.start);
    agent_buf_puts(&ctx->out, "\n");
}

/* Search one text file and emit matching lines with plain line numbers. */
static void agent_search_file(agent_search_ctx *ctx, const char *path) {
    if (ctx->results >= ctx->max_results) return;
    if (ctx->glob && ctx->glob[0]) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        if (fnmatch(ctx->glob, base, 0) != 0 && fnmatch(ctx->glob, path, 0) != 0)
            return;
    }
    char err[256];
    char *data = NULL;
    size_t len = 0;
    if (agent_read_file_bytes(path, &data, &len, err, sizeof(err)) != 0) return;
    if (memchr(data, '\0', len)) {
        free(data);
        return;
    }
    agent_line_spans spans = {0};
    agent_split_lines(data, len, &spans);
    bool printed_file = false;
    int last_context_line = -1;
    for (int i = 0; i < spans.len && ctx->results < ctx->max_results; i++) {
        agent_line_span sp = spans.v[i];
        if (!agent_search_line_matches(ctx, data + sp.start, sp.content_end - sp.start))
            continue;
        if (!printed_file) {
            agent_buf_puts(&ctx->out, path);
            agent_buf_puts(&ctx->out, "\n");
            printed_file = true;
        }
        int from = i - ctx->context;
        int to = i + ctx->context;
        if (from < 0) from = 0;
        if (to >= spans.len) to = spans.len - 1;
        if (from <= last_context_line) from = last_context_line + 1;
        for (int j = from; j <= to; j++) {
            agent_search_emit_line(ctx, data, spans.v[j], j + 1);
            last_context_line = j;
        }
        ctx->results++;
    }
    if (printed_file) agent_buf_puts(&ctx->out, "\n");
    agent_line_spans_free(&spans);
    free(data);
}

/* Recursively search a file or directory, avoiding .git and stopping once the
 * result cap is reached. */
static void agent_search_path(agent_search_ctx *ctx, const char *path, int depth) {
    if (ctx->results >= ctx->max_results || depth > 24) return;
    struct stat st;
    if (lstat(path, &st) != 0) return;
    if (S_ISREG(st.st_mode)) {
        agent_search_file(ctx, path);
        return;
    }
    if (!S_ISDIR(st.st_mode)) return;
    DIR *dir = opendir(path);
    if (!dir) return;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL && ctx->results < ctx->max_results) {
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
        if (!strcmp(de->d_name, ".git")) continue;
        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", path, de->d_name);
        agent_search_path(ctx, child, depth + 1);
    }
    closedir(dir);
}

/* Implement the search tool using either literal matching or POSIX regex. */
static char *agent_tool_search(agent_worker *w, const agent_tool_call *call) {
    const char *query = agent_tool_arg_value(call, "query");
    if (!query || !query[0]) return xstrdup("Tool error: search requires query\n");
    const char *path = agent_tool_arg_value(call, "path");
    if (!path || !path[0]) path = ".";
    const char *mode = agent_tool_arg_value(call, "mode");
    bool use_regex = mode && !strcmp(mode, "regex");
    bool case_sensitive = agent_parse_bool_default(agent_tool_arg_value(call, "case_sensitive"), true);
    int context = agent_parse_int_default(agent_tool_arg_value(call, "context"), 0, 0, 5);
    int max_results = agent_parse_int_default(agent_tool_arg_value(call, "max_results"), 50, 1, 500);
    const char *glob = agent_tool_arg_value(call, "glob");
    if (agent_tool_use_docker_filesystem(w)) {
        char err[256];
        char *search_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                                    err, sizeof(err));
        if (!search_path) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, err);
            agent_buf_puts(&b, "\n");
            return agent_buf_take(&b);
        }
        char grep_flags[32] = "-nI";
        if (use_regex) strcat(grep_flags, "E");
        else strcat(grep_flags, "F");
        if (!case_sensitive) strcat(grep_flags, "i");
        char context_arg[32];
        snprintf(context_arg, sizeof(context_arg), "%d", context);
        char max_arg[32];
        snprintf(max_arg, sizeof(max_arg), "%d", max_results);
        const char *script =
            "{ if [ -d \"$1\" ]; then "
            "  if [ -n \"$4\" ]; then "
            "    find \"$1\" \\( -name .git -o -name node_modules -o -name .agents -o -name .codex -o -name build -o -name target -o -name .cache \\) -type d -prune -o -type f -name \"$4\" -print0 | xargs -0 grep \"$2\" -C \"$3\" -- \"$5\"; "
            "  else "
            "    find \"$1\" \\( -name .git -o -name node_modules -o -name .agents -o -name .codex -o -name build -o -name target -o -name .cache \\) -type d -prune -o -type f -print0 | xargs -0 grep \"$2\" -C \"$3\" -- \"$5\"; "
            "  fi; "
            "else "
            "  grep \"$2\" -C \"$3\" -- \"$5\" \"$1\"; "
            "fi; } | sed -n \"1,${6}p;${6}q\"";
        char *argv[] = {
            "/bin/sh", "-c", (char *)script, "sh",
            search_path, grep_flags, context_arg, (char *)(glob ? glob : ""),
            (char *)query, max_arg, NULL
        };
        agent_buf out = {0};
        agent_buf stderr_out = {0};
        int status = 0;
        if (!agent_docker_shell_exec_argv_stderr(w, argv, false,
                                                 "docker search", &out,
                                                 &stderr_out, &status) ||
            status != 0) {
            char *msg = agent_tool_error_with_stderr("docker search failed",
                                                     &stderr_out);
            free(out.ptr);
            free(stderr_out.ptr);
            free(search_path);
            return msg;
        }
        if (!out.ptr || !out.ptr[0]) {
            agent_buf result = {0};
            agent_buf_puts(&result, "No matches.\n");
            agent_tool_append_stderr_warning(&result, &stderr_out);
            free(out.ptr);
            free(stderr_out.ptr);
            free(search_path);
            return agent_buf_take(&result);
        }
        agent_tool_append_stderr_warning(&out, &stderr_out);
        free(stderr_out.ptr);
        free(search_path);
        return agent_buf_take(&out);
    }
    char err[256];
    char *search_path = agent_resolve_tool_path(w, path, AGENT_PATH_EXISTING,
                                                err, sizeof(err));
    if (!search_path) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: ");
        agent_buf_puts(&b, err);
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    agent_search_ctx ctx = {
        .query = query,
        .glob = glob,
        .use_regex = use_regex,
        .case_sensitive = case_sensitive,
        .context = context,
        .max_results = max_results,
    };
    if (ctx.use_regex) {
        int flags = REG_EXTENDED | REG_NOSUB;
        if (!ctx.case_sensitive) flags |= REG_ICASE;
        int rc = regcomp(&ctx.regex, query, flags);
        if (rc != 0) {
            char msg[256];
            regerror(rc, &ctx.regex, msg, sizeof(msg));
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: invalid regex: ");
            agent_buf_puts(&b, msg);
            agent_buf_puts(&b, "\n");
            free(search_path);
            return agent_buf_take(&b);
        }
        ctx.regex_ready = true;
    }
    agent_search_path(&ctx, search_path, 0);
    free(search_path);
    if (ctx.regex_ready) regfree(&ctx.regex);
    if (!ctx.out.ptr) agent_buf_puts(&ctx.out, "No matches\n");
    else {
        char hdr[96];
        snprintf(hdr, sizeof(hdr), "%d match%s shown\n\n",
                 ctx.results, ctx.results == 1 ? "" : "es");
        size_t hdr_len = strlen(hdr);
        if (ctx.out.len + hdr_len + 1 > ctx.out.cap) {
            ctx.out.cap = ctx.out.len + hdr_len + 1;
            ctx.out.ptr = xrealloc(ctx.out.ptr, ctx.out.cap);
        }
        memmove(ctx.out.ptr + hdr_len, ctx.out.ptr, ctx.out.len + 1);
        memcpy(ctx.out.ptr, hdr, hdr_len);
        ctx.out.len += hdr_len;
    }
    return agent_buf_take(&ctx.out);
}

/* ============================================================================
 * Browser Web Tools
 * ============================================================================
 *
 * The browser subsystem lives in ds4_web.c: it owns visible Chrome and CDP.  The
 * agent side only asks for permission, dispatches tools, and caps web_fetch
 * output using the same "head plus temp file" shape as bash.
 */

#define AGENT_WEB_HEAD_BYTES (8*1024)
#define AGENT_WEB_HEAD_LINES 100

static int agent_count_lines(const char *s) {
    if (!s || !s[0]) return 0;
    int lines = 0;
    for (const char *p = s; *p; p++) {
        if (*p == '\n') lines++;
    }
    if (s[strlen(s) - 1] != '\n') lines++;
    return lines;
}

static char *agent_string_head(const char *s, int max_lines, size_t max_bytes,
                               int *lines_read, bool *byte_limited) {
    if (lines_read) *lines_read = 0;
    if (byte_limited) *byte_limited = false;
    if (!s) return xstrdup("");
    size_t used = 0;
    int lines = 0;
    while (s[used] && used < max_bytes && lines < max_lines) {
        if (s[used++] == '\n') lines++;
    }
    if (s[used] && used >= max_bytes && byte_limited) *byte_limited = true;
    if (used && s[used - 1] != '\n' && lines < max_lines) lines++;
    if (lines_read) *lines_read = lines;
    return xstrndup(s, used);
}

static bool agent_write_temp_text(const char *temp_dir, const char *prefix,
                                  const char *text,
                                  char *path, size_t path_len,
                                  char *err, size_t err_len) {
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof(tmpl), "%s/%s_XXXXXX", temp_dir ? temp_dir : "/tmp",
             prefix);
    int fd = mkstemp(tmpl);
    if (fd < 0) {
        snprintf(err, err_len, "failed to create temporary file: %s", strerror(errno));
        return false;
    }
    size_t len = text ? strlen(text) : 0;
    const char *p = text ? text : "";
    size_t left = len;
    while (left) {
        ssize_t n = write(fd, p, left);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            snprintf(err, err_len, "failed to write temporary file: %s", strerror(errno));
            close(fd);
            unlink(tmpl);
            return false;
        }
        p += n;
        left -= (size_t)n;
    }
    if (close(fd) != 0) {
        snprintf(err, err_len, "failed to close temporary file: %s", strerror(errno));
        unlink(tmpl);
        return false;
    }
    snprintf(path, path_len, "%s", tmpl);
    return true;
}

static char *agent_tool_web_browse(agent_worker *w, const agent_tool_call *call) {
    const char *query = agent_tool_arg_value(call, "query");
    if (!query || !query[0]) return xstrdup("Tool error: web_browse requires query\n");
    char err[256] = {0};
    agent_publishf_system_status(w, "Searching Google for %s...", query);
    char *md = ds4_web_google_search(w->web, query, err, sizeof(err));
    if (!md) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: web_browse failed: ");
        agent_buf_puts(&b, err[0] ? err : "unknown error");
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    return md;
}

static char *agent_tool_web_fetch(agent_worker *w, const agent_tool_call *call) {
    const char *url = agent_tool_arg_value(call, "url");
    if (!url || !url[0]) return xstrdup("Tool error: web_fetch requires url\n");
    char err[256] = {0};
    agent_publishf_system_status(w, "Opening page %s...", url);
    char *md = ds4_web_visit_page(w->web, url, err, sizeof(err));
    if (!md) {
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: web_fetch failed: ");
        agent_buf_puts(&b, err[0] ? err : "unknown error");
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }

    char path[PATH_MAX];
    if (!agent_write_temp_text(w->cfg->temp_directory, "ds4_agent_web", md,
                               path, sizeof(path), err, sizeof(err)))
    {
        free(md);
        agent_buf b = {0};
        agent_buf_puts(&b, "Tool error: web_fetch failed: ");
        agent_buf_puts(&b, err[0] ? err : "could not store rendered page");
        agent_buf_puts(&b, "\n");
        return agent_buf_take(&b);
    }
    /* Allow the agent to read rendered page files and clean them up later. */
    agent_temp_files_track(w, path);
    int total_lines = agent_count_lines(md);
    int shown_lines = 0;
    bool byte_limited = false;
    char *head = agent_string_head(md, AGENT_WEB_HEAD_LINES, AGENT_WEB_HEAD_BYTES,
                                   &shown_lines, &byte_limited);
    bool truncated = byte_limited || shown_lines < total_lines;
    agent_buf out = {0};
    char line[PATH_MAX + 256];
    snprintf(line, sizeof(line),
             "web_fetch url=%s\noutput_path=%s (%zu bytes, %d lines)\n",
             url, path, strlen(md), total_lines);
    agent_buf_puts(&out, line);
    if (truncated) {
        snprintf(line, sizeof(line), "<head -%d %s>\n",
                 AGENT_WEB_HEAD_LINES, path);
        agent_buf_puts(&out, line);
        agent_buf_puts(&out, head);
        if (head[0] && head[strlen(head) - 1] != '\n') agent_buf_puts(&out, "\n");
        agent_buf_puts(&out, "</head>\n");
        agent_buf_puts(&out,
            "Use read path=<output_path> start_line=<line> max_lines=<count> raw=true to inspect more rendered Markdown.\n");
    } else {
        agent_buf_puts(&out, "<markdown>\n");
        agent_buf_puts(&out, head);
        if (head[0] && head[strlen(head) - 1] != '\n') agent_buf_puts(&out, "\n");
        agent_buf_puts(&out, "</markdown>\n");
    }
    free(head);
    free(md);
    /* Defer deletion - the model may need to read this file via the read tool.
     * Cleanup happens during worker free or via /purge_auto_files. */
    return agent_buf_take(&out);
}

/* ============================================================================
 * Asynchronous Bash Jobs
 * ============================================================================
 *
 * Bash commands are tracked jobs, not blocking one-shot calls.  Each job owns a
 * process, a pipe, and a secure /tmp output file.  The first observation is
 * head-biased so headers and early errors are visible; later progress updates
 * are tail-biased and report how much output was added since the previous
 * observation.
 */

#define AGENT_BASH_HEAD_BYTES (8*1024)
#define AGENT_BASH_HEAD_LINES 100
#define AGENT_BASH_TAIL_BYTES (32*1024)
#define AGENT_BASH_PROGRESS_TAIL_LINES 4
#define AGENT_BASH_FINAL_TAIL_LINES 20

struct agent_bash_job {
    int id;
    pid_t pid;
    int pipe_fd;
    int tmp_fd;
    char path[PATH_MAX];
    char *cmd;
    double start_time;
    double timeout_sec;
    size_t bytes;
    int newline_count;
    char last_byte;
    size_t observed_bytes;
    int observed_display_lines;
    bool observed_once;
    bool command_output_seen;
    int exit_status;
    bool running;
    bool timed_out;
    struct agent_bash_job *next;
    agent_worker *worker;  /* back-pointer for terminal state restoration */
};

static int agent_bash_display_lines(const agent_bash_job *job) {
    if (!job || job->bytes == 0) return 0;
    return job->newline_count + (job->last_byte != '\n');
}

static void agent_bash_publish_output_chunk(agent_bash_job *job,
                                            const char *s, size_t n) {
    if (!job) return;
    agent_publish_command_output_chunk(job->worker, s, n,
                                       &job->command_output_seen);
}

static void agent_bash_note_output(agent_bash_job *job, const char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '\n') job->newline_count++;
    }
    if (n) job->last_byte = s[n - 1];
    job->bytes += n;
}

static void agent_bash_job_free(agent_bash_job *job) {
    if (!job) return;
    if (job->running && job->pid > 0) {
        kill(-job->pid, SIGKILL);
        kill(job->pid, SIGKILL);
        waitpid(job->pid, NULL, 0);
    }
    if (job->pipe_fd >= 0) close(job->pipe_fd);
    if (job->tmp_fd >= 0) close(job->tmp_fd);
    /* Temp output files are retained for later read/more calls and are cleaned
     * by the worker only after they have been idle long enough. */
    free(job->cmd);
    free(job);
}

static void agent_bash_jobs_free(agent_worker *w) {
    agent_bash_job *job = w->bash_jobs;
    while (job) {
        agent_bash_job *next = job->next;
        agent_bash_job_free(job);
        job = next;
    }
    w->bash_jobs = NULL;
}

static agent_bash_job *agent_bash_find_job(agent_worker *w, int id, pid_t pid) {
    for (agent_bash_job *job = w->bash_jobs; job; job = job->next) {
        if ((id > 0 && job->id == id) || (id <= 0 && pid > 0 && job->pid == pid))
            return job;
    }
    return NULL;
}

static void agent_bash_remove_job(agent_worker *w, agent_bash_job *target) {
    agent_bash_job **link = &w->bash_jobs;
    while (*link) {
        if (*link == target) {
            *link = target->next;
            target->next = NULL;
            agent_bash_job_free(target);
            return;
        }
        link = &(*link)->next;
    }
}

static void agent_bash_drain(agent_bash_job *job) {
    if (!job || job->pipe_fd < 0) return;
    char tmp[4096];
    for (;;) {
        ssize_t n = read(job->pipe_fd, tmp, sizeof(tmp));
        if (n > 0) {
            agent_bash_note_output(job, tmp, (size_t)n);
            if (job->tmp_fd >= 0) {
                write_all(job->tmp_fd, tmp, (size_t)n);
                agent_temp_files_note_write(job->worker, job->path);
            }
            agent_bash_publish_output_chunk(job, tmp, (size_t)n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
}

static void agent_worker_note_terminal_mode_may_have_changed(agent_worker *w) {
    if (!w) return;
    pthread_mutex_lock(&w->mu);
    w->raw_mode_needs_restore = true;
    pthread_mutex_unlock(&w->mu);
}

static void agent_bash_finalize(agent_bash_job *job, int status) {
    agent_bash_drain(job);
    if (job && job->command_output_seen && job->last_byte != '\n' &&
        job->worker && job->worker->cfg && job->worker->cfg->command_output)
        agent_publish(job->worker, "\n", 1);
    if (job->pipe_fd >= 0) {
        close(job->pipe_fd);
        job->pipe_fd = -1;
    }
    if (job->tmp_fd >= 0) {
        close(job->tmp_fd);
        job->tmp_fd = -1;
    }
    if (WIFEXITED(status)) job->exit_status = WEXITSTATUS(status);
    else if (WIFSIGNALED(status)) job->exit_status = 128 + WTERMSIG(status);
    else job->exit_status = -1;
    job->running = false;
    /* A child can still open /dev/tty directly and alter terminal state even
     * though its stdin is /dev/null.  Ask the UI thread to verify raw mode at
     * a safe point instead of touching linenoise from the worker path. */
    agent_worker_note_terminal_mode_may_have_changed(job->worker);
}

/* Drain available output, notice process exit, and enforce timeout.  This is
 * called opportunistically by status/wait/compaction instead of a background
 * reaper thread, keeping all bash job state owned by the agent worker. */
static void agent_bash_poll(agent_bash_job *job) {
    if (!job || !job->running) return;
    agent_bash_drain(job);

    int status = 0;
    pid_t rc = waitpid(job->pid, &status, WNOHANG);
    if (rc == job->pid) {
        agent_bash_finalize(job, status);
        return;
    }
    if (rc < 0 && errno != EINTR) {
        job->exit_status = -1;
        job->running = false;
        if (job->pipe_fd >= 0) {
            close(job->pipe_fd);
            job->pipe_fd = -1;
        }
        if (job->tmp_fd >= 0) {
            close(job->tmp_fd);
            job->tmp_fd = -1;
        }
        agent_worker_note_terminal_mode_may_have_changed(job->worker);
        return;
    }
    if (now_sec() - job->start_time >= job->timeout_sec) {
        job->timed_out = true;
        kill(-job->pid, SIGKILL);
        kill(job->pid, SIGKILL);
        while (waitpid(job->pid, &status, 0) < 0 && errno == EINTR) {}
        agent_bash_finalize(job, status);
    }
}

static bool agent_executable_exists(const char *path) {
    return path && path[0] && access(path, X_OK) == 0;
}

static bool agent_command_in_path(const char *command) {
    if (!command || !command[0]) return false;
    if (strchr(command, '/')) return agent_executable_exists(command);

    const char *path = getenv("PATH");
    if (!path || !path[0]) return false;
    const char *p = path;
    while (*p) {
        const char *end = strchr(p, ':');
        size_t n = end ? (size_t)(end - p) : strlen(p);
        if (n > 0) {
            char dir[PATH_MAX];
            if (n >= sizeof(dir)) n = sizeof(dir) - 1;
            memcpy(dir, p, n);
            dir[n] = '\0';

            char candidate[PATH_MAX];
            int written = snprintf(candidate, sizeof(candidate), "%s/%s", dir,
                                   command);
            if (written > 0 && written < (int)sizeof(candidate) &&
                agent_executable_exists(candidate))
                return true;
        }
        if (!end) break;
        p = end + 1;
    }
    return false;
}

/* What: run `docker version` and copy the client version string.
 * Why: startup diagnostics should tell the user which Docker command was found
 * without requiring an active sandbox container.
 * Callers: main() while reporting Docker availability. */
static bool agent_read_docker_version(const char *docker_command,
                                      char *version, size_t version_len) {
    if (!docker_command || !docker_command[0] || !version || version_len == 0)
        return false;
    version[0] = '\0';

    agent_config tmp_cfg = {0};
    tmp_cfg.docker_command = docker_command;
    const char *const argv[] = {"version", "--format", "{{.Client.Version}}", NULL};
    agent_buf out = {0};
    int status = 0;
    if (!agent_docker_exec(NULL, &tmp_cfg, argv, NULL, 0, true,
                           "docker version", &out, &status))
        return false;
    if (!out.ptr || !out.ptr[0]) {
        free(out.ptr);
        return false;
    }
    size_t copy = out.len;
    if (copy >= version_len) copy = version_len - 1;
    memcpy(version, out.ptr, copy);
    version[copy] = '\0';
    free(out.ptr);

    /* Strip trailing whitespace (including newlines, carriage returns, etc.) */
    size_t nread = strlen(version);
    while (nread > 0 &&
           (version[nread - 1] == '\n' || version[nread - 1] == '\r' ||
            version[nread - 1] == ' ' || version[nread - 1] == '\t')) {
        version[--nread] = '\0';
    }
    return nread > 0;
}

static void agent_bash_prepare_env(const char *working_dir,
                                   const char *temp_dir) {
    if (!working_dir || !working_dir[0]) return;

    /* Keep command-local config inside the approved workspace so common
     * developer tools do not abort on blocked HOME lookups.  Temp files go
     * to the configured temp directory so they are accessible. */
    setenv("HOME", working_dir, 1);
    setenv("TMPDIR", temp_dir && temp_dir[0] ? temp_dir : working_dir, 1);
    setenv("TMP", temp_dir && temp_dir[0] ? temp_dir : working_dir, 1);
    setenv("TEMP", temp_dir && temp_dir[0] ? temp_dir : working_dir, 1);
    setenv("XDG_CONFIG_HOME", working_dir, 1);
    setenv("XDG_CACHE_HOME", working_dir, 1);
    setenv("TERM", "dumb", 1);
    setenv("PAGER", "cat", 1);
    setenv("GIT_PAGER", "cat", 1);

    /* Git treats permission errors while probing global/system config as
     * fatal. Point those lookups at in-jail paths instead of blocked ones. */
    setenv("GIT_CONFIG_NOSYSTEM", "1", 1);
    setenv("GIT_CONFIG_SYSTEM", "/dev/null", 1);
    setenv("GIT_CONFIG_GLOBAL", "/dev/null", 1);
}

/* What: report whether a worker has an active Docker sandbox container.
 * Why: bash jobs and filesystem tools share the same active-sandbox predicate.
 * Callers: agent_docker_shell_start(), agent_tool_use_docker_filesystem(),
 * agent_bash_exec_docker(), agent_tool_dispatch(), and worker startup. */
static bool agent_bash_use_docker_sandbox(const agent_worker *w) {
    return w && w->cfg &&
        w->cfg->docker_available &&
        w->cfg->docker_container &&
        w->cfg->docker_container[0];
}

/* What: decide whether file tools should address paths inside Docker.
 * Why: read/write/edit/list/search must switch filesystem backends as a group
 * when a sandbox is selected.
 * Callers: agent_read_range(), agent_tool_write(), agent_tool_edit(),
 * agent_tool_list(), and agent_tool_search(). */
static bool agent_tool_use_docker_filesystem(const agent_worker *w) {
    return agent_bash_use_docker_sandbox(w);
}

/* What: enforce strict-sandbox mode for model tools except web tools.
 * Why: strict mode should fail closed before filesystem or bash execution can
 * touch the host when no Docker sandbox is selected.
 * Callers: agent_tool_dispatch(). */
static bool agent_tool_requires_docker_sandbox(const agent_worker *w,
                                               const char *tool_name) {
    if (!w || !w->cfg || !w->cfg->strict_sandbox) return false;
    if (tool_name &&
        (!strcmp(tool_name, "web_browse") || !strcmp(tool_name, "web_fetch")))
        return false;
    return true;
}

static char *agent_tool_sandbox_required_error(void) {
    return xstrdup("sandbox not set, /no_strict_sandbox to disable this error\n");
}

static void agent_buf_append_shell_quoted(agent_buf *b, const char *s) {
    bool needs_quote = !s || !s[0];
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (isspace(*p) || *p == '\'' || *p == '"' || *p == '\\' ||
            *p == '$' || *p == ';' || *p == '&' || *p == '|' ||
            *p == '>' || *p == '<' || *p == '`' || *p == '(' ||
            *p == ')' || *p == '{' || *p == '}' || *p == '[' ||
            *p == ']' || *p == '~' || *p == '*' || *p == '?' ||
            *p == '!' || *p == '#' || *p == '=' || *p == '%' ||
            *p == '^' || *p == '@') {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        agent_buf_puts(b, s ? s : "");
        return;
    }
    agent_buf_puts(b, "'");
    for (const char *p = s ? s : ""; *p; p++) {
        if (*p == '\'') agent_buf_puts(b, "'\\''");
        else agent_buf_append(b, p, 1);
    }
    agent_buf_puts(b, "'");
}

/* What: format a Docker argv vector as a shell-quoted debug command string.
 * Why: `/docker debug` should show the exact command shape without executing
 * through a shell or losing quoting information.
 * Callers: agent_docker_exec(), agent_bash_start_mode(), and
 * agent_command_docker_create(); tests inspect its output. */
static char *agent_docker_debug_command_text(char *const argv[]) {
    agent_buf b = {0};
    for (int i = 0; argv && argv[i]; i++) {
        if (i) agent_buf_puts(&b, " ");
        agent_buf_append_shell_quoted(&b, argv[i]);
    }
    return agent_buf_take(&b);
}

static void agent_exec_command(const char *command, char *const argv[]) {
    if (strchr(command, '/')) execv(command, argv);
    else execvp(command, argv);
}

/* What: append one `-e KEY=value` pair to a Docker argv using caller storage.
 * Why: Docker env strings must remain valid until exec, so callers provide the
 * backing buffers while this helper keeps argv construction uniform.
 * Callers: agent_docker_exec_add_standard_env(). */
static void agent_docker_exec_add_env(char **argv, int *argc,
                                      char *buf, size_t len,
                                      const char *key, const char *value) {
    if (!argv || !argc || !buf || len == 0 || !key || !key[0] ||
        !value || !value[0])
        return;
    memset(buf, 0, len);
    snprintf(buf, len, "%s=%s", key, value);
    argv[(*argc)++] = "-e";
    argv[(*argc)++] = buf;
}

/* What: append the standard DS4 tool environment to a Docker exec argv.
 * Why: sandboxed commands should see the same HOME/temp/pager/git-config
 * behavior as local tools and avoid probing blocked host locations.
 * Callers: agent_docker_shell_start(), agent_docker_build_shell_argv(), and
 * agent_docker_write_file_bytes(). */
static void agent_docker_exec_add_standard_env(char **argv, int *argc,
                                               char *home_env, size_t home_len,
                                               char *tmpdir_env, size_t tmpdir_len,
                                               char *tmp_env, size_t tmp_len,
                                               char *temp_env, size_t temp_len,
                                               char *xdg_config_env, size_t xdg_config_len,
                                               char *xdg_cache_env, size_t xdg_cache_len,
                                               const char *working_dir,
                                               const char *temp_dir) {
    const char *effective_temp =
        (temp_dir && temp_dir[0]) ? temp_dir : working_dir;
    agent_docker_exec_add_env(argv, argc, home_env, home_len,
                              "HOME", working_dir);
    agent_docker_exec_add_env(argv, argc, tmpdir_env, tmpdir_len,
                              "TMPDIR", effective_temp);
    agent_docker_exec_add_env(argv, argc, tmp_env, tmp_len,
                              "TMP", effective_temp);
    agent_docker_exec_add_env(argv, argc, temp_env, temp_len,
                              "TEMP", effective_temp);
    agent_docker_exec_add_env(argv, argc, xdg_config_env, xdg_config_len,
                              "XDG_CONFIG_HOME", working_dir);
    agent_docker_exec_add_env(argv, argc, xdg_cache_env, xdg_cache_len,
                              "XDG_CACHE_HOME", working_dir);
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "TERM=dumb";
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "PAGER=cat";
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "GIT_PAGER=cat";
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "GIT_CONFIG_NOSYSTEM=1";
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "GIT_CONFIG_SYSTEM=/dev/null";
    argv[(*argc)++] = "-e"; argv[(*argc)++] = "GIT_CONFIG_GLOBAL=/dev/null";
}

/* What: build `docker exec -i ... /bin/sh -c <cmd>` argv for one shell command.
 * Why: bash-in-Docker and debug logging need the same argv construction and env
 * parity as filesystem commands, but they do not use the persistent shell.
 * Callers: agent_bash_exec_docker() and agent_bash_start_mode(). */
static void agent_docker_build_shell_argv(char **argv, int *argc,
                                          const char *docker_command,
                                          const char *container,
                                          const char *working_dir,
                                          const char *temp_dir,
                                          const char *cmd,
                                          char *home_env, size_t home_len,
                                          char *tmpdir_env, size_t tmpdir_len,
                                          char *tmp_env, size_t tmp_len,
                                          char *temp_env, size_t temp_len,
                                          char *xdg_config_env, size_t xdg_config_len,
                                          char *xdg_cache_env, size_t xdg_cache_len) {
    *argc = 0;
    argv[(*argc)++] = (char *)docker_command;
    argv[(*argc)++] = "exec";
    argv[(*argc)++] = "-i";
    if (working_dir && working_dir[0]) {
        argv[(*argc)++] = "-w";
        argv[(*argc)++] = (char *)working_dir;
    }
    agent_docker_exec_add_standard_env(argv, argc,
                                       home_env, home_len,
                                       tmpdir_env, tmpdir_len,
                                       tmp_env, tmp_len,
                                       temp_env, temp_len,
                                       xdg_config_env, xdg_config_len,
                                       xdg_cache_env, xdg_cache_len,
                                       working_dir, temp_dir);
    argv[(*argc)++] = (char *)container;
    argv[(*argc)++] = "/bin/sh";
    argv[(*argc)++] = "-c";
    argv[(*argc)++] = (char *)(cmd ? cmd : "");
    argv[*argc] = NULL;
}

/* What: run a host-side Docker CLI command, optionally feeding stdin and
 * capturing combined stdout/stderr.
 * Why: one-shot Docker commands need a single fork/exec/capture primitive, and
 * write-heavy commands must poll stdout while feeding stdin to avoid pipe
 * deadlocks.
 * Callers: agent_read_docker_version(), agent_docker_capture(),
 * agent_docker_write_file_bytes(), agent_command_docker_create(), and
 * agent_command_docker_stop(); unit tests cover invalid input, debug output,
 * empty stdin, and large stdin.
 *
 * argv_tail starts with the Docker subcommand, e.g. {"ps", "-a", NULL}.
 * stdin_data == NULL connects stdin to /dev/null; non-NULL writes stdin_len
 * bytes exactly, including zero-byte empty-file writes. */
static bool agent_docker_exec(agent_worker *w,
                               const agent_config *cfg,
                               const char *const argv_tail[],
                               const char *stdin_data,
                               size_t stdin_len,
                               bool unbounded_output,
                               const char *op,
                               agent_buf *out,
                               int *exit_status) {
    if (out) memset(out, 0, sizeof(*out));
    if (exit_status) *exit_status = -1;
    if (!cfg || !argv_tail || !argv_tail[0]) return false;

    const char *docker_command =
        (cfg->docker_command && cfg->docker_command[0]) ?
        cfg->docker_command : "docker";
    int tail_argc = 0;
    while (argv_tail[tail_argc]) tail_argc++;

    char *argv[48];
    int argc = 0;
    argv[argc++] = (char *)docker_command;
    for (int i = 0; i < tail_argc; i++)
        argv[argc++] = (char *)argv_tail[i];
    argv[argc] = NULL;

    if (w && w->cfg && w->cfg->docker_debug) {
        char *cmd = agent_docker_debug_command_text(argv);
        if (cmd) {
            const char *prefix = "\x1b[96m[docker debug] ";
            const char *suffix = "\x1b[0m\n\n";
            agent_publish(w, prefix, strlen(prefix));
            agent_publish(w, cmd, strlen(cmd));
            agent_publish(w, suffix, strlen(suffix));
            free(cmd);
        }
    } else if (cfg && cfg->docker_debug) {
        char *cmd = agent_docker_debug_command_text(argv);
        if (cmd) {
            bool color = isatty(STDOUT_FILENO) != 0;
            if (color) printf("\x1b[96m[docker debug] %s\x1b[0m\n\n", cmd);
            else printf("[docker debug] %s\n\n", cmd);
            fflush(stdout);
            free(cmd);
        }
    }

    int stdin_pipe[2] = {-1, -1};
    if (stdin_data && pipe(stdin_pipe) != 0) {
        if (out) agent_buf_puts(out, "failed to create stdin pipe");
        return false;
    }
    int stdout_pipe[2];
    if (pipe(stdout_pipe) != 0) {
        if (stdin_pipe[0] >= 0) {
            close(stdin_pipe[0]);
            close(stdin_pipe[1]);
        }
        if (out) agent_buf_puts(out, "failed to create stdout pipe");
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        if (stdin_pipe[0] >= 0) {
            close(stdin_pipe[0]);
            close(stdin_pipe[1]);
        }
        close(stdout_pipe[0]);
        close(stdout_pipe[1]);
        if (out) agent_buf_puts(out, "failed to fork");
        return false;
    }
    if (pid == 0) {
        if (stdin_pipe[0] >= 0) {
            close(stdin_pipe[1]);
            dup2(stdin_pipe[0], STDIN_FILENO);
            close(stdin_pipe[0]);
        } else {
            int null_fd = open("/dev/null", O_RDONLY);
            if (null_fd >= 0) {
                dup2(null_fd, STDIN_FILENO);
                if (null_fd != STDIN_FILENO) close(null_fd);
            } else {
                close(STDIN_FILENO);
            }
        }
        close(stdout_pipe[0]);
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stdout_pipe[1], STDERR_FILENO);
        close(stdout_pipe[1]);
        agent_exec_command(docker_command, argv);
        dprintf(STDERR_FILENO, "%s failed to exec docker: %s\n",
                op ? op : "docker exec", strerror(errno));
        _exit(127);
    }

    size_t stdin_written = 0;
    bool stdin_open = stdin_pipe[0] >= 0;
    if (stdin_open) {
        close(stdin_pipe[0]);
        set_nonblock(stdin_pipe[1], true, NULL);
    }
    close(stdout_pipe[1]);
    set_nonblock(stdout_pipe[0], true, NULL);
    if (out) memset(out, 0, sizeof(*out));
    char chunk[4096];
    bool stdout_open = true;
    while (stdout_open || stdin_open) {
        struct pollfd pfds[2];
        nfds_t nfds = 0;
        int stdout_idx = -1;
        int stdin_idx = -1;
        if (stdout_open) {
            stdout_idx = (int)nfds;
            pfds[nfds].fd = stdout_pipe[0];
            pfds[nfds].events = POLLIN | POLLHUP | POLLERR;
            pfds[nfds].revents = 0;
            nfds++;
        }
        if (stdin_open) {
            stdin_idx = (int)nfds;
            pfds[nfds].fd = stdin_pipe[1];
            pfds[nfds].events = POLLOUT | POLLHUP | POLLERR;
            pfds[nfds].revents = 0;
            nfds++;
        }
        int prc = poll(pfds, nfds, -1);
        if (prc < 0) {
            if (errno == EINTR) continue;
            if (stdin_open) close(stdin_pipe[1]);
            close(stdout_pipe[0]);
            while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
            return false;
        }
        if (stdout_idx >= 0 &&
            (pfds[stdout_idx].revents & (POLLIN | POLLHUP | POLLERR))) {
            for (;;) {
                ssize_t n = read(stdout_pipe[0], chunk, sizeof(chunk));
                if (n > 0) {
                    if (out) {
                        if (unbounded_output) agent_buf_append_full(out, chunk, (size_t)n);
                        else agent_buf_append(out, chunk, (size_t)n);
                    }
                    continue;
                }
                if (n == 0) {
                    close(stdout_pipe[0]);
                    stdout_open = false;
                    break;
                }
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close(stdout_pipe[0]);
                if (stdin_open) close(stdin_pipe[1]);
                while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
                return false;
            }
        }
        if (stdin_idx >= 0 &&
            (pfds[stdin_idx].revents & (POLLOUT | POLLHUP | POLLERR))) {
            while (stdin_written < stdin_len) {
                ssize_t n = write(stdin_pipe[1], stdin_data + stdin_written,
                                  stdin_len - stdin_written);
                if (n > 0) {
                    stdin_written += (size_t)n;
                    continue;
                }
                if (n < 0 && errno == EINTR) continue;
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
                close(stdin_pipe[1]);
                stdin_open = false;
                break;
            }
            if (stdin_open && stdin_written == stdin_len) {
                close(stdin_pipe[1]);
                stdin_open = false;
            }
        }
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) return false;
    }
    if (exit_status) *exit_status = status;
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* What: read an entire sandbox file into memory through `cat` in the
 * persistent Docker shell.
 * Why: whole-file reads need uncapped capture so large files are not truncated,
 * while still resolving the path in the container filesystem.
 * Callers: agent_read_range() for whole-file Docker reads, agent_tool_edit(),
 * agent_tool_list(), and agent_tool_search(). */
static bool agent_docker_read_file_bytes(agent_worker *w, const char *path,
                                         char **data, size_t *len,
                                         char *err, size_t err_len,
                                         agent_buf *stderr_out) {
    if (data) *data = NULL;
    if (len) *len = 0;
    if (stderr_out) memset(stderr_out, 0, sizeof(*stderr_out));
    if (!path || !path[0]) {
        snprintf(err, err_len, "%s", 
                 "docker read failed, path is empty");
        return false;
    }
    char *argv[] = {"cat", (char *)(path), NULL};
    agent_buf out = {0};
    agent_buf shell_err = {0};
    int status = 0;
    bool ok = agent_docker_shell_exec_argv_stderr(w, argv, true,
                                                  "docker read", &out,
                                                  &shell_err, &status);
    if (!ok || status != 0) {
        snprintf(err, err_len, "%s",
                 shell_err.ptr && shell_err.ptr[0] ? shell_err.ptr :
                 (out.ptr && out.ptr[0] ? out.ptr : "docker read failed"));
        free(out.ptr);
        free(shell_err.ptr);
        return false;
    }
    if (data) *data = out.ptr ? out.ptr : xstrdup("");
    else free(out.ptr);
    if (len) *len = out.len;
    if (stderr_out) *stderr_out = shell_err;
    else free(shell_err.ptr);
    return true;
}

/* What: write bytes to a sandbox file by streaming stdin into
 * `docker exec ... sh -c 'cat > "$1"'`.
 * Why: file writes must preserve arbitrary content, including empty files and
 * binary bytes, so they use direct Docker stdin instead of shell interpolation.
 * Callers: agent_tool_write() and agent_tool_edit(). */
static bool agent_docker_write_file_bytes(agent_worker *w, const char *path,
                                          const char *data, size_t len,
                                          char *err, size_t err_len,
                                          agent_buf *stderr_out) {
    const char *working_dir = agent_primary_working_directory(w);
    const char *temp_dir = (w && w->cfg && w->cfg->temp_directory[0]) ?
        w->cfg->temp_directory : NULL;
    char parent[PATH_MAX];
    if (!path || !path[0]) {
        snprintf(err, err_len, "missing path");
        return false;
    }
    if (!agent_path_parent(path, parent))
        snprintf(parent, sizeof(parent), ".");

    char *dir_argv[] = {"test", "-d", parent, NULL};
    agent_buf out = {0};
    agent_buf dir_err = {0};
    int dir_status = 0;
    bool ok = agent_docker_shell_exec_argv_stderr(w, dir_argv, false,
                                                  "docker write", &out,
                                                  &dir_err, &dir_status);
    free(out.ptr);
    out.ptr = NULL;
    out.len = 0;
    out.cap = 0;
    if (!ok || dir_status != 0) {
        snprintf(err, err_len, "%s",
                 dir_err.ptr && dir_err.ptr[0] ? dir_err.ptr :
                 "parent directory does not exist");
        free(dir_err.ptr);
        return false;
    }
    if (stderr_out) *stderr_out = dir_err;
    else free(dir_err.ptr);

    char home_env[PATH_MAX + 16];
    char tmpdir_env[PATH_MAX + 16];
    char tmp_env[PATH_MAX + 16];
    char temp_env[PATH_MAX + 16];
    char xdg_config_env[PATH_MAX + 32];
    char xdg_cache_env[PATH_MAX + 32];
    char *tail[48];
    int argc = 0;
    tail[argc++] = "exec";
    tail[argc++] = "-i";
    if (working_dir && working_dir[0]) {
        tail[argc++] = "-w";
        tail[argc++] = (char *)working_dir;
    }
    agent_docker_exec_add_standard_env(tail, &argc,
                                       home_env, sizeof(home_env),
                                       tmpdir_env, sizeof(tmpdir_env),
                                       tmp_env, sizeof(tmp_env),
                                       temp_env, sizeof(temp_env),
                                       xdg_config_env, sizeof(xdg_config_env),
                                       xdg_cache_env, sizeof(xdg_cache_env),
                                       working_dir, temp_dir);
    tail[argc++] = (char *)w->cfg->docker_container;
    tail[argc++] = "/bin/sh";
    tail[argc++] = "-c";
    tail[argc++] = "cat > \"$1\"";
    tail[argc++] = "sh";
    tail[argc++] = (char *)path;
    tail[argc] = NULL;
    ok = agent_docker_exec(w, w->cfg, (const char *const *)tail,
                           data ? data : "", len, false,
                           "docker write", &out, NULL);
    if (!ok) {
        snprintf(err, err_len, "%s",
                 out.ptr && out.ptr[0] ? out.ptr : "docker write failed");
        free(out.ptr);
        return false;
    }
    free(out.ptr);
    return true;
}

/* What: create a directory tree inside the Docker sandbox using `mkdir -p`.
 * Why: the mkdir tool needs Docker filesystem support; this follows the
 * same pattern as agent_docker_write_file_bytes but simpler (no stdin pipe).
 * Callers: agent_tool_mkdir(). */
static bool agent_docker_mkdir_p(agent_worker *w, const char *path,
                                  char *err, size_t err_len) {
    if (!path || !path[0]) {
        snprintf(err, err_len, "missing path");
        return false;
    }
    char *argv[] = {"mkdir", "-p", (char *)path, NULL};
    agent_buf out = {0};
    agent_buf err_buf = {0};
    int status = 0;
    bool ok = agent_docker_shell_exec_argv_stderr(w, argv, false,
                                                  "docker mkdir", &out,
                                                  &err_buf, &status);
    free(out.ptr);
    if (!ok || status != 0) {
        snprintf(err, err_len, "%s",
                 err_buf.ptr && err_buf.ptr[0] ? err_buf.ptr :
                 "mkdir failed in container");
        free(err_buf.ptr);
        return false;
    }
    free(err_buf.ptr);
    return true;
}

static void agent_bash_exec_local(const char *cmd, const char *working_dir,
                                  const char *temp_dir) {
    if (working_dir && chdir(working_dir) != 0) _exit(126);
    agent_bash_prepare_env(working_dir, temp_dir);
    execl("/bin/sh", "sh", "-c", cmd ? cmd : "", (char *)NULL);
    _exit(127);
}

/* What: child-process exec path for running a bash tool command inside Docker.
 * Why: long-running bash jobs need their own process group and output file, so
 * they use a direct `docker exec` instead of the synchronous persistent shell.
 * Callers: the child branch of agent_bash_start_mode(). */
static void agent_bash_exec_docker(agent_worker *w, const char *cmd,
                                   const char *working_dir) {
    if (!w || !w->cfg || !agent_bash_use_docker_sandbox(w)) {
        dprintf(STDERR_FILENO, "docker sandbox is not active\n");
        _exit(127);
    }

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    const char *temp_dir = w->cfg->temp_directory[0] ?
        w->cfg->temp_directory : NULL;
    char home_env[PATH_MAX + 16];
    char tmpdir_env[PATH_MAX + 16];
    char tmp_env[PATH_MAX + 16];
    char temp_env[PATH_MAX + 16];
    char xdg_config_env[PATH_MAX + 32];
    char xdg_cache_env[PATH_MAX + 32];
    char *argv[48];
    int argc = 0;
    agent_docker_build_shell_argv(argv, &argc, docker_command,
                                  w->cfg->docker_container,
                                  working_dir, temp_dir, cmd,
                                  home_env, sizeof(home_env),
                                  tmpdir_env, sizeof(tmpdir_env),
                                  tmp_env, sizeof(tmp_env),
                                  temp_env, sizeof(temp_env),
                                  xdg_config_env, sizeof(xdg_config_env),
                                  xdg_cache_env, sizeof(xdg_cache_env));
    (void)argc;
    agent_exec_command(docker_command, argv);
    dprintf(STDERR_FILENO, "docker bash failed to exec docker: %s\n",
            strerror(errno));
    _exit(127);
}

/* Spawn a shell command into its own process group so bash_stop/timeout can
 * kill grandchildren created by the shell, not just the /bin/sh wrapper. */
static agent_bash_job *agent_bash_start_mode(agent_worker *w, const char *cmd,
                                             int timeout_sec, bool use_docker,
                                             char *err, size_t err_len) {
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "%s/ds4_agent_output_XXXXXX",
             w->cfg->temp_directory);
    int tmpfd = mkstemp(tmp_path);
    if (tmpfd < 0) {
        snprintf(err, err_len, "failed to create temporary output file: %s", strerror(errno));
        return NULL;
    }

    const char *working_dir = agent_primary_working_directory(w);
    if (use_docker && w && w->cfg && w->cfg->docker_debug) {
        const char *docker_command =
            (w->cfg->docker_command && w->cfg->docker_command[0]) ?
            w->cfg->docker_command : "docker";
        const char *temp_dir = w->cfg->temp_directory[0] ?
            w->cfg->temp_directory : NULL;
        char home_env[PATH_MAX + 16];
        char tmpdir_env[PATH_MAX + 16];
        char tmp_env[PATH_MAX + 16];
        char temp_env[PATH_MAX + 16];
        char xdg_config_env[PATH_MAX + 32];
        char xdg_cache_env[PATH_MAX + 32];
        char *argv[48];
        int argc = 0;
        agent_docker_build_shell_argv(argv, &argc, docker_command,
                                      w->cfg->docker_container,
                                      working_dir, temp_dir, cmd,
                                      home_env, sizeof(home_env),
                                      tmpdir_env, sizeof(tmpdir_env),
                                      tmp_env, sizeof(tmp_env),
                                      temp_env, sizeof(temp_env),
                                      xdg_config_env, sizeof(xdg_config_env),
                                      xdg_cache_env, sizeof(xdg_cache_env));
        (void)argc;
        char *cmd_text = agent_docker_debug_command_text(argv);
        if (cmd_text) {
            const char *prefix = "\x1b[96m[docker debug] ";
            const char *suffix = "\x1b[0m\n\n";
            agent_publish(w, prefix, strlen(prefix));
            agent_publish(w, cmd_text, strlen(cmd_text));
            agent_publish(w, suffix, strlen(suffix));
            free(cmd_text);
        }
    }

    int pipefd[2];
    if (pipe(pipefd) != 0) {
        snprintf(err, err_len, "failed to create pipe: %s", strerror(errno));
        close(tmpfd);
        unlink(tmp_path);
        return NULL;
    }
    pid_t pid = fork();
    if (pid < 0) {
        snprintf(err, err_len, "failed to fork: %s", strerror(errno));
        close(pipefd[0]);
        close(pipefd[1]);
        close(tmpfd);
        unlink(tmp_path);
        return NULL;
    }
    if (pid == 0) {
        setpgid(0, 0);
        close(tmpfd);
        /* The bash tool is not interactive.  Give the shell /dev/null as
         * stdin so it does not inherit the live linenoise terminal and reset
         * it from raw mode to cooked mode behind the agent's back. */
        int null_fd = open("/dev/null", O_RDONLY);
        if (null_fd >= 0) {
            if (dup2(null_fd, STDIN_FILENO) < 0)
                close(STDIN_FILENO);
            if (null_fd != STDIN_FILENO)
                close(null_fd);
        } else {
            close(STDIN_FILENO);
        }
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        if (use_docker) agent_bash_exec_docker(w, cmd, working_dir);
        else agent_bash_exec_local(cmd, working_dir, w->cfg->temp_directory);
    }
    close(pipefd[1]);
    setpgid(pid, pid);
    int old_flags;
    set_nonblock(pipefd[0], true, &old_flags);

    agent_bash_job *job = xmalloc(sizeof(*job));
    memset(job, 0, sizeof(*job));
    if (w->next_bash_job_id <= 0) w->next_bash_job_id = 1;
    job->id = w->next_bash_job_id++;
    job->pid = pid;
    job->pipe_fd = pipefd[0];
    job->tmp_fd = tmpfd;
    snprintf(job->path, sizeof(job->path), "%s", tmp_path);
    /* Allow the agent to read bash output files and clean them up later. */
    agent_temp_files_track(w, tmp_path);
    job->cmd = xstrdup(cmd);
    job->start_time = now_sec();
    job->timeout_sec = timeout_sec;
    job->exit_status = -1;
    job->running = true;
    job->worker = w;
    job->next = w->bash_jobs;
    w->bash_jobs = job;
    return job;
}

static agent_bash_job *agent_bash_start(agent_worker *w, const char *cmd,
                                        int timeout_sec, char *err, size_t err_len) {
    return agent_bash_start_mode(w, cmd, timeout_sec,
                                 agent_bash_use_docker_sandbox(w),
                                 err, err_len);
}

static void agent_tail_append(agent_buf *b, const char *s, size_t n, size_t max) {
    if (!n) return;
    agent_buf_append(b, s, n);
    if (b->len > max) {
        size_t drop = b->len - max;
        memmove(b->ptr, b->ptr + drop, b->len - drop + 1);
        b->len -= drop;
    }
}

/* Read the first max_lines from the output file, with a byte cap to avoid a
 * pathological single long line flooding the next model turn. */
static char *agent_bash_read_head(const agent_bash_job *job, int max_lines,
                                  size_t max_bytes, int *lines_read,
                                  bool *byte_limited) {
    if (lines_read) *lines_read = 0;
    if (byte_limited) *byte_limited = false;
    if (!job || !job->path[0] || job->bytes == 0) return xstrdup("");
    FILE *fp = fopen(job->path, "rb");
    if (!fp) return xstrdup("<failed to reopen output file>\n");
    agent_temp_files_note_read(job->worker, job->path);

    agent_buf out = {0};
    int lines = 0;
    while (lines < max_lines && out.len < max_bytes) {
        int c = fgetc(fp);
        if (c == EOF) {
            if (ferror(fp) && errno == EINTR) {
                clearerr(fp);
                continue;
            }
            break;
        }
        char ch = (char)c;
        agent_buf_append(&out, &ch, 1);
        if (ch == '\n') lines++;
    }
    if (out.len >= max_bytes && !feof(fp) && byte_limited) *byte_limited = true;
    fclose(fp);
    if (lines_read) *lines_read = lines + (out.len && out.ptr[out.len - 1] != '\n');
    if (!out.ptr) return xstrdup("");
    return agent_buf_take(&out);
}

/* Read the last max_lines from the full output file.  The model-visible label
 * says "tail -N <file>" so it is clear this is not the complete output. */
static char *agent_bash_read_tail_lines(const agent_bash_job *job, int max_lines) {
    if (!job || !job->path[0] || job->bytes == 0) return xstrdup("");
    FILE *fp = fopen(job->path, "rb");
    if (!fp) return xstrdup("<failed to reopen output file>\n");
    agent_temp_files_note_read(job->worker, job->path);

    agent_buf tail = {0};
    char tmp[2048];
    for (;;) {
        size_t n = fread(tmp, 1, sizeof(tmp), fp);
        if (n) agent_tail_append(&tail, tmp, n, AGENT_BASH_TAIL_BYTES);
        if (n < sizeof(tmp)) {
            if (ferror(fp) && errno == EINTR) {
                clearerr(fp);
                continue;
            }
            break;
        }
    }
    fclose(fp);
    if (!tail.ptr) return xstrdup("");

    char *start = tail.ptr;
    int newlines = 0;
    for (char *p = tail.ptr + tail.len; p > tail.ptr; p--) {
        if (p[-1] == '\n' && ++newlines > max_lines) {
            start = p;
            break;
        }
    }
    char *out = xstrdup(start);
    free(tail.ptr);
    return out;
}

/* Build the tool result for a bash job.  mark_observed advances the per-job
 * cursor so the next status reports only fresh output. */
static char *agent_bash_observation(agent_bash_job *job, bool mark_observed) {
    agent_bash_poll(job);
    bool first_observation = !job->observed_once;
    int display_lines = agent_bash_display_lines(job);
    double elapsed = now_sec() - job->start_time;

    agent_buf out = {0};
    char line[PATH_MAX + 256];
    if (job->running) {
        snprintf(line, sizeof(line),
            "bash job=%d pid=%ld status=running elapsed_sec=%.1f timeout_sec=%.0f\n",
            job->id, (long)job->pid, elapsed, job->timeout_sec);
    } else {
        snprintf(line, sizeof(line),
            "bash job=%d pid=%ld status=done elapsed_sec=%.1f timed_out=%d\n",
            job->id, (long)job->pid, elapsed, job->timed_out ? 1 : 0);
    }
    agent_buf_puts(&out, line);
    if (!job->running) {
        snprintf(line, sizeof(line), "exit_status=%d\n", job->exit_status);
        agent_buf_puts(&out, line);
    }

    if (job->bytes == 0) {
        agent_buf_puts(&out, "<output>\n</output>\n");
    } else if (first_observation) {
        int shown_lines = 0;
        bool byte_limited = false;
        char *head = agent_bash_read_head(job, AGENT_BASH_HEAD_LINES,
                                          AGENT_BASH_HEAD_BYTES,
                                          &shown_lines, &byte_limited);
        bool truncated = byte_limited || display_lines > shown_lines;
        if (!job->running && !truncated) {
            agent_buf_puts(&out, "<output>\n");
            agent_buf_puts(&out, head);
            if (head[0] && head[strlen(head) - 1] != '\n') agent_buf_puts(&out, "\n");
            agent_buf_puts(&out, "</output>\n");
        } else {
            snprintf(line, sizeof(line),
                     "output_path=%s (%zu bytes, %d lines)\n",
                     job->path[0] ? job->path : "<unavailable>",
                     job->bytes, display_lines);
            agent_buf_puts(&out, line);
            snprintf(line, sizeof(line), "<head -%d %s>\n",
                     AGENT_BASH_HEAD_LINES, job->path);
            agent_buf_puts(&out, line);
            agent_buf_puts(&out, head);
            if (head[0] && head[strlen(head) - 1] != '\n') agent_buf_puts(&out, "\n");
            agent_buf_puts(&out, "</head>\n");
        }
        free(head);
    } else {
        int tail_lines = job->running ? AGENT_BASH_PROGRESS_TAIL_LINES :
                                        AGENT_BASH_FINAL_TAIL_LINES;
        char *tail = agent_bash_read_tail_lines(job, tail_lines);
        snprintf(line, sizeof(line),
                 "output_path=%s (%zu bytes, %d lines)\n",
                 job->path[0] ? job->path : "<unavailable>",
                 job->bytes, display_lines);
        agent_buf_puts(&out, line);
        snprintf(line, sizeof(line), "<tail -%d %s>\n", tail_lines, job->path);
        agent_buf_puts(&out, line);
        agent_buf_puts(&out, tail);
        if (tail[0] && tail[strlen(tail) - 1] != '\n') agent_buf_puts(&out, "\n");
        snprintf(line, sizeof(line), "</tail>\n");
        agent_buf_puts(&out, line);
        free(tail);
    }
    if (job->running) {
        snprintf(line, sizeof(line),
            "\nUse bash_status job=%d to get info before refresh time; use bash_stop job=%d to stop execution\n",
            job->id, job->id);
        agent_buf_puts(&out, line);
    }

    if (mark_observed) {
        job->observed_bytes = job->bytes;
        job->observed_display_lines = display_lines;
        job->observed_once = true;
    }
    return agent_buf_take(&out);
}

static void agent_bash_publish_observation(agent_worker *w, const char *obs) {
    if (w && w->cfg && !w->cfg->command_output) return;
    if (!obs || !obs[0]) return;
    const char *body = NULL;
    const char *label = strstr(obs, "\n<head ");
    const char *close = NULL;
    if (label) {
        close = "</head>";
    } else {
        label = strstr(obs, "\n<tail ");
        if (label) close = "</tail>";
    }
    if (label) {
        const char *tag_end = strstr(label, ">\n");
        if (tag_end) {
            body = tag_end + 2;
        }
    } else {
        label = strstr(obs, "\n<output>\n");
        if (label) {
            body = label + strlen("\n<output>\n");
            close = "</output>";
        }
    }
    if (!body || !body[0]) return;
    const char *end = close ? strstr(body, close) : NULL;
    size_t n = end ? (size_t)(end - body) : strlen(body);
    if (n == 0) {
        const char *done = strstr(obs, "status=done");
        const char *exitp = strstr(obs, "\nexit_status=");
        if (done && exitp) {
            int exit_status = atoi(exitp + strlen("\nexit_status="));
            char msg[96];
            if (exit_status == 0) {
                snprintf(msg, sizeof(msg), "[bash completed with no output]\n");
                agent_publish(w, "\x1b[90m", 5);
                agent_publish(w, msg, strlen(msg));
                agent_publish(w, "\x1b[0m", 4);
            } else {
                snprintf(msg, sizeof(msg),
                         "[bash failed with no output, exit_status=%d]\n",
                         exit_status);
                agent_publish(w, "\x1b[38;5;208m", 11);
                agent_publish(w, msg, strlen(msg));
                agent_publish(w, "\x1b[0m", 4);
            }
        }
        return;
    }
}

static void agent_bash_refresh_for(agent_worker *w, agent_bash_job *job,
                                   int refresh_sec) {
    double start = now_sec();
    while (job->running && now_sec() - start < refresh_sec) {
        if (worker_should_interrupt(w)) break;
        agent_bash_poll(job);
        if (!job->running) break;
        struct pollfd pfd = {.fd = job->pipe_fd, .events = POLLIN};
        poll(&pfd, 1, 100);
    }
    agent_bash_poll(job);
}

/* Common implementation for bash, bash_status, and bash_stop. */
static char *agent_bash_job_tool_result(agent_worker *w, agent_bash_job *job,
                                        bool wait, int refresh_sec,
                                        bool stop, bool remove_if_done) {
    if (stop && job->running) {
        kill(-job->pid, SIGTERM);
        kill(job->pid, SIGTERM);
        double start = now_sec();
        while (job->running && now_sec() - start < 1.0) {
            agent_bash_poll(job);
            if (!job->running) break;
            usleep(20000);
        }
        if (job->running) {
            kill(-job->pid, SIGKILL);
            kill(job->pid, SIGKILL);
        }
    }
    if (wait || stop) agent_bash_refresh_for(w, job, refresh_sec);
    else agent_bash_poll(job);

    char *obs = agent_bash_observation(job, true);
    agent_bash_publish_observation(w, obs);
    if (remove_if_done && !job->running) agent_bash_remove_job(w, job);
    return obs;
}

static int agent_tool_job_id(const agent_tool_call *call) {
    return agent_parse_int_default(agent_tool_arg_value(call, "job"), 0, 0, INT_MAX);
}

static pid_t agent_tool_pid(const agent_tool_call *call) {
    return (pid_t)agent_parse_int_default(agent_tool_arg_value(call, "pid"), 0, 0, INT_MAX);
}

static char *agent_trimmed_dup(const char *s, size_t len) {
    while (len > 0 && isspace((unsigned char)*s)) {
        s++;
        len--;
    }
    while (len > 0 && isspace((unsigned char)s[len - 1])) len--;
    return xstrndup(s, len);
}

static void agent_question_choices_push(char ***items,
                                        int *len,
                                        int *cap,
                                        const char *start,
                                        size_t n) {
    if (!items || !len || !cap || !start) return;
    char *choice = agent_trimmed_dup(start, n);
    if (!choice[0]) {
        free(choice);
        return;
    }
    if (*len == AGENT_ASK_QUESTION_MAX_CHOICES) {
        free(choice);
        return;
    }
    if (*len == *cap) {
        *cap = *cap ? *cap * 2 : 4;
        *items = xrealloc(*items, (size_t)*cap * sizeof((*items)[0]));
    }
    (*items)[(*len)++] = choice;
}

static bool agent_parse_question_choices_text(const char *text,
                                              char ***out_v,
                                              int *out_count) {
    if (out_v) *out_v = NULL;
    if (out_count) *out_count = 0;
    if (!text || !text[0]) return true;

    char **items = NULL;
    int len = 0;
    int cap = 0;
    const char *sep = strchr(text, '\n') ? "\n" : ",";
    char *buf = xstrdup(text);
    char *save = NULL;
    for (char *tok = strtok_r(buf, sep, &save); tok;
         tok = strtok_r(NULL, sep, &save))
    {
        agent_question_choices_push(&items, &len, &cap, tok, strlen(tok));
    }
    free(buf);
    if (out_v) *out_v = items;
    else agent_string_array_free(items, len);
    if (out_count) *out_count = len;
    return true;
}

static bool agent_parse_question_choices_array(const char *text,
                                               char ***out_v,
                                               int *out_count) {
    if (out_v) *out_v = NULL;
    if (out_count) *out_count = 0;
    if (!text) return false;

    const unsigned char *p = (const unsigned char *)text;
    while (isspace(*p)) p++;
    if (*p != '[') return false;
    p++;

    char **items = NULL;
    int len = 0;
    int cap = 0;
    for (;;) {
        while (isspace(*p)) p++;
        if (*p == ']') {
            p++;
            break;
        }
        if (*p != '"') {
            agent_string_array_free(items, len);
            return false;
        }
        p++;
        agent_buf item = {0};
        while (*p && *p != '"') {
            if (*p == '\\') {
                p++;
                if (!*p) {
                    agent_string_array_free(items, len);
                    free(item.ptr);
                    return false;
                }
                char c = (char)*p;
                switch (c) {
                    case '"': case '\\': case '/':
                        agent_buf_append(&item, &c, 1);
                        break;
                    case 'b': c = '\b'; agent_buf_append(&item, &c, 1); break;
                    case 'f': c = '\f'; agent_buf_append(&item, &c, 1); break;
                    case 'n': c = '\n'; agent_buf_append(&item, &c, 1); break;
                    case 'r': c = '\r'; agent_buf_append(&item, &c, 1); break;
                    case 't': c = '\t'; agent_buf_append(&item, &c, 1); break;
                    default:
                        /* Model-emitted choices are DSML text, not strict JSON.
                         * Preserve unknown backslash escapes like \@ literally. */
                        agent_buf_append(&item, "\\", 1);
                        agent_buf_append(&item, &c, 1);
                        break;
                }
                p++;
                continue;
            }
            char c = (char)*p++;
            agent_buf_append(&item, &c, 1);
        }
        if (*p != '"') {
            agent_string_array_free(items, len);
            free(item.ptr);
            return false;
        }
        p++;
        char *choice = agent_buf_take(&item);
        agent_question_choices_push(&items, &len, &cap, choice, strlen(choice));
        free(choice);
        while (isspace(*p)) p++;
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        agent_string_array_free(items, len);
        return false;
    }

    while (isspace(*p)) p++;
    if (*p) {
        agent_string_array_free(items, len);
        return false;
    }
    if (out_v) *out_v = items;
    else agent_string_array_free(items, len);
    if (out_count) *out_count = len;
    return true;
}

static bool agent_parse_question_choices(const char *text,
                                         char ***out_v,
                                         int *out_count) {
    if (out_v) *out_v = NULL;
    if (out_count) *out_count = 0;
    if (!text || !text[0]) return true;

    char **items = NULL;
    int count = 0;
    if (agent_parse_question_choices_array(text, &items, &count)) {
        if (out_v) *out_v = items;
        else agent_string_array_free(items, count);
        if (out_count) *out_count = count;
        return true;
    }

    return agent_parse_question_choices_text(text, out_v, out_count);
}

static char *agent_tool_ask_question(agent_worker *w,
                                     const agent_tool_call *call) {
    const char *question = agent_tool_arg_value(call, "question");
    if (!question || !question[0])
        return xstrdup("Tool error: ask_question requires question\n");

    char **choices = NULL;
    int choice_count = 0;
    if (!agent_parse_question_choices(agent_tool_arg_value(call, "choices"),
                                      &choices, &choice_count))
    {
        return xstrdup("Tool error: ask_question choices must be strings\n");
    }
    char *result = worker_request_question(w, question, choices, choice_count);
    agent_string_array_free(choices, choice_count);
    return result;
}

/* --- skill_list tool handler --- */
static void agent_json_escape_n(agent_buf *out, const char *text,
                                size_t text_len) {
    static const char hex[] = "0123456789abcdef";
    if (!out || !text || !text_len) return;
    for (size_t i = 0; i < text_len; i++) {
        unsigned char c = text[i];
        switch (c) {
        case '"': agent_buf_puts(out, "\\\""); break;
        case '\\': agent_buf_puts(out, "\\\\"); break;
        case '\b': agent_buf_puts(out, "\\b"); break;
        case '\f': agent_buf_puts(out, "\\f"); break;
        case '\n': agent_buf_puts(out, "\\n"); break;
        case '\r': agent_buf_puts(out, "\\r"); break;
        case '\t': agent_buf_puts(out, "\\t"); break;
        default:
            if (c < 0x20) {
                char escaped[] = "\\u0000";
                escaped[4] = hex[c >> 4];
                escaped[5] = hex[c & 0x0f];
                agent_buf_append_full(out, escaped, 6);
            } else {
                agent_buf_append_full(out, (const char *)&text[i], 1);
            }
            break;
        }
    }
}

static void agent_json_escape(agent_buf *out, const char *text) {
    if (!text) return;
    agent_json_escape_n(out, text, strlen(text));
}

static char *agent_tool_skill_list(agent_worker *w,
                                   const agent_tool_call *call) {
    agent_buf result = {0};
    const char *query = agent_tool_arg_value(call, "query");

    /* Collect matching skills, up to 10 */
    struct { char name[AGENT_SKILL_NAME_MAX]; char desc[AGENT_SKILL_DESC_MAX]; char path[AGENT_SKILL_PATH_MAX]; } matches[10];
    int nmatches = 0;
    int total = 0;

    agent_skill_registry *registry = w->skill_registry;
    if (!registry) return xstrdup("{\"skills\":[],\"total_count\":0,\"has_more\":false}\n");
    pthread_mutex_lock(&registry->mu);
    for (agent_skill *s = registry->head; s; s = s->next) {
        bool match = true;
        if (query && query[0]) {
            /* Case-insensitive substring match on name */
            match = false;
            const char *sname = s->name;
            for (const char *sp = sname; *sp; sp++) {
                const char *p = sp;
                const char *q = query;
                while (*p && *q) {
                    char pc = *p >= 'A' && *p <= 'Z' ? *p + 32 : *p;
                    char qc = *q >= 'A' && *q <= 'Z' ? *q + 32 : *q;
                    if (pc == qc) { p++; q++; }
                    else break;
                }
                if (!*q) { match = true; break; }
            }
        }
        if (match) {
            total++;
            if (nmatches < 10) {
                strncpy(matches[nmatches].name, s->name, sizeof(matches[nmatches].name) - 1);
                matches[nmatches].name[sizeof(matches[nmatches].name) - 1] = '\0';
                strncpy(matches[nmatches].desc, s->description, sizeof(matches[nmatches].desc) - 1);
                matches[nmatches].desc[sizeof(matches[nmatches].desc) - 1] = '\0';
                strncpy(matches[nmatches].path, s->path, sizeof(matches[nmatches].path) - 1);
                matches[nmatches].path[sizeof(matches[nmatches].path) - 1] = '\0';
                nmatches++;
            }
        }
    }
    pthread_mutex_unlock(&registry->mu);

    /* Build JSON response */
    agent_buf_puts(&result, "{\n");
    agent_buf_puts(&result, "  \"skills\": [\n");
    for (int i = 0; i < nmatches; i++) {
        if (i > 0) agent_buf_puts(&result, ",\n");
        agent_buf_puts(&result, "    {\n");
        agent_buf_puts(&result, "      \"name\": \"");
        agent_json_escape(&result, matches[i].name);
        agent_buf_puts(&result, "\",\n");
        agent_buf_puts(&result, "      \"description\": \"");
        agent_json_escape(&result, matches[i].desc);
        agent_buf_puts(&result, "\",\n");
        agent_buf_puts(&result, "      \"path\": \"");
        agent_json_escape(&result, matches[i].path);
        agent_buf_puts(&result, "\"\n");
        agent_buf_puts(&result, "    }");
    }
    agent_buf_puts(&result, "\n  ],\n");
    agent_buf_puts(&result, "  \"total_count\": ");
    char cnt[32];
    snprintf(cnt, sizeof(cnt), "%d", total);
    agent_buf_puts(&result, cnt);
    agent_buf_puts(&result, ",\n");
    agent_buf_puts(&result, "  \"has_more\": ");
    agent_buf_puts(&result, nmatches < total ? "true" : "false");
    agent_buf_puts(&result, "\n}\n");
    return agent_buf_take(&result);
}

/* ============================================================================
 * Tool Dispatch
 * ============================================================================
 */

static char *agent_execute_tool_call(agent_worker *w, const agent_tool_call *call) {
    agent_buf result = {0};
    if (!call->name) return xstrdup("Tool error: missing tool name\n");
    if (!strcmp(call->name, "ask_question"))
        return agent_tool_ask_question(w, call);
    if (!strcmp(call->name, "skill_list"))
        return agent_tool_skill_list(w, call);
    if (agent_tool_index_for_name(call->name) < 0) {
        char header[256];
        snprintf(header, sizeof(header), "\n[tool:%s] unknown tool\n", call->name);
        agent_publish(w, header, strlen(header));
        agent_buf_puts(&result, "Tool error: unknown tool: ");
        agent_buf_puts(&result, call->name);
        agent_buf_puts(&result, "\n");
        agent_append_available_tools(&result, &w->tool_policy);
        return agent_buf_take(&result);
    }
    if (!ds4_agent_tool_policy_allows(&w->tool_policy, call->name)) {
        char summary[256];
        ds4_agent_tool_policy_format(&w->tool_policy, summary, sizeof(summary));
        char header[512];
        snprintf(header, sizeof(header),
                 "\n[tool:%s] blocked by tool-access policy (%s)\n",
                 call->name, summary);
        agent_publish(w, header, strlen(header));
        agent_buf_puts(&result, "Tool error: ");
        agent_buf_puts(&result, call->name);
        agent_buf_puts(&result, " is not allowed by current tool-access policy (");
        agent_buf_puts(&result, summary);
        agent_buf_puts(&result, ")\n");
        return agent_buf_take(&result);
    }

    if (agent_tool_requires_docker_sandbox(w, call->name) &&
        !agent_bash_use_docker_sandbox(w))
        return agent_tool_sandbox_required_error();

    if (!strcmp(call->name, "read")) return agent_tool_read(w, call);
    if (!strcmp(call->name, "more")) return agent_tool_more(w, call);
    if (!strcmp(call->name, "write")) return agent_tool_write(w, call);
    if (!strcmp(call->name, "list")) return agent_tool_list(w, call);
    if (!strcmp(call->name, "edit")) return agent_tool_edit(w, call);
    if (!strcmp(call->name, "search")) return agent_tool_search(w, call);
    if (!strcmp(call->name, "web_browse")) return agent_tool_web_browse(w, call);
    if (!strcmp(call->name, "web_fetch")) return agent_tool_web_fetch(w, call);
    if (!strcmp(call->name, "mkdir")) return agent_tool_mkdir(w, call);

    if (!strcmp(call->name, "bash")) {
        const char *cmd = agent_tool_arg_value(call, "command");
        if (!cmd || !cmd[0]) return xstrdup("Tool error: bash requires command\n");
        int timeout = agent_parse_timeout(agent_tool_arg_value(call, "timeout_sec"));
        int refresh = agent_parse_int_default(agent_tool_arg_value(call, "refresh_sec"),
                                              60, 1, 3600);
        char err[160] = {0};
        agent_bash_job *job = agent_bash_start(w, cmd, timeout, err, sizeof(err));
        if (!job) {
            agent_buf_puts(&result, "Tool error: bash failed to start: ");
            agent_buf_puts(&result, err[0] ? err : "unknown error");
            agent_buf_puts(&result, "\n");
            return agent_buf_take(&result);
        }
        return agent_bash_job_tool_result(w, job, true, refresh, false, true);
    }

    if (!strcmp(call->name, "bash_status") ||
        !strcmp(call->name, "bash_stop"))
    {
        int job_id = agent_tool_job_id(call);
        pid_t pid = agent_tool_pid(call);
        agent_bash_job *job = agent_bash_find_job(w, job_id, pid);
        if (!job) {
            char msg[128];
            snprintf(msg, sizeof(msg), "Tool error: bash job not found: job=%d pid=%ld\n",
                     job_id, (long)pid);
            return xstrdup(msg);
        }
        int refresh = agent_parse_int_default(agent_tool_arg_value(call, "refresh_sec"),
                                              60, 1, 3600);
        bool stop = !strcmp(call->name, "bash_stop");
        bool wait = stop;
        return agent_bash_job_tool_result(w, job, wait, refresh, stop, true);
    }
    return xstrdup("Tool error: internal dispatcher mismatch\n");
}

/* Execute all tool calls from one DSML block, preserving per-call labels in the
 * combined result so the model can associate observations with calls. */
static char *agent_execute_tool_calls_ordered(agent_worker *w,
                                              const agent_tool_calls *calls,
                                              bool stop_on_interrupt,
                                              bool *interrupted) {
    agent_buf all = {0};
    if (interrupted) *interrupted = false;
    for (int i = 0; i < calls->len; i++) {
        if (stop_on_interrupt && worker_should_interrupt(w)) {
            agent_buf_puts(&all,
                "Tool result interrupted:\nTool error: interrupted by user\n");
            if (interrupted) *interrupted = true;
            break;
        }
        char *res = agent_execute_tool_call(w, &calls->v[i]);
        char hdr[128];
        snprintf(hdr, sizeof(hdr), "Tool result %d (%s):\n", i + 1,
                 calls->v[i].name ? calls->v[i].name : "unknown");
        agent_buf_puts(&all, hdr);
        agent_buf_puts(&all, res);
        if (res[0] && res[strlen(res) - 1] != '\n') agent_buf_puts(&all, "\n");
        free(res);
        if (stop_on_interrupt && worker_should_interrupt(w)) {
            agent_buf_puts(&all,
                "Tool result interrupted:\nTool error: interrupted by user\n");
            if (interrupted) *interrupted = true;
            break;
        }
    }
    if (calls->len == 0) agent_buf_puts(&all, "Tool error: empty tool call block\n");
    return agent_buf_take(&all);
}

static char *agent_execute_tool_calls(agent_worker *w,
                                      const agent_tool_calls *calls) {
    return agent_execute_tool_calls_ordered(w, calls, false, NULL);
}

/* If compaction happens while a bash process is still alive, inject a small
 * tool-role reminder into the rebuilt transcript.  Otherwise the summary could
 * preserve the user's task but lose the fact that an external process still
 * needs status/wait/stop handling. */
static char *agent_bash_jobs_compaction_observation(agent_worker *w) {
    if (!w->bash_jobs) return NULL;
    agent_buf out = {0};
    agent_buf_puts(&out,
        "Bash job update after context compaction. Running jobs still need explicit bash_status or bash_stop if relevant.\n");
    for (agent_bash_job *job = w->bash_jobs, *next = NULL; job; job = next) {
        next = job->next;
        char *obs = agent_bash_observation(job, true);
        char hdr[64];
        snprintf(hdr, sizeof(hdr), "\nJob %d:\n", job->id);
        agent_buf_puts(&out, hdr);
        agent_buf_puts(&out, obs);
        free(obs);
        if (!job->running) agent_bash_remove_job(w, job);
    }
    return agent_buf_take(&out);
}

/* ============================================================================
 * Context Compaction
 * ============================================================================
 *
 * Compaction asks the model for durable task state, then rebuilds the live
 * transcript as: system prompt + summary + recent verbatim tail.  This keeps
 * the active KV usable while avoiding unbounded transcript growth.
 */

/* Decide when to compact before an ordinary turn or before appending a large
 * tool result.  The fixed free-token threshold is capped proportionally for
 * smaller contexts so tests with tiny contexts still compact rather than fail. */
static bool agent_worker_should_compact(agent_worker *w) {
    int ctx = w->cfg->gen.ctx_size;
    int used = w->transcript.len;
    if (ctx <= 0 || used <= 0) return false;
    if (used >= (ctx * AGENT_COMPACT_SOFT_PERCENT) / 100) return true;
    int free_threshold = AGENT_COMPACT_MIN_FREE_TOKENS;
    int proportional = ctx / 4;
    if (free_threshold > proportional) free_threshold = proportional;
    return ctx - used <= free_threshold;
}

static int agent_special_token_id(ds4_engine *engine, const char *rendered) {
    ds4_tokens t = {0};
    ds4_tokenize_rendered_chat(engine, rendered, &t);
    int id = t.len == 1 ? t.v[0] : -1;
    ds4_tokens_free(&t);
    return id;
}

/* Pick a recent verbatim tail for the compacted transcript.  Prefer a user
 * boundary inside the budget so the rebuilt context starts at a natural turn. */
static int agent_compact_tail_start(agent_worker *w, int bottom, int sys_len) {
    int tail_budget = w->cfg->gen.ctx_size / AGENT_COMPACT_TAIL_DIVISOR;
    if (tail_budget > AGENT_COMPACT_TAIL_CAP_TOKENS)
        tail_budget = AGENT_COMPACT_TAIL_CAP_TOKENS;
    if (tail_budget < 1) tail_budget = 1;

    int target = bottom - tail_budget;
    if (target < sys_len) target = sys_len;

    int user_id = agent_special_token_id(w->engine, "<｜User｜>");
    if (user_id < 0) return target;

    for (int i = target; i < bottom; i++) {
        if (w->transcript.v[i] == user_id) return i;
    }
    return target;
}

static void agent_tokens_append_range(ds4_tokens *dst, const ds4_tokens *src,
                                      int start, int end) {
    if (start < 0) start = 0;
    if (end > src->len) end = src->len;
    for (int i = start; i < end; i++) ds4_tokens_push(dst, src->v[i]);
}

/* Build the private prompt used to ask the model for durable state.  The prompt
 * explicitly forbids tool calls because the result is consumed internally, not
 * delivered as an assistant turn. */
static char *agent_compact_make_prompt(const char *reason) {
    agent_buf b = {0};
    agent_buf_puts(&b,
        "Internal ds4-agent context compaction request. This is not a user request.\n"
        "Write a durable task-state summary of the conversation so far. Preserve only facts that matter for continuing the work:\n"
        "- user goals, constraints, and preferences\n"
        "- files inspected or edited\n"
        "- commands run and important results\n"
        "- decisions, rejected approaches, known bugs, and pending next steps\n"
        "- reloadable bulky data with exact paths/ranges/commands when available\n\n"
        "Do not invent facts. Do not include generic narration. Do not include raw file contents unless they were essential to a conclusion.\n"
        "After the summary, stop. Do not continue the user task, do not call tools, and do not output thinking tags or DSML markup.\n"
        "Output only the compact summary.\n");
    if (reason && reason[0]) {
        agent_buf_puts(&b, "\nCompaction reason: ");
        agent_buf_puts(&b, reason);
        agent_buf_puts(&b, "\n");
    }
    return agent_buf_take(&b);
}

/* Perform the full compaction exchange and rebuild the live DS4 session from
 * the compacted transcript.  Any failure invalidates live KV because the model
 * may have just seen private compaction instructions that are not part of the
 * real conversation. */
static bool agent_worker_compact(agent_worker *w, const char *reason,
                                 char *err, size_t err_len) {
    const int bottom = w->transcript.len;
    if (bottom <= 0) return true;

    ds4_tokens sys = {0};
    agent_worker_build_system_tokens(w, &sys);
    if (bottom <= sys.len) {
        ds4_tokens_free(&sys);
        return true;
    }

    agent_publishf(w,
        "\n\x1b[1;95mCOMPACTING\x1b[0m %s: summarizing durable task state\n\x1b[38;5;245m",
        reason && reason[0] ? reason : "context");

    char *prompt_text = agent_compact_make_prompt(reason);
    ds4_tokens prompt = {0};
    ds4_tokens_copy(&prompt, &w->transcript);
    ds4_chat_append_message(w->engine, &prompt, "user", prompt_text);
    free(prompt_text);
    ds4_chat_append_assistant_prefix(w->engine, &prompt, DS4_THINK_NONE);

    pthread_mutex_lock(&w->mu);
    w->status.state = AGENT_WORKER_COMPACTING;
    w->progress_started_at = now_sec();
    w->status.prefill_done = 0;
    w->status.prefill_total = 0;
    w->status.prefill_tps = 0.0;
    w->status.generated = 0;
    w->status.gen_tps = 0.0;
    w->status.greedy_sampling = false;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);

    int summary_room = w->cfg->gen.ctx_size - prompt.len - 1;
    if (summary_room < 256) {
        snprintf(err, err_len, "not enough context left to request compaction summary");
        ds4_tokens_free(&prompt);
        ds4_tokens_free(&sys);
        agent_publish(w, "\x1b[0m\n", 5);
        return false;
    }
    int summary_max = summary_room < AGENT_COMPACT_SUMMARY_MAX_TOKENS ?
                      summary_room : AGENT_COMPACT_SUMMARY_MAX_TOKENS;

    ds4_session_set_progress(w->session, worker_progress_cb, w);
    ds4_session_set_display_progress(w->session, worker_progress_cb, w);
    ds4_session_set_cancel(w->session, worker_cancel_session_cb, w);
    int sync_rc = agent_worker_session_sync(w, &prompt,
                                            AGENT_WORKER_COMPACTING,
                                            err, err_len);
    ds4_session_set_cancel(w->session, NULL, NULL);
    ds4_session_set_progress(w->session, NULL, NULL);
    ds4_session_set_display_progress(w->session, NULL, NULL);
    if (sync_rc == DS4_SESSION_SYNC_INTERRUPTED) {
        ds4_session_invalidate(w->session);
        snprintf(err, err_len, "interrupted");
        agent_publish_system_status(
            w, "Compaction interrupted; keeping the previous conversation state.");
        ds4_tokens_free(&prompt);
        ds4_tokens_free(&sys);
        agent_publish(w, "\x1b[0m\n", 5);
        worker_clear_interrupt(w);
        return false;
    }
    if (sync_rc != 0) {
        ds4_session_invalidate(w->session);
        ds4_tokens_free(&prompt);
        ds4_tokens_free(&sys);
        agent_publish(w, "\x1b[0m\n", 5);
        return false;
    }

    /* From here until the final rebuild, the live KV contains the internal
     * compaction prompt/summary, while w->transcript still contains the real
     * conversation.  If anything fails, invalidate live KV so the next turn
     * cannot accidentally continue from the private compaction exchange. */
    agent_buf summary = {0};
    char eval_err[160] = {0};
    int think_end_id = agent_special_token_id(w->engine, "</think>");
    int dsml_id = agent_special_token_id(w->engine, "｜DSML｜");
    double t0 = now_sec();
    for (int i = 0; i < summary_max; i++) {
        if (worker_should_interrupt(w)) {
            snprintf(err, err_len, "interrupted");
            ds4_session_invalidate(w->session);
            ds4_tokens_free(&prompt);
            ds4_tokens_free(&sys);
            free(summary.ptr);
            agent_publish(w, "\x1b[0m\n", 5);
            agent_publish_system_status(
                w, "Compaction interrupted; keeping the previous conversation state.");
            worker_clear_interrupt(w);
            return false;
        }
        int token = ds4_session_argmax(w->session);
        if (token == ds4_token_eos(w->engine)) break;
        if (token == think_end_id || token == dsml_id) {
            if (token == dsml_id && summary.len && summary.ptr[summary.len - 1] == '<') {
                summary.ptr[--summary.len] = '\0';
            }
            agent_trace(w, "compaction summary stopped before control token id=%d", token);
            break;
        }
        if (agent_worker_session_eval(w, token, AGENT_WORKER_COMPACTING,
                                      eval_err, sizeof(eval_err)) != 0) {
            snprintf(err, err_len, "%s", eval_err);
            ds4_session_invalidate(w->session);
            ds4_tokens_free(&prompt);
            ds4_tokens_free(&sys);
            free(summary.ptr);
            agent_publish(w, "\x1b[0m\n", 5);
            return false;
        }

        size_t text_len = 0;
        char *text = ds4_token_text(w->engine, token, &text_len);
        agent_buf_append(&summary, text, text_len);
        agent_publish(w, text, text_len);
        free(text);

        double dt = now_sec() - t0;
        pthread_mutex_lock(&w->mu);
        w->status.generated = i + 1;
        w->status.gen_tps = dt > 0.0 ? (double)(i + 1) / dt : 0.0;
        w->status.greedy_sampling = false;
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);
    }
    agent_publish(w, "\x1b[0m\n", 5);
    ds4_tokens_free(&prompt);

    if (!summary.ptr || !summary.ptr[0]) {
        snprintf(err, err_len, "compaction summary was empty");
        ds4_session_invalidate(w->session);
        ds4_tokens_free(&sys);
        free(summary.ptr);
        return false;
    }

    int tail_start = agent_compact_tail_start(w, bottom, sys.len);
    ds4_tokens compacted = {0};
    ds4_tokens_copy(&compacted, &sys);

    agent_buf summary_msg = {0};
    agent_buf_puts(&summary_msg,
        "\n\n[ds4-agent compacted earlier conversation. Durable task-state summary follows.]\n");
    agent_buf_puts(&summary_msg, summary.ptr);
    if (summary_msg.len && summary_msg.ptr[summary_msg.len - 1] != '\n')
        agent_buf_puts(&summary_msg, "\n");
    agent_buf_puts(&summary_msg, "[End compacted summary. Recent conversation continues verbatim below.]\n\n");
    ds4_chat_append_message(w->engine, &compacted, "system", summary_msg.ptr);
    free(summary_msg.ptr);
    free(summary.ptr);

    agent_tokens_append_range(&compacted, &w->transcript, tail_start, bottom);

    agent_publishf(w,
        "\x1b[1;95mCOMPACTING\x1b[0m rebuilding context: old=%d summary+tail=%d tail=%d\n",
        bottom, compacted.len, bottom - tail_start);

    ds4_tokens old_transcript = {0};
    ds4_tokens_copy(&old_transcript, &w->transcript);
    ds4_tokens_free(&w->transcript);
    w->transcript = compacted;
    if (agent_worker_sync_tokens(w, &w->transcript, true, err, err_len) != 0) {
        ds4_session_invalidate(w->session);
        ds4_tokens_free(&w->transcript);
        w->transcript = old_transcript;
        ds4_tokens_free(&sys);
        return false;
    }
    agent_worker_note_system_prompt_seen(w);
    ds4_tokens_free(&old_transcript);
    ds4_tokens_free(&sys);
    char *bash_update = agent_bash_jobs_compaction_observation(w);
    if (bash_update) {
        ds4_chat_append_message(w->engine, &w->transcript, "tool", bash_update);
        w->session_dirty = true;
        agent_trace_text(w, "tool-after-compaction", bash_update, strlen(bash_update));
        agent_publish(w, "\x1b[90mCOMPACTING added bash job update after rebuild\x1b[0m\n",
                      strlen("\x1b[90mCOMPACTING added bash job update after rebuild\x1b[0m\n"));
        free(bash_update);
    }
    agent_trace(w, "compacted reason=\"%s\" old=%d new=%d tail_start=%d tail=%d",
                reason ? reason : "", bottom, w->transcript.len,
                tail_start, bottom - tail_start);
    return true;
}

static bool agent_worker_compact_if_needed(agent_worker *w, const char *reason,
                                           char *err, size_t err_len) {
    if (!agent_worker_should_compact(w)) return true;
    return agent_worker_compact(w, reason, err, err_len);
}

static int worker_accept_generated_token(agent_worker *w,
                                         int token,
                                         int *generated,
                                         double t0,
                                         agent_stream_renderer *stream,
                                         char *err,
                                         size_t err_len) {
    if (agent_worker_session_eval(w, token, AGENT_WORKER_GENERATING,
                                  err, err_len) != 0)
        return 1;

    ds4_tokens_push(&w->transcript, token);

    size_t text_len = 0;
    char *text = ds4_token_text(w->engine, token, &text_len);
    agent_trace_token(w, token, text, text_len, *generated + 1);
    agent_stream_text(stream, text, text_len, false);
    free(text);
    (*generated)++;

    double dt = now_sec() - t0;
    pthread_mutex_lock(&w->mu);
    w->status.generated = *generated;
    w->status.gen_tps = dt > 0.0 ? (double)*generated / dt : 0.0;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
    return 0;
}

static int worker_force_generated_text(agent_worker *w,
                                       const char *text,
                                       int max_tokens,
                                       int *generated,
                                       double t0,
                                       agent_stream_renderer *stream,
                                       char *err,
                                       size_t err_len) {
    ds4_tokens tokens = {0};
    ds4_tokenize_text(w->engine, text, &tokens);
    if (tokens.len > max_tokens - *generated) {
        snprintf(err, err_len, "not enough generation room to force %s", text);
        ds4_tokens_free(&tokens);
        return 1;
    }
    for (int i = 0; i < tokens.len && *generated < max_tokens; i++) {
        if (worker_accept_generated_token(w, tokens.v[i], generated, t0,
                                          stream, err, err_len) != 0) {
            ds4_tokens_free(&tokens);
            return 1;
        }
    }
    ds4_tokens_free(&tokens);
    return 0;
}

/* ============================================================================
 * Model Worker Thread
 * ============================================================================
 */

/* DSML structure is a machine-readable grammar, so once the model has clearly
 * started a tool stanza we decode grammar bytes greedily.  Parameter values are
 * different: they can be shell commands, code, file contents, or edit bodies,
 * and should keep the configured sampling behavior.  The only exception is a
 * parameter closing tag once it is clearly DSML syntax, not ordinary text such
 * as HTML/XML/code containing "</".
 *
 * This helper is intentionally derived only from the current streaming parser
 * state.  The state object is local to one assistant round, so malformed output,
 * EOS, Ctrl+C, or the next turn cannot accidentally leave sampling greedy. */
static bool agent_stream_wants_greedy_sampling(const agent_stream_renderer *sr) {
    if (!sr || !sr->parser) return false;
    if (sr->parser->state == AGENT_DSML_ERROR ||
        sr->parser->state == AGENT_DSML_DONE)
        return false;

    /* A possible opening marker is being held back by the start detector.  A
     * single '<' is too common in prose/code to justify forcing argmax; after
     * the second byte, the buffered prefix still matching here is specifically
     * DSML-shaped ("<｜..." or the tolerated "<D..." typo). */
    if (sr->dsml_start_len > 1) return true;
    if (!sr->dsml_active) return false;

    if (sr->parser->state == AGENT_DSML_STRUCTURAL)
        return true;
    if (sr->parser->state != AGENT_DSML_PARAM_VALUE)
        return false;

    return sr->parser->param_close_prefix;
}

static int worker_sample_with_mode(agent_worker *w, const agent_config *cfg,
                                   bool greedy, uint64_t *rng) {
    return ds4_session_sample(w->session,
                              greedy ? 0.0f : cfg->gen.temperature,
                              0,
                              greedy ? 1.0f : cfg->gen.top_p,
                              greedy ? 0.0f : cfg->gen.min_p,
                              rng);
}

static void worker_set_greedy_sampling(agent_worker *w, bool greedy) {
    pthread_mutex_lock(&w->mu);
    if (w->status.greedy_sampling != greedy) {
        w->status.greedy_sampling = greedy;
        agent_wake_locked(w);
    }
    pthread_mutex_unlock(&w->mu);
}

static bool agent_skill_name_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '_' || c == '-';
}

static char *agent_expand_skill_references(agent_worker *w, const char *text) {
    if (!text) return xstrdup("");
    if (!w || !w->skill_registry) return xstrdup(text);

    agent_buf out = {0};
    agent_skill_registry *registry = w->skill_registry;
    const char *cursor = text;
    while (*cursor) {
        if (cursor[0] == '\\' && cursor[1] == '@') {
            agent_buf_append_full(&out, "@", 1);
            cursor += 2;
            continue;
        }
        if (*cursor != '@') {
            agent_buf_append_full(&out, cursor++, 1);
            continue;
        }

        const char *name = cursor + 1;
        const char *end = name;
        while (*end && agent_skill_name_char(*end)) end++;
        size_t name_len = (size_t)(end - name);
        char matched_name[AGENT_SKILL_NAME_MAX] = {0};
        char matched_path[AGENT_SKILL_PATH_MAX] = {0};
        size_t best_len = 0;
        pthread_mutex_lock(&registry->mu);
        for (size_t try_len = name_len; try_len > 0 && best_len == 0; try_len--) {
            for (const agent_skill *skill = registry->head;
                 skill; skill = skill->next) {
                if (strlen(skill->name) == try_len &&
                    memcmp(skill->name, name, try_len) == 0) {
                    best_len = try_len;
                    snprintf(matched_name, sizeof(matched_name), "%s", skill->name);
                    snprintf(matched_path, sizeof(matched_path), "%s", skill->path);
                    break;
                }
            }
        }
        pthread_mutex_unlock(&registry->mu);
        if (best_len == 0) {
            agent_buf_append_full(&out, cursor, 1 + name_len);
            cursor = end;
            continue;
        }

        agent_buf_puts(&out, "[skill:");
        agent_buf_puts(&out, matched_name);
        agent_buf_puts(&out, "](");
        agent_buf_puts(&out, matched_path);
        agent_buf_puts(&out, ")");
        cursor = name + best_len;
    }
    return agent_buf_take(&out);
}

/* Run one user turn until the assistant stops or returns a tool call.  Tool
 * results are appended to the transcript and the loop continues, which gives
 * the model native DSML tool iteration without a client/server protocol. */
static int worker_run_turn(agent_worker *w, const char *user_text) {
    agent_config *cfg = w->cfg;
    ds4_think_mode think_mode;
    pthread_mutex_lock(&w->mu);
    think_mode = effective_think_mode(cfg);
    w->interrupt = false;
    w->model_tool_round_used = 0;
    w->autonomy_stop_reason[0] = '\0';
    w->status.error[0] = '\0';
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);

    char compact_err[160] = {0};
    if (!agent_worker_compact_if_needed(w, "soft limit before user turn",
                                        compact_err, sizeof(compact_err)))
    {
        if (agent_err_is_interrupted(compact_err)) {
            worker_clear_interrupt(w);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }
        agent_set_error(w, compact_err[0] ? compact_err : "context compaction failed");
        return 1;
    }
    agent_worker_sync_skills_prompt(w);
    agent_worker_maybe_append_datetime_context(w);
    agent_trace_text(w, "user", user_text ? user_text : "",
                     user_text ? strlen(user_text) : 0);
    if (!w->session_title) {
        w->session_title = agent_session_title_from_prompt(user_text, 0);
        w->session_created_at = (uint64_t)time(NULL);
        agent_session_identity_sha(w->session_title, w->session_created_at,
                                   w->session_sha);
    }
    char *replaced = agent_expand_skill_references(w, user_text);
    user_text = replaced;
    ds4_chat_append_message(w->engine, &w->transcript, "user", user_text);
    free(replaced);

    uint64_t rng = cfg->gen.seed ? cfg->gen.seed :
        ((uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32) ^ (uint64_t)clock());
    pthread_mutex_lock(&w->mu);
    w->user_activity = true;
    w->session_dirty = true;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);

    /* A user turn may contain any number of assistant/tool/assistant rounds.
     * Coding agents naturally perform long read/edit/test loops, so there is
     * deliberately no artificial "too many tool calls" ceiling here: context
     * pressure, compaction, user Ctrl+C, and the model's final answer are the
     * real stopping conditions.  The transcript is the single source of truth:
     * after a DSML stanza completes we terminate that assistant message, append
     * the tool result as a tool message, then ask the model to continue. */
    for (int tool_round = 0; ; tool_round++) {
        pthread_mutex_lock(&w->mu);
        w->model_tool_round_used = tool_round;
        bool budget_exhausted = w->model_tool_round_budget > 0 &&
            tool_round >= w->model_tool_round_budget;
        if (budget_exhausted)
            snprintf(w->autonomy_stop_reason, sizeof(w->autonomy_stop_reason),
                     "budget-exhausted");
        pthread_mutex_unlock(&w->mu);
        if (budget_exhausted) {
            agent_publish_system_status(w, "Subagent budget exhausted");
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }
        if (tool_round > 0 &&
            !agent_worker_compact_if_needed(w, "soft limit before tool continuation",
                                            compact_err, sizeof(compact_err)))
        {
            if (agent_err_is_interrupted(compact_err)) {
                worker_clear_interrupt(w);
                agent_set_status(w, AGENT_WORKER_IDLE);
                return 0;
            }
            agent_set_error(w, compact_err[0] ? compact_err : "context compaction failed");
            return 1;
        }
        agent_worker_maybe_append_system_prompt_reminder(w);
        ds4_chat_append_assistant_prefix(w->engine, &w->transcript, think_mode);

        const ds4_tokens *prompt_for_sync = &w->transcript;
        int old_pos = ds4_session_pos(w->session);
        int common = ds4_session_common_prefix(w->session, &w->transcript);
        int cached = common == old_pos && w->transcript.len >= old_pos ? common : 0;

        int suffix = prompt_for_sync->len - cached;
        agent_trace(w, "prefill tool_round=%d transcript=%d prompt=%d cached=%d suffix=%d think=%s",
                    tool_round, w->transcript.len, prompt_for_sync->len,
                    cached, suffix, ds4_think_mode_name(think_mode));
        agent_trace_tokens(w, "prefill_suffix", prompt_for_sync, cached);

        pthread_mutex_lock(&w->mu);
        unsigned prefill_label = w->status.state == AGENT_WORKER_PREFILL ?
            w->status.prefill_label : agent_next_prefill_label();
        w->status.state = AGENT_WORKER_PREFILL;
        w->progress_base = cached;
        w->progress_started_at = now_sec();
        w->status.prefill_done = 0;
        w->status.prefill_total = suffix;
        w->status.prefill_label = prefill_label;
        w->status.prefill_tps = 0.0;
        w->status.generated = 0;
        w->status.gen_tps = 0.0;
        w->status.greedy_sampling = false;
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);

        char err[160];
        ds4_session_set_progress(w->session, worker_progress_cb, w);
        ds4_session_set_display_progress(w->session, worker_progress_cb, w);
        ds4_session_set_cancel(w->session, worker_cancel_session_cb, w);
        int sync_rc = agent_worker_session_sync(w, prompt_for_sync,
                                                AGENT_WORKER_PREFILL,
                                                err, sizeof(err));
        ds4_session_set_cancel(w->session, NULL, NULL);
        ds4_session_set_progress(w->session, NULL, NULL);
        ds4_session_set_display_progress(w->session, NULL, NULL);
        if (sync_rc == DS4_SESSION_SYNC_INTERRUPTED) {
            agent_publish_system_status(
                w, "Model reading interrupted; the model may only be aware of the prefix processed so far.");
            ds4_tokens_push(&w->transcript, ds4_token_eos(w->engine));
            worker_clear_interrupt(w);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }
        if (sync_rc != 0) {
            agent_set_error(w, err);
            return 1;
        }

        int max_tokens = cfg->gen.n_predict;
        int room = ds4_session_ctx(w->session) - ds4_session_pos(w->session);
        if (room <= 1) max_tokens = 0;
        else if (max_tokens > room - 1) max_tokens = room - 1;

        bool use_color = isatty(STDOUT_FILENO) != 0;
        agent_token_renderer renderer = {
            .engine = w->engine,
            .worker = w,
            .format_thinking = ds4_think_mode_enabled(think_mode),
            .format_markdown = use_color,
            .in_think = ds4_think_mode_enabled(think_mode),
            .use_color = use_color,
            .last_output_newline = true,
        };
        agent_dsml_parser dsml = {.state = AGENT_DSML_SEARCH};
        agent_stream_renderer stream = {
            .renderer = &renderer,
            .parser = &dsml,
            .in_think = ds4_think_mode_enabled(think_mode),
        };
        agent_edit_upto_forcer upto_forcer = {0};
        bool got_tool = false;
        bool malformed_tool = false;
        bool early_tool_error = false;
        bool tool_execution_interrupted = false;
        int generated = 0;
        double t0 = now_sec();

        pthread_mutex_lock(&w->mu);
        w->status.state = AGENT_WORKER_GENERATING;
        w->status.prefill_done = 0;
        w->status.prefill_total = 0;
        w->status.prefill_tps = 0.0;
        w->status.greedy_sampling = false;
        agent_wake_locked(w);
        pthread_mutex_unlock(&w->mu);

        bool status_greedy_sampling = false;
        while (generated < max_tokens && !worker_should_interrupt(w)) {
            worker_apply_pending_power(w);
            bool greedy_sampling = agent_stream_wants_greedy_sampling(&stream);
            if (greedy_sampling != status_greedy_sampling) {
                worker_set_greedy_sampling(w, greedy_sampling);
                status_greedy_sampling = greedy_sampling;
            }
            int token = worker_sample_with_mode(w, cfg, greedy_sampling, &rng);
            if (token == ds4_token_eos(w->engine)) break;

            size_t text_len = 0;
            char *text = ds4_token_text(w->engine, token, &text_len);
            if (!stream.dsml_origin_in_think &&
                agent_edit_upto_forcer_should_replace(w, &upto_forcer, &dsml,
                                                       text, text_len))
            {
                agent_trace(w, "edit old auto-upto replaced token=%d text=%.*s",
                            token, (int)(text_len > 80 ? 80 : text_len), text);
                free(text);
                if (worker_force_generated_text(w, "[upto]\n", max_tokens,
                                                &generated, t0, &stream,
                                                err, sizeof(err)) != 0) {
                    agent_dsml_parser_free(&dsml);
                    agent_set_error(w, err);
                    return 1;
                }
            } else {
                free(text);
                if (worker_accept_generated_token(w, token, &generated, t0,
                                                  &stream, err, sizeof(err)) != 0) {
                    agent_dsml_parser_free(&dsml);
                    agent_set_error(w, err);
                    return 1;
                }
            }

            greedy_sampling = agent_stream_wants_greedy_sampling(&stream);
            if (greedy_sampling != status_greedy_sampling) {
                worker_set_greedy_sampling(w, greedy_sampling);
                status_greedy_sampling = greedy_sampling;
            }

            if (dsml.state == AGENT_DSML_DONE) {
                got_tool = true;
                break;
            }
            if (stream.tool_preflight_error) {
                early_tool_error = true;
                break;
            }
            if (dsml.state == AGENT_DSML_ERROR) {
                malformed_tool = true;
                break;
            }
            if (stream.dsml_in_think) {
                malformed_tool = true;
                break;
            }
        }

        bool interrupted = worker_should_interrupt(w);
        agent_stream_text(&stream, NULL, 0, true);
        renderer_finish(&renderer);
        worker_set_greedy_sampling(w, false);
        if (interrupted) {
            ds4_tokens_push(&w->transcript, ds4_token_eos(w->engine));
            agent_dsml_parser_free(&dsml);
            agent_publish_system_status(w, "Stopped by user");
            pthread_mutex_lock(&w->mu);
            snprintf(w->autonomy_stop_reason, sizeof(w->autonomy_stop_reason),
                     "interrupted");
            pthread_mutex_unlock(&w->mu);
            worker_clear_interrupt(w);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }
        bool in_thinking_block = stream.dsml_origin_in_think;
        if (stream.dsml_in_think) {
            got_tool = false;
            malformed_tool = true;
            early_tool_error = false;
            snprintf(dsml.error, sizeof(dsml.error),
                     "malformed DSML tool call inside <think></think>");
        } else if (got_tool && in_thinking_block &&
                   !agent_thinking_tool_block_validate(
                       &dsml, dsml.error, sizeof(dsml.error))) {
            got_tool = false;
            malformed_tool = true;
            early_tool_error = false;
        } else if (!malformed_tool && dsml.state == AGENT_DSML_ERROR) {
            malformed_tool = true;
        } else if (!got_tool && !malformed_tool && !early_tool_error &&
                   !interrupted &&
                   (dsml.state == AGENT_DSML_STRUCTURAL ||
                    dsml.state == AGENT_DSML_PARAM_VALUE))
        {
            malformed_tool = true;
            snprintf(dsml.error, sizeof(dsml.error),
                     "incomplete DSML tool call");
        }

        ds4_tokens_push(&w->transcript, ds4_token_eos(w->engine));

        if (!got_tool && !malformed_tool && !early_tool_error) {
            agent_dsml_parser_free(&dsml);
            pthread_mutex_lock(&w->mu);
            if (!w->autonomy_stop_reason[0])
                snprintf(w->autonomy_stop_reason,
                         sizeof(w->autonomy_stop_reason), "done");
            pthread_mutex_unlock(&w->mu);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }

        char *tool_result;
        if (early_tool_error) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: ");
            agent_buf_puts(&b, stream.tool_preflight_error_msg[0] ?
                           stream.tool_preflight_error_msg :
                           "edit old selector failed before new was generated");
            agent_buf_puts(&b, "\n");
            tool_result = agent_buf_take(&b);
        } else if (malformed_tool) {
            agent_buf b = {0};
            agent_buf_puts(&b, "Tool error: invalid DSML tool call: ");
            agent_buf_puts(&b, dsml.error[0] ? dsml.error : "parse error");
            agent_buf_puts(&b, "\n");
            agent_buf_puts(&b, agent_dsml_syntax_reminder);
            tool_result = agent_buf_take(&b);
        } else if (in_thinking_block) {
            tool_result = agent_execute_tool_calls_ordered(
                w, &dsml.calls, true, &tool_execution_interrupted);
            if (tool_execution_interrupted) worker_clear_interrupt(w);
        } else {
            tool_result = agent_execute_tool_calls(w, &dsml.calls);
        }
        int projected_tokens = 0;
        if (!agent_tool_result_fits_context(w, tool_result,
                                            AGENT_TOOL_RESULT_RESERVE_TOKENS,
                                            &projected_tokens))
        {
            if (!agent_worker_compact(w, "tool result would exceed context",
                                      compact_err, sizeof(compact_err)))
            {
                free(tool_result);
                agent_dsml_parser_free(&dsml);
                if (agent_err_is_interrupted(compact_err)) {
                    worker_clear_interrupt(w);
                    agent_set_status(w, AGENT_WORKER_IDLE);
                    return 0;
                }
                agent_set_error(w, compact_err[0] ? compact_err : "context compaction failed");
                return 1;
            }
            if (!agent_tool_result_fits_context(w, tool_result,
                                                AGENT_TOOL_RESULT_RESERVE_TOKENS,
                                                &projected_tokens))
            {
                free(tool_result);
                agent_buf b = {0};
                char msg[256];
                snprintf(msg, sizeof(msg),
                         "Tool error: tool result still does not fit after context compaction "
                         "(projected_prompt=%d tokens, ctx=%d, reserve=%d). "
                         "Retry with a smaller read/search/bash output.\n",
                         projected_tokens, w->cfg->gen.ctx_size,
                         AGENT_TOOL_RESULT_RESERVE_TOKENS);
                agent_buf_puts(&b, msg);
                tool_result = agent_buf_take(&b);
                if (!agent_tool_result_fits_context(w, tool_result, 16, NULL)) {
                    free(tool_result);
                    agent_dsml_parser_free(&dsml);
                    agent_set_error(w, "context full after compaction");
                    return 1;
                }
            }
        }
        ds4_chat_append_message(w->engine, &w->transcript, "tool", tool_result);
        free(tool_result);
        agent_dsml_parser_free(&dsml);

        if (tool_execution_interrupted) {
            agent_publish_system_status(w, "Stopped by user");
            pthread_mutex_lock(&w->mu);
            snprintf(w->autonomy_stop_reason, sizeof(w->autonomy_stop_reason),
                     "interrupted");
            pthread_mutex_unlock(&w->mu);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return 0;
        }

        char *queued_user = worker_request_queued_user_drain(w);
        if (queued_user && queued_user[0]) {
            agent_trace_text(w, "queued_user", queued_user, strlen(queued_user));
            ds4_chat_append_message(w->engine, &w->transcript, "user", queued_user);
            pthread_mutex_lock(&w->mu);
            w->user_activity = true;
            w->session_dirty = true;
            agent_wake_locked(w);
            pthread_mutex_unlock(&w->mu);
        }
        free(queued_user);
    }
}

static void worker_request_save(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->save_requested = true;
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void worker_request_compact(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->compact_requested = true;
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void worker_request_power(agent_worker *w, int power) {
    pthread_mutex_lock(&w->mu);
    w->requested_power = power;
    w->power_requested = true;
    w->status.power_percent = power;
    pthread_cond_signal(&w->cond);
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void worker_set_think_mode(agent_worker *w, ds4_think_mode mode) {
    pthread_mutex_lock(&w->mu);
    w->cfg->gen.think_mode = mode;
    w->status.think_mode = mode;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static ds4_think_mode worker_cycle_think_mode(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    ds4_think_mode mode = agent_next_think_mode(w->cfg->gen.think_mode);
    w->cfg->gen.think_mode = mode;
    w->status.think_mode = mode;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
    return mode;
}

static bool worker_take_save_requested(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool requested = w->save_requested;
    w->save_requested = false;
    pthread_mutex_unlock(&w->mu);
    return requested;
}

static bool worker_take_compact_requested(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool requested = w->compact_requested;
    w->compact_requested = false;
    pthread_mutex_unlock(&w->mu);
    return requested;
}

static bool worker_take_power_requested(agent_worker *w, int *power) {
    pthread_mutex_lock(&w->mu);
    bool requested = w->power_requested;
    if (requested) {
        if (power) *power = w->requested_power;
        w->power_requested = false;
    }
    pthread_mutex_unlock(&w->mu);
    return requested;
}

static void worker_apply_pending_power(agent_worker *w) {
    int power = 0;
    if (!worker_take_power_requested(w, &power)) return;
    if (agent_worker_session_set_power(w, power) != 0) {
        agent_publishf(w, "\npower change failed\n");
        return;
    }
    pthread_mutex_lock(&w->mu);
    w->cfg->engine.power_percent = power;
    w->status.power_percent = power;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);
}

static void worker_run_deferred_save(agent_worker *w) {
    if (!worker_take_save_requested(w)) return;
    agent_set_status(w, AGENT_WORKER_SAVING);
    char err[160] = {0};
    char sha[41];
    int tokens = 0;
    if (agent_worker_save_session_now(w, sha, &tokens, err, sizeof(err)))
        agent_publishf(w, "\nsaved session %.8s (%d tokens)\n", sha, tokens);
    else
        agent_publishf(w, "\nsave failed: %s\n", err[0] ? err : "unknown error");
    agent_set_status(w, AGENT_WORKER_IDLE);
}

static void worker_run_deferred_compact(agent_worker *w) {
    if (!worker_take_compact_requested(w)) return;
    if (!agent_worker_has_user_session(w)) {
        agent_publishf(w, "\ncompact skipped: nothing to compact\n");
        return;
    }

    int before = w->transcript.len;
    char err[160] = {0};
    if (agent_worker_compact(w, "user requested compaction", err, sizeof(err))) {
        if (w->transcript.len != before) {
            pthread_mutex_lock(&w->mu);
            w->session_dirty = true;
            agent_wake_locked(w);
            pthread_mutex_unlock(&w->mu);
        } else {
            agent_publishf(w, "\ncompact skipped: nothing to compact\n");
        }
        agent_set_status(w, AGENT_WORKER_IDLE);
    } else {
        if (agent_err_is_interrupted(err)) {
            worker_clear_interrupt(w);
            agent_set_status(w, AGENT_WORKER_IDLE);
            return;
        }
        agent_set_error(w, err[0] ? err : "context compaction failed");
    }
}

/* Worker thread entry point.  The UI thread submits plain user text; this
 * thread owns all DS4 session mutation, tool execution, and compaction. */
static void *worker_main(void *arg) {
    agent_worker *w = arg;
    agent_trace(w, "agent worker start ctx=%d backend=%s model=%s trace=%s",
                w->cfg->gen.ctx_size,
                ds4_backend_name(w->cfg->engine.backend),
                w->cfg->engine.model_path ? w->cfg->engine.model_path : "",
                w->cfg->gen.trace_path ? w->cfg->gen.trace_path : "");
    char init_err[160] = {0};
    if (!agent_worker_wait_distributed_route(w, init_err, sizeof(init_err))) {
        agent_set_error(w, init_err[0] ? init_err : "failed to initialize distributed route");
    } else if (w->cfg->recover_session && w->cfg->recover_session[0]) {
        if (!agent_worker_recover_session(w, w->cfg->recover_session,
                                          init_err, sizeof(init_err))) {
            agent_set_error(w, init_err[0] ? init_err : "failed to recover session");
        }
    } else if (!agent_worker_reset_to_sysprompt(w, init_err, sizeof(init_err))) {
        agent_set_error(w, init_err[0] ? init_err : "failed to initialize system prompt");
    }
    agent_trace_tokens(w, "initial_system_prompt", &w->transcript, 0);
    pthread_mutex_lock(&w->mu);
    w->initialized = true;
    agent_wake_locked(w);
    pthread_mutex_unlock(&w->mu);

    while (true) {
        pthread_mutex_lock(&w->mu);
        while (!w->stop && !w->cmd_text && !w->save_requested &&
               !w->compact_requested && !w->power_requested)
            pthread_cond_wait(&w->cond, &w->mu);
        if (w->stop) {
            pthread_mutex_unlock(&w->mu);
            break;
        }
        if (w->power_requested) {
            pthread_mutex_unlock(&w->mu);
            worker_apply_pending_power(w);
            continue;
        }
        if (!w->cmd_text && w->save_requested) {
            pthread_mutex_unlock(&w->mu);
            worker_run_deferred_save(w);
            continue;
        }
        if (!w->cmd_text && w->compact_requested) {
            pthread_mutex_unlock(&w->mu);
            worker_run_deferred_compact(w);
            continue;
        }
        char *cmd = w->cmd_text;
        w->cmd_text = NULL;
        pthread_mutex_unlock(&w->mu);

        worker_run_turn(w, cmd);
        free(cmd);
        worker_apply_pending_power(w);
        worker_run_deferred_compact(w);
        worker_run_deferred_save(w);
    }

    agent_set_status(w, AGENT_WORKER_STOPPED);
    return NULL;
}

/* ============================================================================
 * Worker/UI Synchronization Helpers
 * ============================================================================
 */

static int set_nonblock(int fd, bool on, int *old_flags) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (old_flags) *old_flags = flags;
    int next = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    return fcntl(fd, F_SETFL, next);
}

/* Check and clear the raw_mode_needs_restore flag under the worker mutex.
 * Returns true if the UI thread should verify/reapply linenoise raw mode. */
bool worker_check_raw_mode_restore(agent_worker *w) {
    bool needs = false;
    pthread_mutex_lock(&w->mu);
    if (w->raw_mode_needs_restore) {
        w->raw_mode_needs_restore = false;
        needs = true;
    }
    pthread_mutex_unlock(&w->mu);
    return needs;
}

void drain_wake_fd(int fd) {
    char buf[128];
    for (;;) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n > 0) continue;
        if (n < 0 && errno == EINTR) continue;
        break;
    }
}

/* Submit one user turn if the worker is idle.  Busy submissions are rejected so
 * the UI can keep the typed text editable instead of silently queueing it. */
bool worker_submit(agent_worker *w, const char *text) {
    pthread_mutex_lock(&w->mu);
    bool ok = w->initialized && w->status.state == AGENT_WORKER_IDLE && !w->cmd_text;
    if (ok) {
        w->cmd_text = xstrdup(text);
        /* A submitted turn is no longer idle, even if the worker thread has
         * not yet reached its real prefill accounting.  Non-interactive mode
         * depends on this to avoid exiting in the small handoff window between
         * accepting stdin and starting generation. */
        w->status.state = AGENT_WORKER_PREFILL;
        w->status.prefill_done = 0;
        w->status.prefill_total = 0;
        w->status.prefill_label = agent_next_prefill_label();
        w->status.prefill_tps = 0.0;
        w->status.generated = 0;
        w->status.gen_tps = 0.0;
        w->status.greedy_sampling = false;
        pthread_cond_signal(&w->cond);
    }
    pthread_mutex_unlock(&w->mu);
    return ok;
}

static int worker_status_power_locked(agent_worker *w) {
    if (w->power_requested) return w->requested_power;
    int power = w->cfg->engine.power_percent;
    return power > 0 ? power : 100;
}

static void worker_update_status_config_locked(agent_worker *w) {
    w->status.ctx_used = w->transcript.len;
    w->status.ctx_size = w->cfg->gen.ctx_size;
    w->status.think_mode = w->cfg->gen.think_mode;
    w->status.power_percent = worker_status_power_locked(w);
    w->status.session_id = w->session_slot_id;
    snprintf(w->status.session_name, sizeof(w->status.session_name), "%s",
             w->session_slot_name);
    w->status.background_sessions = w->background_sessions;
    w->status.unread_sessions = w->unread_sessions;
}

static void worker_update_status_workspace_locked(agent_worker *w) {
    const char *root = w->working_directories.len > 0 ?
        w->working_directories.v[0] : "";
    snprintf(w->status.workspace, sizeof(w->status.workspace), "%s", root);
}

/* What: copy the currently selected Docker container name into the status
 * snapshot while the worker mutex is held.
 * Why: the footer needs an immutable status copy so UI redraws do not read
 * mutable config while commands switch sandboxes.
 * Callers: worker_consume(), worker_get_status(), and worker_is_initialized(). */
static void worker_update_status_docker_locked(agent_worker *w) {
    const char *name = (w && w->cfg && w->cfg->docker_container) ?
        w->cfg->docker_container : "";
    snprintf(w->status.docker_container,
             sizeof(w->status.docker_container), "%s", name);
}

static void worker_update_status_writable_locked(agent_worker *w) {
    agent_temp_files_cleanup_now(w);
    agent_buf workspace = {0};
    agent_buf auto_allowed = {0};
    if (w->working_directories.len > 0) {
        for (int i = 0; i < w->working_directories.len; i++) {
            if (i) agent_buf_puts(&workspace, ", ");
            agent_buf_puts(&workspace, w->working_directories.v[i]);
        }
    }
    if (w->auto_allowed_paths.len > 0) {
        for (int i = 0; i < w->auto_allowed_paths.len; i++) {
            if (i) agent_buf_puts(&auto_allowed, ", ");
            agent_buf_puts(&auto_allowed, w->auto_allowed_paths.v[i]);
        }
    }
    snprintf(w->status.writable_workspace_paths,
             sizeof(w->status.writable_workspace_paths), "%s",
             workspace.ptr ? workspace.ptr : "");
    snprintf(w->status.writable_temp_paths,
             sizeof(w->status.writable_temp_paths), "%s",
             (w->cfg && w->cfg->temp_directory[0]) ? w->cfg->temp_directory : "");
    snprintf(w->status.writable_auto_paths,
             sizeof(w->status.writable_auto_paths), "%s",
             auto_allowed.ptr ? auto_allowed.ptr : "");
    free(workspace.ptr);
    free(auto_allowed.ptr);
}

/* What: compute the four-character tool-permissions string (R=read, W=write,
 * B=browse/web, X=bash) from the worker's tool policy.
 * Why: the UI footer shows which tool classes are enabled next to the sandbox
 * name so the user can see the effective access level at a glance.
 * Callers: worker_update_status_*_locked functions. */
static void worker_update_status_tool_policy_locked(agent_worker *w) {
    const ds4_agent_tool_policy *pol = &w->tool_policy;
    w->status.tool_permissions[0] =
        ds4_agent_tool_policy_class_allowed(pol, AGENT_TOOLS_CLASS_READ)  ? 'R' : '-';
    w->status.tool_permissions[1] =
        ds4_agent_tool_policy_class_allowed(pol, AGENT_TOOLS_CLASS_WRITE) ? 'W' : '-';
    w->status.tool_permissions[2] =
        ds4_agent_tool_policy_class_allowed(pol, AGENT_TOOLS_CLASS_WEB)   ? 'B' : '-';
    w->status.tool_permissions[3] =
        ds4_agent_tool_policy_class_allowed(pol, AGENT_TOOLS_CLASS_BASH)  ? 'X' : '-';
    w->status.tool_permissions[4] = '\0';
}

/* Request interruption at the next model/tool polling point. */
void worker_interrupt(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->interrupt = true;
    if (w->cfg &&
        w->cfg->engine.distributed.role == DS4_DISTRIBUTED_COORDINATOR &&
        (w->status.state == AGENT_WORKER_PREFILL ||
         w->status.state == AGENT_WORKER_GENERATING ||
         w->status.state == AGENT_WORKER_COMPACTING))
    {
        w->status.state = AGENT_WORKER_DRAINING;
        w->status.prefill_tps = 0.0;
        w->status.greedy_sampling = false;
        agent_wake_locked(w);
    }
    pthread_mutex_unlock(&w->mu);
}

/* Stop the worker thread. */
static void worker_stop(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    w->stop = true;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->mu);
}

/* The UI thread consumes output in batches.  Taking ownership of w->out under
 * the mutex keeps terminal writes outside the lock while preserving order. */
void worker_consume(agent_worker *w, char **out, size_t *out_len, agent_status *status) {
    pthread_mutex_lock(&w->mu);
    if (out) {
        *out = w->out;
        *out_len = w->out_len;
        w->out = NULL;
        w->out_len = 0;
        w->out_cap = 0;
    }
    worker_update_status_config_locked(w);
    worker_update_status_workspace_locked(w);
    worker_update_status_docker_locked(w);
    worker_update_status_writable_locked(w);
    worker_update_status_tool_policy_locked(w);
    if (status) *status = w->status;
    w->wake_pending = false;
    pthread_mutex_unlock(&w->mu);
}

void worker_get_status(agent_worker *w, agent_status *status) {
    pthread_mutex_lock(&w->mu);
    worker_update_status_config_locked(w);
    worker_update_status_workspace_locked(w);
    worker_update_status_docker_locked(w);
    worker_update_status_writable_locked(w);
    worker_update_status_tool_policy_locked(w);
    *status = w->status;
    pthread_mutex_unlock(&w->mu);
}

bool worker_is_idle(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    bool idle = w->initialized &&
        (w->status.state == AGENT_WORKER_IDLE ||
         w->status.state == AGENT_WORKER_ERROR);
    pthread_mutex_unlock(&w->mu);
    return idle;
}

static bool worker_is_initialized(agent_worker *w, agent_status *status) {
    pthread_mutex_lock(&w->mu);
    worker_update_status_config_locked(w);
    worker_update_status_workspace_locked(w);
    worker_update_status_docker_locked(w);
    worker_update_status_writable_locked(w);
    worker_update_status_tool_policy_locked(w);
    if (status) *status = w->status;
    bool initialized = w->initialized;
    pthread_mutex_unlock(&w->mu);
    return initialized;
}

static void agent_worker_print_workspaces(agent_worker *w) {
    pthread_mutex_lock(&w->mu);
    if (w->working_directories.len == 0 && w->auto_allowed_paths.len == 0) {
        pthread_mutex_unlock(&w->mu);
        printf("no working directories or auto-allowed paths configured\n");
        return;
    }
    if (w->working_directories.len > 0) {
        printf("working directories:\n");
        for (int i = 0; i < w->working_directories.len; i++) {
            printf("%c %d. %s\n", i == 0 ? '*' : ' ', i + 1,
                   w->working_directories.v[i]);
        }
    }
    if (w->auto_allowed_paths.len > 0) {
        printf("auto-allowed paths (bypass workspace checks):\n");
        for (int i = 0; i < w->auto_allowed_paths.len; i++) {
            printf("  %d. %s\n", i + 1, w->auto_allowed_paths.v[i]);
        }
    }
    pthread_mutex_unlock(&w->mu);
}

static bool agent_worker_add_workspace(agent_worker *w, const char *path,
                                       char *resolved_out, size_t resolved_len,
                                       char *err, size_t err_len) {
    char resolved[PATH_MAX];
    if (!agent_resolve_working_directory_arg(path, resolved, err, err_len))
        return false;
    pthread_mutex_lock(&w->mu);
    bool exists = agent_path_list_contains(&w->working_directories, resolved);
    if (!exists) {
        agent_path_list_append(&w->working_directories, resolved);
        worker_update_status_workspace_locked(w);
        agent_wake_locked(w);
    }
    pthread_mutex_unlock(&w->mu);
    if (resolved_out) snprintf(resolved_out, resolved_len, "%s", resolved);
    if (exists) snprintf(err, err_len, "workspace already configured: %s", resolved);
    return !exists;
}

static bool agent_worker_remove_workspace(agent_worker *w, const char *path,
                                          char *removed, size_t removed_len,
                                          char *err, size_t err_len) {
    char resolved[PATH_MAX];
    bool can_resolve = agent_resolve_working_directory_arg(path, resolved,
                                                          err, err_len);
    pthread_mutex_lock(&w->mu);
    bool ok = false;
    if (can_resolve) {
        /* Never remove the last workspace — doing so disables jail enforcement
         * and bash sandboxing. */
        if (w->working_directories.len == 1 &&
            agent_path_list_contains(&w->working_directories, resolved)) {
            snprintf(err, err_len, "cannot remove last workspace: %s", resolved);
        } else {
            ok = agent_path_list_remove(&w->working_directories, resolved, false);
            if (ok) {
                if (removed) snprintf(removed, removed_len, "%s", resolved);
                /* Remove any auto-allowed paths that fall under this workspace. */
                agent_path_list_remove(&w->auto_allowed_paths, resolved, true);
                worker_update_status_workspace_locked(w);
                agent_wake_locked(w);
            } else {
                /* Not a workspace root; try auto-allowed paths instead. */
                ok = agent_path_list_remove(&w->auto_allowed_paths, resolved, false);
                if (ok) {
                    if (removed) snprintf(removed, removed_len, "%s", resolved);
                }
            }
        }
    } else {
        /* Path is not a directory (e.g. a file in auto_allowed_paths).
         * Try removing it from auto_allowed_paths using the raw path. */
        ok = agent_path_list_remove(&w->auto_allowed_paths, path, false);
        if (ok) {
            if (removed) snprintf(removed, removed_len, "%s", path);
        }
    }
    pthread_mutex_unlock(&w->mu);
    if (!ok) {
        if (!err[0])
            snprintf(err, err_len, "not configured: %s", path);
    }
    return ok;
}

static bool stdout_is_tty(void) {
    return isatty(STDOUT_FILENO) != 0;
}

static char *agent_format_user_prompt_echo(const char *text) {
    agent_buf b = {0};
    if (stdout_is_tty()) {
        agent_buf_puts(&b, "\x1b[1;91m*\x1b[1;97m ");
        agent_buf_puts(&b, text);
        agent_buf_puts(&b, "\x1b[0m\n\n");
    } else {
        agent_buf_puts(&b, "* ");
        agent_buf_puts(&b, text);
        agent_buf_puts(&b, "\n\n");
    }
    return agent_buf_take(&b);
}

/* ============================================================================
 * Terminal Prompt, Status Footer, And Async Output Rendering
 * ============================================================================
 */

#define AGENT_INPUT_INITIAL_BUFLEN 4096
#define AGENT_INPUT_MAX_BUFLEN (1024*1024)
#define AGENT_STATUS_STYLE_START "\x1b[48;5;238;38;5;252m"
#define AGENT_STATUS_STYLE_END "\x1b[0m"
#define AGENT_QUEUE_STYLE "\x1b[38;5;87;1m"
#define AGENT_STATUS_REDRAW_INTERVAL_SEC 0.20

static void build_prompt_text(const agent_status *st, char *buf, size_t len) {
    if (st && st->session_name[0])
        snprintf(buf, len, "%s#%" PRIu64 "> ", st->session_name, st->session_id);
    else
        snprintf(buf, len, "#> ");
}



static unsigned agent_next_prefill_label(void) {
    static unsigned next;
    return next++;
}

void agent_prompt_queue_push(agent_prompt_queue *q, const char *text) {
    if (q->len == q->cap) {
        q->cap = q->cap ? q->cap * 2 : 4;
        q->v = xrealloc(q->v, q->cap * sizeof(q->v[0]));
    }
    q->v[q->len++] = xstrdup(text ? text : "");
}

char *agent_prompt_queue_pop(agent_prompt_queue *q) {
    if (!q->len) return NULL;
    char *text = q->v[0];
    memmove(q->v, q->v + 1, (q->len - 1) * sizeof(q->v[0]));
    q->len--;
    return text;
}

void agent_prompt_queue_push_front(agent_prompt_queue *q, char *text) {
    if (q->len == q->cap) {
        q->cap = q->cap ? q->cap * 2 : 4;
        q->v = xrealloc(q->v, q->cap * sizeof(q->v[0]));
    }
    memmove(q->v + 1, q->v, q->len * sizeof(q->v[0]));
    q->v[0] = text;
    q->len++;
}

char *agent_prompt_queue_take_all(agent_prompt_queue *q) {
    if (!q->len) return NULL;
    if (q->len == 1) return agent_prompt_queue_pop(q);

    agent_buf b = {0};
    for (size_t i = 0; i < q->len; i++) {
        char hdr[64];
        if (i) agent_buf_puts(&b, "\n\n");
        snprintf(hdr, sizeof(hdr), "Queued user message %zu:\n", i + 1);
        agent_buf_puts(&b, hdr);
        agent_buf_puts(&b, q->v[i]);
        free(q->v[i]);
    }
    q->len = 0;
    return agent_buf_take(&b);
}

const char *agent_prompt_queue_peek(const agent_prompt_queue *q) {
    return q->len ? q->v[0] : NULL;
}

void agent_prompt_queue_free(agent_prompt_queue *q) {
    for (size_t i = 0; i < q->len; i++) free(q->v[i]);
    free(q->v);
    memset(q, 0, sizeof(*q));
}



/* --- Display-width measurement and badge formatting --- */

/* Measure visible display columns of a string, skipping ANSI escape sequences.
 * Each non-continuation UTF-8 byte counts as one display column.
 * Four-byte UTF-8 characters (0xF0-0xF7) like 🚫 and 🧠 get width 2
 * because terminals render them at 2 columns.  Three-byte characters
 * (0xE0-0xEF) like ▶ and ⚡ are width 1 in most terminals so remain
 * at 1.  This is a simplified wcwidth that matches the emoji used
 * in agent badges. */
static size_t agent_display_width(const char *s) {
    size_t width = 0;
    bool in_escape = false;
    while (s && *s) {
        unsigned char c = (unsigned char)*s++;
        if (in_escape) {
            if (c == 'm') in_escape = false;
            continue;
        }
        if (c == 0x1b) { in_escape = true; continue; }
        /* Skip UTF-8 continuation bytes (0x80-0xBF) */
        if ((c & 0xc0) == 0x80) continue;
        width++;
        /* Four-byte UTF-8 start byte → wide emoji, add extra column */
        if (c >= 0xf0) width++;
        /* Variation selectors U+FE00-U+FE0F (EF B8 80 through EF B8 8F)
         * are zero-width.  Undo the width increment when detected. */
        if (c == 0xef && s[0] && s[1]) {
            unsigned char b2 = (unsigned char)s[0];
            unsigned char b3 = (unsigned char)s[1];
            if (b2 == 0xb8 && b3 >= 0x80 && b3 <= 0x8f) {
                width--;
            }
        }
    }
    return width;
}

/* Format a badge for one resident session.  Produces visible content (no ANSI)
 * and the fully styled badge text.  Badge grammar (from spec):
 *   [ {▶|■|!} <id>:<name> | <icon> <used>k/<total>k | | <activity> | <perms> ]
 * where <icon> is 🚫/🧠/⚡️ (no "think:" prefix) and <activity> is mutually
 * exclusive: pp with one-decimal speed, gen with one-decimal speed, or ---. */
static void agent_format_badge(const ds4_agent_subagent_status *item,
                                char *visible, size_t visible_len,
                                char *styled, size_t styled_len) {
    char used[32], total_ctx[32];
    agent_format_ctx_size(item->ctx_used, used, sizeof(used));
    agent_format_ctx_size(item->ctx_size, total_ctx, sizeof(total_ctx));

    /* Indicator: attention (!) overrides engine-owner (▶), else non-owner (■).
     * Focus brackets (magenta) are selected by item->active independently. */
    const char *indicator;
    const char *indicator_color;
    const char *bracket_color;
    bool needs_attention = item->approval_blocked ||
        item->state == DS4_AGENT_SUBAGENT_STATE_ERROR ||
        item->queued_output;
    if (needs_attention) {
        indicator = "!";
        indicator_color = "\x1b[33m";
        bracket_color = item->active ? "\x1b[35m" : "";
    } else if (item->engine_owner) {
        indicator = "▶";  /* ▶ U+25B6 */
        indicator_color = "\x1b[32m";
        bracket_color = item->active ? "\x1b[35m" : "";
    } else {
        indicator = "■";  /* U+25A0 */
        indicator_color = "\x1b[35m";
        bracket_color = item->active ? "\x1b[35m" : "";
    }

    /* Mutually exclusive fixed-width activity field.
     * Prefill: pp <pct width 5>% <integer speed width 4> t/s
     * Generation/compaction: gen <speed width 5, one decimal> t/s
     * Otherwise: --- (neutral placeholder, contains neither pp nor gen) */
    char activity[64];
    if (item->gen_tps > 0.0 &&
        (item->state == DS4_AGENT_SUBAGENT_STATE_RUNNING ||
         item->state == DS4_AGENT_SUBAGENT_STATE_WAITING_MODEL)) {
        snprintf(activity, sizeof(activity), "gen %5.1f t/s", item->gen_tps);
    } else if (item->state == DS4_AGENT_SUBAGENT_STATE_RUNNING &&
               item->prefill_total > 0) {
        int done = item->prefill_done;
        int total_p = item->prefill_total > 0 ? item->prefill_total : 1;
        if (done > total_p) done = total_p;
        double pct = 100.0 * (double)done / (double)total_p;
        snprintf(activity, sizeof(activity), "pp %5.1f%% %4.0f t/s",
                 pct, item->prefill_tps);
    } else {
        snprintf(activity, sizeof(activity), "%s", "---");
    }

    /* Identity: <id>:<name> */
    char identity[128];
    snprintf(identity, sizeof(identity), "%" PRIu64 ":%s", item->id.value, item->name);

    /* Permissions as final field */
    const char *perms = item->tool_permissions[0] ? item->tool_permissions : "----";

    /* Visible content (no ANSI control sequences) */
    snprintf(visible, visible_len, "[ %s %s | %s %5s/%s | %s | %s ]",
             indicator, identity, agent_think_mode_icon(item->think_mode),used, total_ctx,
              activity, perms);

    /* Styled version with ANSI.  For active sessions, magenta brackets
     * surround the badge.  After the colored indicator, restore the
     * status-line background with AGENT_STATUS_STYLE_START. */
    const char *close = bracket_color && bracket_color[0] ? bracket_color : "";
    snprintf(styled, styled_len,
             "%s[ %s%s" AGENT_STATUS_STYLE_START " %s | %s %5s/%s | %s | %s %s]" AGENT_STATUS_STYLE_START,
             bracket_color,
             indicator_color, indicator,
             identity, agent_think_mode_icon(item->think_mode),
             used, total_ctx,
             activity, perms,
             close);
}


/* Build the editable footer from the manager's resident-session list.
 * The list already includes main at index 0, so every badge is rendered
 * exactly once in stable main-then-subagent order.  No separate active
 * badge is prepended.  Badges wrap across multiple rows by display width;
 * no session is ever omitted or replaced with an ellipsis.
 *
 * With queued prompts, the footer becomes multiple rows: a compact queue
 * preview first, then the normal status rows. */
static void build_footer_text(const agent_status *st, ds4_agent_subagents *subagents,
                              const agent_prompt_queue *queue, int cols,
                              char *buf, size_t len) {
    if (len == 0) return;
    bool color = stdout_is_tty();

    /* Sandbox indicator prepended once before the first badge */
    char sandbox[64];
    if (st->docker_container[0])
        snprintf(sandbox, sizeof(sandbox), "✅ %s ", st->docker_container);
    else
        snprintf(sandbox, sizeof(sandbox), "\xf0\x9f\x9a\xa8 no-sandbox ");  /* 🚨 */

    agent_buf footer = {0};
    agent_buf_puts(&footer, sandbox);

    /* Render every resident session from the manager list.  This includes
     * main (slot 0), so no separate build_status_text is needed. */
    if (subagents) {
        size_t n = 0;
        if (ds4_agent_subagent_list(subagents, NULL, 0, &n) == 0 && n > 0) {
            ds4_agent_subagent_status *items = xmalloc(n * sizeof(items[0]));
            ds4_agent_subagent_list(subagents, items, n, &n);

            /* Multi-row wrapping by display width.
             * We track the visible-column usage of the current row.
             * When the next badge does not fit, we start a new row.
             * If one badge alone exceeds cols, we split it across rows. */
            size_t sandbox_w = agent_display_width(sandbox);
            size_t row_used = sandbox_w;
            bool first_badge = true;

            for (size_t i = 0; i < n; i++) {
                char visible[256], styled[320];
                agent_format_badge(&items[i], visible, sizeof(visible),
                                    styled, sizeof(styled));
                size_t badge_w = agent_display_width(visible);
                size_t sep_w = first_badge ? 0 : 1;  /* space between badges */

                /* Check if the badge fits on the current row.
                 * If the row is empty (row_used == 0), always place it
                 * even if it exceeds cols (we will split it). */
                if (row_used + sep_w + badge_w > (size_t)cols && row_used > 0) {
                    /* Start a new row */
                    agent_buf_puts(&footer, "\n");
                    row_used = 0;
                    sep_w = 0;
                }

                /* If the badge still does not fit even on an empty row,
                 * split it at visible-column boundaries. */
                if (row_used + sep_w + badge_w > (size_t)cols) {
                    /* Split the styled text across multiple rows.
                     * We split by display width at column boundaries. */
                    size_t avail = (size_t)cols - row_used - sep_w;
                    if (avail == 0) avail = (size_t)cols;  /* fresh row */
                    bool in_escape = false;
                    size_t col = 0;
                    const char *vp = visible;
                    while (*vp && col < avail) {
                        unsigned char c = (unsigned char)*vp;
                        if ((c & 0xc0) == 0x80) { vp++; continue; } /* continuation */
                        col++;
                        vp++;
                    }
                    /* vp now points to the byte where visible width exceeds avail.
                     * Find the corresponding position in the styled text. */
                    size_t styled_split = 0;
                    in_escape = false;
                    size_t vcol = 0;
                    const char *sp = styled;
                    while (*sp && vcol < col) {
                        unsigned char c = (unsigned char)*sp;
                        if (in_escape) {
                            if (c == 'm') in_escape = false;
                            styled_split = (size_t)(sp - styled) + 1;
                            sp++;
                            continue;
                        }
                        if (c == 0x1b) { in_escape = true; sp++; continue; }
                        if ((c & 0xc0) == 0x80) { sp++; continue; }
                        vcol++;
                        styled_split = (size_t)(sp - styled) + 1;
                        sp++;
                    }
                    /* Write first part on current row */
                    if (sep_w) agent_buf_puts(&footer, " ");
                    agent_buf_append(&footer, styled, styled_split);
                    if (color) agent_buf_puts(&footer, AGENT_STATUS_STYLE_START);
                    /* Remainder on next row */
                    agent_buf_puts(&footer, "\n");
                    agent_buf_puts(&footer, styled + styled_split);
                    row_used = badge_w - col;
                    first_badge = false;
                    continue;
                }

                /* Badge fits on the current row (possibly after wrapping) */
                if (sep_w) agent_buf_puts(&footer, " ");
                agent_buf_puts(&footer, styled);
                row_used += sep_w + badge_w;
                first_badge = false;
            }
            free(items);
        }
    }

    /* Handle queued prompts: multi-row layout with queue preview + footer */
    if (!queue || !queue->len) {
        snprintf(buf, len, "%s", footer.ptr ? footer.ptr : "");
        free(footer.ptr);
        return;
    }

    const char *queued = agent_prompt_queue_peek(queue);
    if (cols < 40) cols = 40;
    int max_rows = 3;
    size_t budget = (size_t)cols * (size_t)max_rows;
    const char *plain_suffix = " (ctrl+x to edit, ESC to send ASAP)";
    size_t queued_len = strlen(queued);
    char more_suffix[160];
    const char *suffix = plain_suffix;
    size_t take = queued_len;
    if (queued_len + strlen(plain_suffix) > budget) {
        size_t reserve = 72;
        take = budget > reserve ? budget - reserve : budget / 2;
        snprintf(more_suffix, sizeof(more_suffix),
                 "... %zu characters more ..., (ctrl+x to edit, ESC to send ASAP)",
                 queued_len - take);
        suffix = more_suffix;
    }

    agent_buf msg = {0};
    agent_buf_puts(&msg, "queued: ");
    for (size_t i = 0; i < take; i++) {
        char c = queued[i];
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
        agent_buf_append(&msg, &c, 1);
    }
    agent_buf_puts(&msg, suffix);
    char *preview = agent_buf_take(&msg);

    agent_buf out = {0};
    size_t pos = 0, preview_len = strlen(preview);
    for (int row = 0; row < max_rows && pos < preview_len; row++) {
        if (row) agent_buf_puts(&out, "\n");
        if (color) agent_buf_puts(&out, AGENT_QUEUE_STYLE);
        size_t part = preview_len - pos;
        if (part > (size_t)cols) part = (size_t)cols;
        agent_buf_append(&out, preview + pos, part);
        if (color) agent_buf_puts(&out, "\x1b[0m");
        pos += part;
    }
    agent_buf_puts(&out, "\n");
    agent_buf_puts(&out, footer.ptr ? footer.ptr : "");
    snprintf(buf, len, "%s", out.ptr ? out.ptr : "");
    free(preview);
    free(out.ptr);
    free(footer.ptr);
}

typedef struct {
    struct linenoiseState edit;
    char *input;
    char prompt[160];
    char status[4096];
    int old_stdin_flags;
    bool active;
    bool hidden;
    bool output_line_open;
    bool prompt_below_output;
    int output_col;
    bool scroll_region;
    int term_rows;
    int term_cols;
    int output_bottom;
    int prompt_row;
    int reserved_rows;
    bool output_cursor_saved;
    bool output_at_scroll_boundary;
    double last_prompt_redraw_time;
    char cpr_buf[32];
    size_t cpr_len;
    bool paste_open;
    bool paste_start_pending;
    char paste_tail[6];
    size_t paste_tail_len;
    double alt_tab_prefix_since;
} agent_editor;

static void editor_queue_bytes(agent_editor *ed, const char *buf, size_t len);
static void editor_hide(agent_editor *ed);
static void editor_show(agent_editor *ed);

typedef enum {
    CPR_INVALID,
    CPR_PARTIAL,
    CPR_COMPLETE,
} cpr_state;

/* Classify a possible terminal cursor-position reply (ESC[row;colR).  User
 * keystrokes can arrive interleaved with these replies, so we only swallow bytes
 * when they are definitely part of a complete CPR sequence. */
static cpr_state cpr_candidate_state(const char *buf, size_t len) {
    if (len == 0) return CPR_PARTIAL;
    if ((unsigned char)buf[0] != 0x1b) return CPR_INVALID;
    if (len == 1) return CPR_PARTIAL;
    if (buf[1] != '[') return CPR_INVALID;
    if (len == 2) return CPR_PARTIAL;

    size_t p = 2;
    if (buf[p] < '0' || buf[p] > '9') return CPR_INVALID;
    while (p < len && buf[p] >= '0' && buf[p] <= '9') p++;
    if (p == len) return CPR_PARTIAL;
    if (buf[p++] != ';') return CPR_INVALID;
    if (p == len) return CPR_PARTIAL;
    if (buf[p] < '0' || buf[p] > '9') return CPR_INVALID;
    while (p < len && buf[p] >= '0' && buf[p] <= '9') p++;
    if (p == len) return CPR_PARTIAL;
    return p + 1 == len && buf[p] == 'R' ? CPR_COMPLETE : CPR_INVALID;
}

static void editor_flush_cpr_candidate(agent_editor *ed) {
    if (!ed->cpr_len) return;
    linenoiseEditQueueInput(&ed->edit, ed->cpr_buf, ed->cpr_len);
    ed->cpr_len = 0;
}

static bool agent_tail_ends_with(const char *tail, size_t tail_len,
                                 const char *seq, size_t seq_len) {
    return tail_len >= seq_len &&
           memcmp(tail + tail_len - seq_len, seq, seq_len) == 0;
}

static bool agent_tail_has_seq_prefix(const char *tail, size_t tail_len,
                                      const char *seq, size_t seq_len) {
    size_t max = tail_len < seq_len - 1 ? tail_len : seq_len - 1;
    for (size_t n = max; n > 0; n--) {
        if (memcmp(tail + tail_len - n, seq, n) == 0) return true;
    }
    return false;
}

/* Track bracketed paste markers outside linenoise.  The nonblocking event loop
 * may receive a paste in chunks; pausing linenoiseEditFeed() until ESC[201~
 * arrives prevents pasted newlines from being interpreted as Enter. */
static void editor_track_bracketed_paste(agent_editor *ed, char c) {
    static const char start[] = "\x1b[200~";
    static const char end[] = "\x1b[201~";

    if (ed->paste_tail_len == sizeof(ed->paste_tail)) {
        memmove(ed->paste_tail, ed->paste_tail + 1, sizeof(ed->paste_tail) - 1);
        ed->paste_tail_len--;
    }
    ed->paste_tail[ed->paste_tail_len++] = c;

    /* The blocking linenoise() path waits inside linenoiseEditPaste() until it
     * sees ESC[201~. In the agent the outer event loop reads stdin in
     * non-blocking chunks; if we let linenoise start parsing ESC[200~ before
     * the closing marker has arrived, pasted newlines can be interpreted as
     * Enter. Keep feeding bytes into linenoise's queue, but don't call
     * linenoiseEditFeed() while the terminal paste envelope is still open. */
    if (agent_tail_ends_with(ed->paste_tail, ed->paste_tail_len,
                             start, sizeof(start) - 1))
    {
        ed->paste_open = true;
        ed->paste_start_pending = false;
    } else if (agent_tail_ends_with(ed->paste_tail, ed->paste_tail_len,
                                    end, sizeof(end) - 1))
    {
        ed->paste_open = false;
        ed->paste_start_pending = false;
    } else {
        ed->paste_start_pending =
            !ed->paste_open &&
            agent_tail_has_seq_prefix(ed->paste_tail, ed->paste_tail_len,
                                      start, sizeof(start) - 1);
    }
}

/* Separate late CPR replies from real user input before handing bytes to
 * linenoise. */
static void editor_filter_input_byte(agent_editor *ed, char c) {
    if (ed->cpr_len || (unsigned char)c == 0x1b) {
        if (ed->cpr_len == sizeof(ed->cpr_buf)) {
            editor_flush_cpr_candidate(ed);
        }
        ed->cpr_buf[ed->cpr_len++] = c;
        cpr_state st = cpr_candidate_state(ed->cpr_buf, ed->cpr_len);
        if (st == CPR_COMPLETE) {
            ed->cpr_len = 0; /* Late terminal cursor report: discard it. */
        } else if (st == CPR_INVALID) {
            editor_flush_cpr_candidate(ed);
        }
        return;
    }
    linenoiseEditQueueInput(&ed->edit, &c, 1);
}

/* Queue raw terminal bytes into linenoise while preserving paste envelopes and
 * filtering cursor-position replies. */
static void editor_queue_bytes(agent_editor *ed, const char *buf, size_t len) {
    for (size_t i = 0; i < len; i++) {
        editor_track_bracketed_paste(ed, buf[i]);
        editor_filter_input_byte(ed, buf[i]);
    }
}

/* Drain stdin in nonblocking mode.  The outer event loop decides when queued
 * bytes are fed to linenoiseEditFeed(). */
static void editor_read_stdin(agent_editor *ed) {
    char buf[256];
    for (;;) {
        ssize_t n = read(STDIN_FILENO, buf, sizeof(buf));
        if (n > 0) {
            editor_queue_bytes(ed, buf, (size_t)n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
}

static bool editor_take_queued_byte(agent_editor *ed, unsigned char byte) {
    struct linenoiseState *l = &ed->edit;
    for (size_t i = l->queued_input_pos; i < l->queued_input_len; i++) {
        if ((unsigned char)l->queued_input[i] != byte) continue;
        memmove(l->queued_input + i, l->queued_input + i + 1,
                l->queued_input_len - i - 1);
        l->queued_input_len--;
        if (l->queued_input_pos > l->queued_input_len)
            l->queued_input_pos = l->queued_input_len;
        return true;
    }
    return false;
}

static bool linenoise_take_queued_sequence(struct linenoiseState *l,
                                           const char *seq, size_t seq_len) {
    if (!l || !seq || seq_len == 0) return false;
    for (size_t i = l->queued_input_pos; i + seq_len <= l->queued_input_len; i++) {
        if (memcmp(l->queued_input + i, seq, seq_len) != 0) continue;
        memmove(l->queued_input + i, l->queued_input + i + seq_len,
                l->queued_input_len - i - seq_len);
        l->queued_input_len -= seq_len;
        if (l->queued_input_pos > l->queued_input_len)
            l->queued_input_pos = l->queued_input_len;
        return true;
    }
    return false;
}

typedef struct {
    const char *seq;
    size_t len;
} agent_key_sequence;

#define AGENT_KEY_SEQ(s) {s, sizeof(s) - 1}
static const agent_key_sequence agent_alt_tab_sequences[] = {
    AGENT_KEY_SEQ("\x1b\t"),           /* Meta/Alt+Tab */
    AGENT_KEY_SEQ("\x1b[Z"),           /* Backtab-style modified Tab */
    AGENT_KEY_SEQ("\x1b[1;3Z"),        /* CSI modifier form */
    AGENT_KEY_SEQ("\x1b[9;3u"),        /* kitty keyboard protocol */
    AGENT_KEY_SEQ("\x1b[27;3;9~"),     /* xterm modifyOtherKeys */
};
#undef AGENT_KEY_SEQ

static bool linenoise_take_queued_alt_tab(struct linenoiseState *l) {
    for (size_t i = 0; i < sizeof(agent_alt_tab_sequences) /
                           sizeof(agent_alt_tab_sequences[0]); i++) {
        if (linenoise_take_queued_sequence(l, agent_alt_tab_sequences[i].seq,
                                           agent_alt_tab_sequences[i].len))
            return true;
    }
    return false;
}

static bool linenoise_queued_alt_tab_prefix_pending(struct linenoiseState *l) {
    if (!l || l->queued_input_pos >= l->queued_input_len) return false;
    for (size_t pos = l->queued_input_pos; pos < l->queued_input_len; pos++) {
        size_t rem = l->queued_input_len - pos;
        for (size_t i = 0; i < sizeof(agent_alt_tab_sequences) /
                               sizeof(agent_alt_tab_sequences[0]); i++) {
            const agent_key_sequence *seq = &agent_alt_tab_sequences[i];
            if (rem >= seq->len) continue;
            if (!memcmp(l->queued_input + pos, seq->seq, rem))
                return true;
        }
    }
    return false;
}

static bool editor_take_alt_tab(agent_editor *ed) {
    bool consumed = linenoise_take_queued_alt_tab(&ed->edit);
    if (consumed) ed->alt_tab_prefix_since = 0.0;
    return consumed;
}

static bool editor_alt_tab_prefix_waiting(agent_editor *ed, double now) {
    const double timeout_sec = 0.050;
    if (!linenoise_queued_alt_tab_prefix_pending(&ed->edit)) {
        ed->alt_tab_prefix_since = 0.0;
        return false;
    }
    if (ed->alt_tab_prefix_since <= 0.0)
        ed->alt_tab_prefix_since = now;
    return now - ed->alt_tab_prefix_since < timeout_sec;
}

static bool editor_take_bare_escape(agent_editor *ed) {
    if (ed->cpr_len == 1 && (unsigned char)ed->cpr_buf[0] == 0x1b) {
        ed->cpr_len = 0;
        return true;
    }
    return false;
}

static void editor_replace_input(agent_editor *ed, const char *text) {
    if (ed->hidden) editor_show(ed);
    linenoiseEditClear(&ed->edit);
    if (text && text[0]) linenoiseEditInsert(&ed->edit, text, strlen(text));
}

/* Fallback cursor tracking for terminals that do not answer CPR quickly.  It is
 * intentionally approximate for wide Unicode; the CPR path handles exact
 * positioning in normal interactive terminals. */
static void editor_note_output(agent_editor *ed, const char *text, size_t len) {
    int cols = ed->edit.cols > 0 ? (int)ed->edit.cols : 80;
    for (size_t i = 0; i < len; i++) {
        size_t start = i;
        unsigned char c = (unsigned char)text[i];
        if (c == 0x1b && i + 1 < len && text[i + 1] == '[') {
            (void)start;
            i += 2;
            while (i < len) {
                unsigned char e = (unsigned char)text[i];
                if (e >= 0x40 && e <= 0x7e) break;
                i++;
            }
            continue;
        }
        if (c == '\n') {
            ed->output_col = 0;
            ed->output_line_open = false;
            continue;
        }
        if (c == '\r') {
            ed->output_col = 0;
            continue;
        }
        if (c == '\b') {
            if (ed->output_col > 0) ed->output_col--;
            continue;
        }

        int width = 1;
        if (c == '\t') {
            width = 8 - (ed->output_col & 7);
        } else if (c < 0x20 || c == 0x7f) {
            width = 0;
        } else if (c >= 0xc0) {
            while (i + 1 < len && (((unsigned char)text[i + 1]) & 0xc0) == 0x80)
                i++;
        } else if ((c & 0xc0) == 0x80) {
            width = 0;
        }

        if (width > 0) {
            ed->output_col = (ed->output_col + width) % cols;
            ed->output_line_open = true;
        }
    }
}

/* Normalize generated LF to CRLF for terminal output without changing the text
 * stored in the transcript. */
static void editor_write_terminal_text(const char *text, size_t len) {
    size_t start = 0;
    for (size_t i = 0; i < len; i++) {
        if (text[i] != '\n') continue;
        if (i > start) write_all(STDOUT_FILENO, text + start, i - start);
        write_all(STDOUT_FILENO, "\r\n", 2);
        start = i + 1;
    }
    if (start < len) write_all(STDOUT_FILENO, text + start, len - start);
}

/* Locate a CPR reply inside a mixed stdin buffer.  Bytes before/after the reply
 * are user input and must be queued back into linenoise. */
static bool find_cpr_reply(const char *buf, size_t len, size_t *start, size_t *end,
                           int *row, int *col) {
    for (size_t i = 0; i + 5 < len; i++) {
        if ((unsigned char)buf[i] != 0x1b || buf[i + 1] != '[') continue;
        size_t p = i + 2;
        int r = 0, c = 0;
        if (p >= len || buf[p] < '0' || buf[p] > '9') continue;
        while (p < len && buf[p] >= '0' && buf[p] <= '9') {
            r = r * 10 + (buf[p++] - '0');
        }
        if (p >= len || buf[p++] != ';') continue;
        if (p >= len || buf[p] < '0' || buf[p] > '9') continue;
        while (p < len && buf[p] >= '0' && buf[p] <= '9') {
            c = c * 10 + (buf[p++] - '0');
        }
        if (p >= len || buf[p] != 'R') continue;
        *start = i;
        *end = p + 1;
        *row = r;
        *col = c;
        return true;
    }
    return false;
}

/* Ask the terminal for the cursor column after writing model output.  Any user
 * bytes read while waiting for the CPR reply are queued back into linenoise so
 * typing during generation is not lost. */
static bool editor_query_cursor(agent_editor *ed, int *col_out) {
    if (!isatty(STDIN_FILENO) || !isatty(STDOUT_FILENO)) return false;

    char buf[512];
    size_t len = 0, start = 0, end = 0;
    int row = 0, col = 0;
    write_all(STDOUT_FILENO, "\x1b[6n", 4);

    for (int attempt = 0; attempt < 8; attempt++) {
        struct pollfd pfd = {.fd = STDIN_FILENO, .events = POLLIN};
        int rc = poll(&pfd, 1, 5);
        if (rc < 0 && errno == EINTR) continue;
        if (rc <= 0) continue;
        for (;;) {
            ssize_t n = read(STDIN_FILENO, buf + len, sizeof(buf) - len);
            if (n > 0) {
                len += (size_t)n;
                if (find_cpr_reply(buf, len, &start, &end, &row, &col)) {
                    if (start) editor_queue_bytes(ed, buf, start);
                    if (end < len) editor_queue_bytes(ed, buf + end, len - end);
                    (void)row;
                    *col_out = col;
                    return col > 0;
                }
                if (len == sizeof(buf)) break;
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            break;
        }
    }

    if (len) editor_queue_bytes(ed, buf, len);
    return false;
}

static void editor_move_to_output_cursor(agent_editor *ed) {
    char seq[64];
    write_all(STDOUT_FILENO, "\x1b[1A", 4);
    int n = snprintf(seq, sizeof(seq), "\x1b[%dG", ed->output_col + 1);
    if (n > 0) write_all(STDOUT_FILENO, seq, (size_t)n);
}

static bool editor_get_terminal_size(int *rows, int *cols) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) != 0) return false;
    if (ws.ws_row < 1 || ws.ws_col < 1) return false;
    *rows = ws.ws_row;
    *cols = ws.ws_col;
    return true;
}

static void editor_csi_cursor(int row, int col) {
    char seq[64];
    int n = snprintf(seq, sizeof(seq), "\x1b[%d;%dH", row, col);
    if (n > 0) write_all(STDOUT_FILENO, seq, (size_t)n);
}

static void editor_save_output_cursor(agent_editor *ed) {
    if (!ed->scroll_region) return;
    write_all(STDOUT_FILENO, "\0337", 2);
    ed->output_cursor_saved = true;
}

static void editor_restore_output_cursor(agent_editor *ed) {
    if (!ed->scroll_region) return;
    if (ed->output_cursor_saved) {
        write_all(STDOUT_FILENO, "\0338", 2);
    } else {
        editor_csi_cursor(ed->output_bottom, 1);
    }
}

static void editor_move_to_prompt_row(agent_editor *ed) {
    if (!ed->scroll_region) return;
    editor_csi_cursor(ed->prompt_row, 1);
}

static void editor_move_to_prompt_cursor(agent_editor *ed) {
    if (!ed->scroll_region) return;
    if (ed->edit.screen_cursor_row > 0 && ed->edit.screen_cursor_col > 0) {
        editor_csi_cursor(ed->edit.screen_cursor_row, ed->edit.screen_cursor_col);
    } else {
        editor_move_to_prompt_row(ed);
    }
}

static void editor_clear_row(int row) {
    editor_csi_cursor(row, 1);
    write_all(STDOUT_FILENO, "\r\x1b[0K", 5);
}

static void editor_clear_prompt_region(agent_editor *ed) {
    if (!ed->scroll_region) return;
    for (int row = ed->prompt_row; row <= ed->term_rows; row++)
        editor_clear_row(row);

    /* In scroll-region mode ds4-agent owns the absolute prompt/status rows.
     * Clearing them directly is more reliable than asking linenoise to clean
     * relative to whatever cursor position the last worker/status transition
     * left behind.  Reset linenoise's render bookkeeping so the next show is a
     * pure write into the reserved rows. */
    ed->edit.oldrows = 0;
    ed->edit.oldstatusrows = 0;
    ed->edit.oldrpos = 1;
    ed->edit.oldpos = ed->edit.pos;
}

static void editor_set_scroll_margin(int bottom) {
    char seq[96];
    int n = snprintf(seq, sizeof(seq), "\x1b[1;%dr", bottom);
    if (n > 0) write_all(STDOUT_FILENO, seq, (size_t)n);
}

static void editor_scroll_output_up(int bottom, int lines) {
    if (lines <= 0) return;
    editor_set_scroll_margin(bottom);
    editor_csi_cursor(bottom, 1);
    for (int i = 0; i < lines; i++)
        write_all(STDOUT_FILENO, "\n", 1);
}

static bool editor_set_scroll_layout(agent_editor *ed, int reserved_rows,
                                     bool allow_shrink,
                                     bool scroll_on_grow) {
    if (!ed->scroll_region) return false;

    int rows = 0, cols = 0;
    if (!editor_get_terminal_size(&rows, &cols)) return false;
    if (rows < 8 || cols < 20) return false;
    if (reserved_rows < 2) reserved_rows = 2;
    if (reserved_rows > rows - 2) reserved_rows = rows - 2;
    if (!allow_shrink && ed->reserved_rows > 0 &&
        ed->term_rows == rows && ed->term_cols == cols &&
        reserved_rows < ed->reserved_rows)
    {
        reserved_rows = ed->reserved_rows;
    }

    int output_bottom = rows - reserved_rows;
    int prompt_row = output_bottom + 1;
    bool changed = ed->term_rows != rows ||
                   ed->term_cols != cols ||
                   ed->output_bottom != output_bottom ||
                   ed->prompt_row != prompt_row ||
                   ed->reserved_rows != reserved_rows;
    if (!changed) return true;

    /* If the prompt grows, rows that were output rows become prompt rows.  Do
     * not simply clear them: first scroll the old output region upward by the
     * number of newly reserved rows, exactly as if the model had printed more
     * lines.  If the prompt shrinks, no output is restored; the output region
     * simply grows downward and the prompt/status block remains bottom
     * anchored. */
    bool scrolled_output = false;
    if (scroll_on_grow &&
        ed->term_rows == rows && ed->term_cols == cols &&
        ed->output_bottom > 0 && output_bottom < ed->output_bottom)
    {
        editor_scroll_output_up(ed->output_bottom,
                                ed->output_bottom - output_bottom);
        scrolled_output = true;
    }

    editor_set_scroll_margin(output_bottom);

    ed->term_rows = rows;
    ed->term_cols = cols;
    ed->output_bottom = output_bottom;
    ed->prompt_row = prompt_row;
    ed->reserved_rows = reserved_rows;
    ed->output_cursor_saved = false;
    ed->output_at_scroll_boundary = scrolled_output;

    for (int row = prompt_row; row <= rows; row++)
        editor_clear_row(row);

    /* If the prompt grew while generated output was in the middle of a line,
     * the scroll above moved that partial line up with its column intact.
     * Preserve that column when saving the new output cursor; otherwise the
     * next token resumes at column 1 and overwrites the line it was extending. */
    int output_col = ed->output_line_open ? ed->output_col + 1 : 1;
    if (output_col < 1) output_col = 1;
    if (output_col > cols) output_col = cols;
    editor_csi_cursor(output_bottom, output_col);
    editor_save_output_cursor(ed);
    editor_move_to_prompt_row(ed);
    return true;
}

static int editor_linenoise_layout_changed(struct linenoiseState *l,
                                           size_t prompt_rows,
                                           size_t status_rows,
                                           void *privdata) {
    (void)l;
    agent_editor *ed = privdata;
    if (!ed || !ed->scroll_region) return 0;
    if (prompt_rows < 1) prompt_rows = 1;
    int reserved = (int)(prompt_rows + status_rows);
    if (!editor_set_scroll_layout(ed, reserved, true, true)) return 0;
    return ed->prompt_row;
}

/* Keep generated output inside a scroll region that excludes the live prompt
 * and status footer.  This lets terminals scroll model/tool output naturally
 * without rewriting the prompt on every streamed token, which is especially
 * important over SSH where full redraws are visibly expensive. */
static bool editor_configure_scroll_region(agent_editor *ed) {
    if (ed->scroll_region) return true;
    if (!isatty(STDIN_FILENO) || !isatty(STDOUT_FILENO)) return false;

    int rows = 0, cols = 0;
    if (!editor_get_terminal_size(&rows, &cols)) return false;
    if (rows < 8 || cols < 20) return false;

    ed->term_rows = 0;
    ed->term_cols = 0;
    ed->output_bottom = 0;
    ed->prompt_row = 0;
    ed->reserved_rows = 0;
    ed->output_cursor_saved = false;
    ed->output_at_scroll_boundary = false;
    ed->scroll_region = true;
    if (!editor_set_scroll_layout(ed, 2, true, false)) return false;

    /* The agent prints backend startup lines before the editor exists.  Once
     * the scroll region is installed, create an append line at the bottom of
     * that region instead of guessing that the old terminal cursor was already
     * there.  Without this first scroll, the first agent/model output can
     * overwrite the last visible startup line. */
    editor_scroll_output_up(ed->output_bottom, 1);
    ed->output_cursor_saved = false;
    editor_csi_cursor(ed->output_bottom, 1);
    editor_save_output_cursor(ed);
    editor_move_to_prompt_row(ed);
    return true;
}

static void editor_restore_terminal_layout(agent_editor *ed) {
    if (!ed->scroll_region) return;
    write_all(STDOUT_FILENO, "\x1b[0m", 4);
    write_all(STDOUT_FILENO, "\x1b[r", 3);
    editor_csi_cursor(ed->term_rows, 1);
    write_all(STDOUT_FILENO, "\r\x1b[0K\r\n", 7);
    ed->scroll_region = false;
    ed->output_cursor_saved = false;
    ed->term_rows = ed->term_cols = 0;
    ed->output_bottom = ed->prompt_row = 0;
    ed->reserved_rows = 0;
    ed->output_at_scroll_boundary = false;
}

/* Start linenoise in nonblocking mode and install the status footer. */
static int editor_start(agent_editor *ed, const char *prompt,
                        const char *status, const char *initial) {
    char *input = xmalloc(AGENT_INPUT_INITIAL_BUFLEN);
    snprintf(ed->prompt, sizeof(ed->prompt), "%s", prompt);
    snprintf(ed->status, sizeof(ed->status), "%s", status ? status : "");
    bool had_scroll_region = ed->scroll_region;
    bool use_scroll_region = editor_configure_scroll_region(ed);
    if (use_scroll_region) {
        if (had_scroll_region)
            editor_set_scroll_layout(ed, 2, true, false);
        editor_move_to_prompt_row(ed);
    }
    if (linenoiseEditStart(&ed->edit, STDIN_FILENO, STDOUT_FILENO,
                           input, AGENT_INPUT_INITIAL_BUFLEN, ed->prompt) != 0)
    {
        editor_restore_terminal_layout(ed);
        free(input);
        return -1;
    }
    const char *status_start = stdout_is_tty() && ed->status[0] ?
        AGENT_STATUS_STYLE_START : "";
    const char *status_end = stdout_is_tty() && ed->status[0] ?
        AGENT_STATUS_STYLE_END : "";
    linenoiseEditSetStatus(&ed->edit, ed->status,
                           status_start, status_end);
    linenoiseEditSetLayoutCallback(&ed->edit, editor_linenoise_layout_changed, ed);
    if (isatty(ed->edit.ifd) || getenv("LINENOISE_ASSUME_TTY")) {
        linenoiseHide(&ed->edit);
        linenoiseShow(&ed->edit);
    }
    ed->input = input;
    ed->edit.buflen_max = AGENT_INPUT_MAX_BUFLEN;
    ed->active = true;
    if (set_nonblock(STDIN_FILENO, true, &ed->old_stdin_flags) != 0)
        ed->old_stdin_flags = -1;
    if (initial && initial[0]) linenoiseEditInsert(&ed->edit, initial, strlen(initial));
    ed->hidden = false;
    ed->output_line_open = false;
    ed->prompt_below_output = false;
    ed->output_col = 0;
    ed->cpr_len = 0;
    ed->paste_open = false;
    ed->paste_start_pending = false;
    ed->paste_tail_len = 0;
    ed->alt_tab_prefix_since = 0.0;
    return 0;
}

/* Stop the live editor and restore stdin flags. */
static void editor_stop(agent_editor *ed) {
    if (!ed->active) return;
    /* ds4-agent treats linenoise as a live input widget, not as persistent
     * command scrollback.  Clear it before shutdown so submitting a line and
     * immediately reopening the editor does not leave the accepted
     * prompt+input duplicated above the fresh prompt. */
    if (!ed->hidden && (isatty(ed->edit.ifd) || getenv("LINENOISE_ASSUME_TTY")))
        editor_hide(ed);
    linenoiseEditStop(&ed->edit);
    if (ed->old_stdin_flags >= 0) fcntl(STDIN_FILENO, F_SETFL, ed->old_stdin_flags);
    free(ed->edit.buf);
    ed->input = NULL;
    ed->active = false;
    ed->hidden = false;
    ed->output_line_open = false;
    ed->prompt_below_output = false;
    ed->output_col = 0;
    ed->cpr_len = 0;
    ed->paste_open = false;
    ed->paste_start_pending = false;
    ed->paste_tail_len = 0;
}

/* Hide the live prompt before model output is written.  In scroll-region mode
 * the output cursor was saved before the prompt was drawn, so restoring it is
 * enough to append more model/tool bytes without touching the prompt rows. */
static void editor_hide(agent_editor *ed) {
    if (!ed->active || ed->hidden) return;
    if (ed->scroll_region) {
        editor_clear_prompt_region(ed);
        editor_restore_output_cursor(ed);
        ed->hidden = true;
        return;
    }
    linenoiseHide(&ed->edit);
    if (ed->prompt_below_output) {
        editor_move_to_output_cursor(ed);
        ed->prompt_below_output = false;
    }
    ed->hidden = true;
}

/* Restore the live prompt after output.  The primary path draws it in the
 * reserved bottom rows; the fallback path keeps the older one-row-below-output
 * trick for terminals where scroll regions are unavailable. */
static void editor_show(agent_editor *ed) {
    if (!ed->active || !ed->hidden) return;
    if (ed->scroll_region) {
        editor_save_output_cursor(ed);
        editor_move_to_prompt_row(ed);
        write_all(STDOUT_FILENO, "\x1b[0m", 4);
        linenoiseShow(&ed->edit);
        ed->hidden = false;
        return;
    }
    if (ed->output_line_open) {
        write_all(STDOUT_FILENO, "\r\n", 2);
        ed->prompt_below_output = true;
    } else {
        ed->prompt_below_output = false;
    }
    /* Model/tool output can leave SGR attributes active while it streams.
     * Redrawing linenoise always starts from normal attributes; tool rendering
     * re-emits its own color on the next streamed byte if it is still inside a
     * colored parameter. */
    write_all(STDOUT_FILENO, "\x1b[0m", 4);
    linenoiseShow(&ed->edit);
    ed->hidden = false;
}

static void editor_update_prompt(agent_editor *ed, const char *prompt) {
    snprintf(ed->prompt, sizeof(ed->prompt), "%s", prompt);
    ed->edit.prompt = ed->prompt;
    ed->edit.plen = strlen(ed->prompt);
}

static void editor_update_status(agent_editor *ed, const char *status) {
    snprintf(ed->status, sizeof(ed->status), "%s", status ? status : "");
    const char *status_start = stdout_is_tty() && ed->status[0] ?
        AGENT_STATUS_STYLE_START : "";
    const char *status_end = stdout_is_tty() && ed->status[0] ?
        AGENT_STATUS_STYLE_END : "";
    linenoiseEditSetStatus(&ed->edit, ed->status,
                           status_start, status_end);
}

static void editor_redraw_visible_prompt(agent_editor *ed);
static bool editor_prompt_redraw_due(agent_editor *ed);

static void editor_set_prompt_status(agent_editor *ed, const char *prompt,
                                     const char *status) {
    bool prompt_changed = strcmp(ed->prompt, prompt) != 0;
    bool status_changed = strcmp(ed->status, status ? status : "") != 0;
    if (!ed->active || (!prompt_changed && !status_changed)) return;
    if (ed->hidden) {
        if (prompt_changed) editor_update_prompt(ed, prompt);
        if (status_changed) editor_update_status(ed, status);
        return;
    }
    /* Live metrics can change on every model-gate handoff.  Keep those
     * status-only updates bounded by the same redraw interval as streamed
     * output.  Leave ed->status unchanged while deferred so the next UI loop
     * still observes a pending change. */
    if (ed->scroll_region && !prompt_changed && status_changed &&
        !editor_prompt_redraw_due(ed))
        return;
    if (ed->scroll_region) {
        if (prompt_changed) editor_update_prompt(ed, prompt);
        if (status_changed) editor_update_status(ed, status);
        editor_redraw_visible_prompt(ed);
        return;
    }
    editor_hide(ed);
    if (prompt_changed) editor_update_prompt(ed, prompt);
    if (status_changed) editor_update_status(ed, status);
    editor_show(ed);
}

static void editor_redraw_visible_prompt(agent_editor *ed) {
    static const char sync_start[] = "\x1b[?2026h";
    static const char sync_end[] = "\x1b[?2026l";
    if (!ed->active || !ed->scroll_region) return;
    /* Synchronized-output start (DECSET 2026) prevents display tearing.
     * The end sequence is always written before returning — there are no
     * early returns after start.  If the process dies abnormally between
     * the two writes, the terminal emulator detects the dead writer and
     * resets private modes, so no lock can persist. */
    write_all(STDOUT_FILENO, sync_start, sizeof(sync_start) - 1);
    editor_clear_prompt_region(ed);
    editor_move_to_prompt_row(ed);
    write_all(STDOUT_FILENO, "\x1b[0m", 4);
    linenoiseShow(&ed->edit);
    write_all(STDOUT_FILENO, sync_end, sizeof(sync_end) - 1);
    ed->last_prompt_redraw_time = now_sec();
}

static bool editor_prompt_redraw_due(agent_editor *ed) {
    double now = now_sec();
    if (ed->last_prompt_redraw_time <= 0.0 ||
        now - ed->last_prompt_redraw_time >= AGENT_STATUS_REDRAW_INTERVAL_SEC)
    {
        return true;
    }
    return false;
}

static void editor_write_scroll_output_preserve_prompt(agent_editor *ed,
                                                       const char *text,
                                                       size_t len) {
    static const char sync_start[] = "\x1b[?2026h";
    static const char sync_end[] = "\x1b[?2026l";
    if (!len) return;

    write_all(STDOUT_FILENO, sync_start, sizeof(sync_start) - 1);
    editor_restore_output_cursor(ed);
    editor_write_terminal_text(text, len);
    editor_note_output(ed, text, len);
    editor_save_output_cursor(ed);
    write_all(STDOUT_FILENO, "\x1b[0m", 4);
    editor_move_to_prompt_cursor(ed);
    write_all(STDOUT_FILENO, sync_end, sizeof(sync_end) - 1);
    ed->output_at_scroll_boundary = true;
}

/* Serialize async model/tool output with linenoise.  This is the central
 * terminal contract.  In scroll-region mode the live prompt stays painted:
 * output is appended in the upper scroll area, then the cursor is returned to
 * linenoise's remembered prompt position.  The fallback path still hides and
 * redraws because it has no protected prompt rows. */
static void editor_write_async(agent_editor *ed, const char *text, size_t len,
                               const char *prompt, const char *status,
                               bool force_show) {
    if (ed->scroll_region && ed->active && !ed->hidden && len) {
        bool prompt_changed = strcmp(ed->prompt, prompt) != 0;
        bool status_changed = strcmp(ed->status, status ? status : "") != 0;

        editor_write_scroll_output_preserve_prompt(ed, text, len);
        if (prompt_changed) editor_update_prompt(ed, prompt);
        if (status_changed) editor_update_status(ed, status);
        if ((force_show || editor_prompt_redraw_due(ed)) &&
            (prompt_changed || status_changed))
        {
            editor_redraw_visible_prompt(ed);
        }
        return;
    }

    editor_hide(ed);
    if (len) {
        editor_write_terminal_text(text, len);
        if (ed->scroll_region) ed->output_at_scroll_boundary = true;
        if (!ed->scroll_region) {
            if (text[len - 1] == '\n' || text[len - 1] == '\r') {
                ed->output_col = 0;
                ed->output_line_open = false;
            } else {
                int col = 0;
                if (editor_query_cursor(ed, &col)) {
                    int cols = ed->edit.cols > 0 ? (int)ed->edit.cols : 80;
                    ed->output_col = col > 0 ? col - 1 : 0;
                    ed->output_line_open = true;
                    if (ed->output_col + 1 >= cols) {
                        write_all(STDOUT_FILENO, "\r\n", 2);
                        ed->output_col = 0;
                    }
                } else {
                    editor_note_output(ed, text, len);
                }
            }
        }
    }
    if (ed->active) {
        editor_update_prompt(ed, prompt);
        editor_update_status(ed, status);
        /* In scroll-region mode this saves the current output cursor and
         * redraws linenoise in the fixed prompt rows.  In fallback mode it may
         * put the prompt below an unfinished generated line. */
        if (force_show || len) editor_show(ed);
    }
}

/* Ctrl+C while idle is an edit-cancel key, not an exit key.  Clear the real
 * linenoise buffer so stale text cannot be submitted later, then leave a short
 * visible hint about the explicit EOF exit path. */
static void editor_cancel_input_with_hint(agent_editor *ed,
                                          const char *prompt,
                                          const char *status) {
    if (!ed->active) return;
    if (ed->hidden) editor_show(ed);
    linenoiseEditClear(&ed->edit);
    const char *msg = stdout_is_tty() ?
        "\x1b[1;33mpress Ctrl+D to exit\x1b[0m\n" :
        "press Ctrl+D to exit\n";
    editor_write_async(ed, msg, strlen(msg), prompt, status, true);
}

/* What: print the Docker-specific slash-command help entries.
 * Why: `/help` and `/docker help` should share one command list so docs stay in
 * sync with the interactive command parser.
 * Callers: runtime_help() and runtime_docker_help(). */
static void runtime_docker_help_body(void) {
    puts("  /docker help");
    puts("               Show Docker sandbox commands.");
    puts("  /docker debug");
    puts("               Toggle docker command debug logging.");
    puts("  /docker create IMAGE NAME [COMMAND]");
    puts("               Create a tagged Docker sandbox and switch to it.");
    puts("               Defaults to `sleep infinity` when COMMAND is omitted.");
    puts("  /docker use [NAME]");
    puts("               Show the current Docker sandbox or switch to NAME.");
    puts("  /docker describe NAME");
    puts("               Show detailed metadata for a tagged Docker sandbox.");
    puts("  /docker destroy NAME");
    puts("               Destroy a stopped, inactive Docker sandbox after confirmation.");
    puts("  /docker stop [NAME]");
    puts("               Stop one tagged Docker sandbox or all agent sandboxes.");
    puts("  /docker list List Docker containers tagged ds4:sandbox.");
}

static void runtime_help(void) {
    puts("Commands:");
    puts("  /help        Show this help.");
    puts("  /save        Save the current session.");
    puts("  /compact     Compact the current session context now.");
    puts("  /list        List saved sessions.");
    puts("  /strict_sandbox");
    puts("               Require an active Docker sandbox before any tool runs.");
    puts("  /no_strict_sandbox");
    puts("               Allow tools to run without an active Docker sandbox.");
    puts("  /command_output on|off");
    puts("               Show or hide command output mirrored in the terminal.");
    puts("  /preserve_agent_files");
    puts("               Preserve agent temp files instead of idle cleanup.");
    puts("  /no_preserve_agent_files");
    puts("               Delete agent temp files after 5 idle minutes (default).");
    puts("  /switch SHA  Load a saved session and show recent history.");
    puts("  /del SHA     Delete a saved session.");
    puts("  /strip SHA   Strip KV payload; /switch rebuilds it by prefill.");
    puts("  /history [N] Show N recent user turns from the current session.");
    puts("  /power N     Set GPU duty cycle percentage, 1..100.");
    puts("  /thinking off|default|max");
    puts("               Set thinking effort level.");
    puts("  /workspace   List workspace roots; +PATH adds, -PATH removes. The first root is the active workspace.");
    puts("  /subagent    Manage resident subagents: new, list, switch, send, stop, close, report, import.");
    puts("  /allow <tools>");
    puts("               Grant tool access to the active session (e.g. /allow read,bash).");
    puts("  /disallow <tools>");
    puts("               Revoke tool access from the active session (e.g. /disallow web or /disallow all).");
    puts("  /purge_auto_files");
    puts("               Delete tracked temp files. Lists files, gives 5s to abort.");
    puts("  /new         Start a fresh session from the system prompt.");
    puts("  /quit, /exit Exit.");
    puts("  Ctrl+C       Interrupt generation; clear edited text.");
    puts("  Enter        Queue text while the agent is busy.");
    puts("  Ctrl+X       Edit the first queued prompt.");
    puts("  ESC          Interrupt and send queued prompt immediately.");
    puts("  Ctrl+D       Exit from an empty prompt.");
}

/* What: print a standalone Docker command help screen.
 * Why: `/docker help` should show only sandbox management commands.
 * Callers: run_agent() slash-command dispatch. */
static void runtime_docker_help(void) {
    puts("Commands:");
    runtime_docker_help_body();
}

void agent_format_ctx_size(int ctx_size, char *buf, size_t len) {
    if (ctx_size >= 1000) {
        if (ctx_size % 1000 == 0) snprintf(buf, len, "%dk", ctx_size / 1000);
        else snprintf(buf, len, "%.1fk", (double)ctx_size / 1000.0);
    } else {
        snprintf(buf, len, "%d", ctx_size);
    }
}

static void agent_format_welcome_banner(const agent_config *cfg,
                                        char *buf, size_t len) {
    char ctx[32];
    agent_format_ctx_size(cfg->gen.ctx_size, ctx, sizeof(ctx));
    if (stdout_is_tty()) {
        snprintf(buf, len,
                 "\x1b[1;97mDwarf\x1b[1;94mStar\x1b[0m 🐋 Agent, context %s tokens\n\n",
                 ctx);
    } else {
        snprintf(buf, len, "DwarfStar Agent, context %s tokens\n\n", ctx);
    }
}

static void editor_write_welcome_banner(agent_editor *editor,
                                        const agent_config *cfg,
                                        const char *prompt,
                                        const char *statusline) {
    char banner[256];
    agent_format_welcome_banner(cfg, banner, sizeof(banner));
    editor_write_async(editor, banner, strlen(banner), prompt, statusline, true);
}

/* What: capture output for a full Docker argv whose first element is the Docker
 * command path.
 * Why: Docker management helpers naturally build full argv arrays, while
 * agent_docker_exec() prepends the command from config and expects argv_tail.
 * Callers: Docker inspect/list helpers, mount refresh, docker use/start,
 * docker destroy, and its wrapper unit tests. */
static bool agent_docker_capture(const agent_config *cfg,
                                 const char *docker_command, char *const argv[],
                                 const char *op, agent_buf *out) {
    if (!cfg || !docker_command || !argv || !argv[0] || !op || !out) return false;
    agent_config tmp_cfg = *cfg;
    tmp_cfg.docker_command = docker_command;
    /* argv_tail starts at argv[1] (skip docker_command which exec prepends) */
    const char *const *argv_tail = (const char *const *)(argv + 1);
    return agent_docker_exec(NULL, &tmp_cfg, argv_tail, NULL, 0, false, op, out, NULL);
}

/* What: inspect one Docker container and emit tab-separated sandbox metadata.
 * Why: list/use/describe/destroy all need the same labels, image, state, and IP
 * fields, and Docker's Go template avoids ad hoc JSON parsing here.
 * Callers: agent_command_docker_list(), agent_command_docker_use(),
 * agent_command_docker_describe(), and agent_command_docker_destroy(). */
static bool agent_docker_inspect_sandbox(const agent_config *cfg,
                                         const char *docker_command,
                                         const char *name,
                                         agent_buf *out) {
    char *argv[] = {
        (char *)docker_command,
        "inspect",
        "--type", "container",
        "--format",
        "{{.Name}}\t{{json .Config.Labels}}\t{{.Config.Image}}\t{{.State.Status}}\t{{range $k,$v := .NetworkSettings.Networks}}{{if $v.IPAddress}}{{$v.IPAddress}} {{end}}{{end}}",
        (char *)(name ? name : ""),
        NULL,
    };
    return agent_docker_capture(cfg, docker_command, argv, "docker inspect", out);
}

/* What: inspect multiple sandbox containers and emit the fields needed to
 * recreate them with updated mounts.
 * Why: changing workspace/temp mounts requires rebuilding each sandbox with
 * its original image, entrypoint, and args.
 * Callers: agent_docker_refresh_mounts(). */
static bool agent_docker_inspect_sandboxes_sync(const agent_config *cfg,
                                                const char *docker_command,
                                                char **names,
                                                int name_count,
                                                agent_buf *out) {
    if (!names || name_count <= 0) return false;
    int argc = 0;
    int argv_cap = 7 + name_count;
    char **argv = xmalloc((size_t)argv_cap * sizeof(char *));
    argv[argc++] = (char *)docker_command;
    argv[argc++] = "inspect";
    argv[argc++] = "--type";
    argv[argc++] = "container";
    argv[argc++] = "--format";
    argv[argc++] =
        "{{.Name}}\t{{.Config.Image}}\t{{.State.Status}}\t{{.Path}}\t{{json .Args}}";
    for (int i = 0; i < name_count; i++) argv[argc++] = names[i];
    argv[argc] = NULL;
    bool ok = agent_docker_capture(cfg, docker_command, argv, "docker inspect", out);
    free(argv);
    return ok;
}

/* What: list Docker container names tagged with the DS4 sandbox label.
 * Why: all sandbox management commands should ignore unrelated user containers.
 * Callers: agent_config_autoload_first_docker_sandbox(),
 * agent_docker_refresh_mounts(), agent_command_docker_list(), and
 * agent_command_docker_stop(). */
static bool agent_docker_list_sandbox_names(const agent_config *cfg,
                                            const char *docker_command,
                                            agent_buf *out) {
    char *argv[] = {
        (char *)docker_command,
        "ps", "-a",
        "--filter", "label=ds4:sandbox",
        "--format", "{{.Names}}",
        NULL,
    };
    return agent_docker_capture(cfg, docker_command, argv, "docker list", out);
}

/* What: check whether Docker support is enabled in config.
 * Why: command handlers and mount refresh should no-op cleanly when the binary
 * was started without usable Docker support.
 * Callers: agent_config_autoload_first_docker_sandbox(),
 * agent_docker_feature_available_worker(), and agent_docker_refresh_mounts(). */
static bool agent_docker_feature_available_cfg(const agent_config *cfg) {
    return cfg && cfg->docker_available;
}

static bool agent_docker_parse_sandbox_row(char *line,
                                           char **container_name,
                                           char **labels_json,
                                           char **image,
                                           char **state,
                                           char **ip) {
    if (container_name) *container_name = NULL;
    if (labels_json) *labels_json = NULL;
    if (image) *image = NULL;
    if (state) *state = NULL;
    if (ip) *ip = NULL;
    if (!line) return false;

    char *name = line;
    char *newline = strchr(name, '\n');
    if (newline) *newline = '\0';
    if (name[0] == '/') name++;
    char *labels = strchr(name, '\t');
    if (!labels) return false;
    *labels++ = '\0';
    char *img = strchr(labels, '\t');
    if (!img) return false;
    *img++ = '\0';
    char *st = strchr(img, '\t');
    if (!st) return false;
    *st++ = '\0';
    char *ip_text = strchr(st, '\t');
    if (!ip_text) return false;
    *ip_text++ = '\0';
    size_t ip_len = strlen(ip_text);
    while (ip_len > 0 && isspace((unsigned char)ip_text[ip_len - 1]))
        ip_text[--ip_len] = '\0';

    if (container_name) *container_name = name;
    if (labels_json) *labels_json = labels;
    if (image) *image = img;
    if (state) *state = st;
    if (ip) *ip = ip_text;
    return true;
}

static bool agent_docker_activate_named_sandbox(agent_worker *w,
                                                agent_config *cfg,
                                                const char *name,
                                                bool print_success,
                                                char *err, size_t err_len) {
    if (err && err_len) err[0] = '\0';
    if (!cfg || !name || !name[0]) {
        if (err && err_len) snprintf(err, err_len, "missing sandbox name");
        return false;
    }
    const char *docker_command =
        (cfg->docker_command && cfg->docker_command[0]) ?
        cfg->docker_command : "docker";
    agent_buf out = {0};
    if (!agent_docker_inspect_sandbox(cfg, docker_command, name, &out)) {
        snprintf(err, err_len, "unable to inspect sandbox: %s", name);
        return false;
    }

    char *line = out.ptr ? out.ptr : "";
    char *container_name = NULL, *labels = NULL, *image = NULL, *state = NULL, *ip = NULL;
    if (!agent_docker_parse_sandbox_row(line, &container_name, &labels, &image,
                                        &state, &ip)) {
        snprintf(err, err_len, "malformed docker inspect output");
        free(out.ptr);
        return false;
    }
    if (!strstr(labels, "\"ds4:sandbox\"")) {
        snprintf(err, err_len, "container is not tagged ds4:sandbox: %s", name);
        free(out.ptr);
        return false;
    }

    if (strcmp(state, "running") != 0) {
        char *start_argv[] = {
            (char *)docker_command,
            "start",
            (char *)name,
            NULL,
        };
        agent_buf start_out = {0};
        if (!agent_docker_capture(cfg, docker_command, start_argv, "docker start",
                                  &start_out)) {
            snprintf(err, err_len, "unable to start sandbox: %s", name);
            free(start_out.ptr);
            free(out.ptr);
            return false;
        }
        free(start_out.ptr);
        free(out.ptr);
        memset(&out, 0, sizeof(out));
        if (!agent_docker_inspect_sandbox(cfg, docker_command, name, &out)) {
            snprintf(err, err_len, "unable to inspect restarted sandbox: %s", name);
            return false;
        }
        line = out.ptr ? out.ptr : "";
        if (!agent_docker_parse_sandbox_row(line, &container_name, &labels,
                                            &image, &state, &ip)) {
            snprintf(err, err_len, "malformed docker inspect output");
            free(out.ptr);
            return false;
        }
    }

    if (strcmp(state, "running") != 0) {
        snprintf(err, err_len, "sandbox did not enter running state: %s", name);
        free(out.ptr);
        return false;
    }

    if (w) agent_docker_shell_stop(w);
    cfg->docker_container = xstrdup(container_name);
    if (print_success) {
        printf("docker sandbox switched to %s (%s, ip=%s)\n",
               container_name, state[0] ? state : "unknown", ip[0] ? ip : "-");
    }
    free(out.ptr);

    if (w && !agent_docker_shell_start(w)) {
        printf("warning: failed to start persistent shell for sandbox %s\n",
               cfg->docker_container ? cfg->docker_container : name);
    }
    return true;
}

static bool agent_prompt_startup_docker_sandbox(agent_config *cfg,
                                                const char *docker_command,
                                                FILE *input,
                                                FILE *output,
                                                char *choice,
                                                size_t choice_len) {
    if (choice && choice_len > 0) choice[0] = '\0';
    agent_buf out = {0};
    if (!agent_docker_list_sandbox_names(cfg, docker_command, &out)) return false;
    if (!out.ptr || !out.ptr[0]) {
        free(out.ptr);
        return false;
    }

    char options[32][PATH_MAX];
    char states[32][32];
    int count = 0;
    char *save = NULL;
    for (char *line = strtok_r(out.ptr, "\n", &save);
         line && count < 32;
         line = strtok_r(NULL, "\n", &save)) {
        if (!line[0]) continue;
        snprintf(options[count], sizeof(options[count]), "%s", line);
        snprintf(states[count], sizeof(states[count]), "%s", "unknown");
        agent_buf inspect = {0};
        if (agent_docker_inspect_sandbox(cfg, docker_command, line, &inspect)) {
            char *name = NULL, *labels = NULL, *image = NULL, *state = NULL, *ip = NULL;
            if (agent_docker_parse_sandbox_row(inspect.ptr ? inspect.ptr : "",
                                               &name, &labels, &image, &state, &ip) &&
                state && state[0]) {
                snprintf(states[count], sizeof(states[count]), "%s", state);
            }
        }
        free(inspect.ptr);
        count++;
    }
    free(out.ptr);
    if (count <= 0) return false;

    FILE *out_stream = output ? output : stdout;
    fprintf(out_stream, "Select docker sandbox for this launch:\n");
    for (int i = 0; i < count; i++)
        fprintf(out_stream, "  %d) %s (%s)\n", i + 1, options[i], states[i]);
    fprintf(out_stream, "  n) none for this launch\n");
    fflush(out_stream);

    char buf[32];
    for (;;) {
        fprintf(out_stream, "Choose 1-%d or n/skip: ", count);
        fflush(out_stream);
        FILE *in = input ? input : stdin;
        int input_fd = input ? fileno(input) : STDIN_FILENO;
        int saved_flags = fcntl(input_fd, F_GETFL, 0);
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(input_fd, F_SETFL, saved_flags & ~O_NONBLOCK);
        bool got_line = fgets(buf, sizeof(buf), in) != NULL;
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(input_fd, F_SETFL, saved_flags);
        if (!got_line) return false;
        char *p = buf;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == 'n' || *p == 'N') return false;
        if (!strncasecmp(p, "skip", 4) || !strncasecmp(p, "none", 4))
            return false;
        /* Parse a full numeric token: require the first character to be a
         * digit, consume contiguous digits, then reject if the next
         * non-whitespace character is not end-of-string.  Accept only
         * values in [1, count]. */
        if (*p >= '1' && *p <= '9') {
            char *end = p;
            long val = 0;
            while (*end >= '0' && *end <= '9') {
                val = val * 10 + (*end - '0');
                end++;
            }
            /* Skip trailing whitespace */
            while (*end == ' ' || *end == '\t') end++;
            /* Reject if any non-whitespace remains (e.g. "10abc") */
            if (*end != '\0' && *end != '\n') {
                continue;
            }
            int idx = (int)val - 1;
            if (idx >= 0 && idx < count) {
                if (choice && choice_len > 0)
                    snprintf(choice, choice_len, "%s", options[idx]);
                return true;
            }
        }
    }
}

/* What: prepare the startup sandbox selection/activation state before worker
 * initialization.
 * Why: startup should only mark a sandbox active after the chosen or configured
 * container has been validated and is running.
 * Callers: main(); startup tests exercise prompt, skip, stopped-start, and
 * failure behavior through this helper. */
static void agent_config_prepare_startup_docker_sandbox_with_streams(agent_config *cfg,
                                                                     FILE *input,
                                                                     FILE *output) {
    if (!cfg) return;
    if (!agent_docker_feature_available_cfg(cfg)) return;
    const char *docker_command =
        (cfg->docker_command && cfg->docker_command[0]) ?
        cfg->docker_command : "docker";
    char chosen[PATH_MAX] = {0};
    const char *target = NULL;

    if (cfg->docker_container && cfg->docker_container[0]) {
        target = cfg->docker_container;
    } else if (cfg->docker_auto) {
        if (cfg->non_interactive) {
            agent_buf out = {0};
            if (agent_docker_list_sandbox_names(cfg, docker_command, &out) &&
                out.ptr && out.ptr[0]) {
                char *newline = strchr(out.ptr, '\n');
                if (newline) *newline = '\0';
                if (out.ptr[0]) {
                    snprintf(chosen, sizeof(chosen), "%s", out.ptr);
                    target = chosen;
                }
            }
            free(out.ptr);
        } else if (agent_prompt_startup_docker_sandbox(cfg, docker_command,
                                                       input, output, chosen,
                                                       sizeof(chosen))) {
            target = chosen;
        } else {
            return;
        }
    }

    if (!target || !target[0]) return;
    char err[256];
    if (!agent_docker_activate_named_sandbox(NULL, cfg, target, false,
                                             err, sizeof(err))) {
        FILE *out_stream = output ? output : stdout;
        fprintf(out_stream, "startup docker sandbox activation failed: %s\n", err);
        cfg->docker_container = NULL;
    }
}

static void agent_config_prepare_startup_docker_sandbox(agent_config *cfg) {
    agent_config_prepare_startup_docker_sandbox_with_streams(cfg, stdin, stdout);
}

/* What: worker-scoped wrapper for Docker feature availability.
 * Why: slash-command handlers work with agent_worker and need a common guard.
 * Callers: agent_docker_command_available(). */
static bool agent_docker_feature_available_worker(agent_worker *w) {
    return w && w->cfg && agent_docker_feature_available_cfg(w->cfg);
}

/* What: validate that a Docker slash command may run and print the user-facing
 * failure if Docker is unavailable.
 * Why: every `/docker ...` command should fail consistently before touching the
 * Docker CLI.
 * Callers: agent_command_docker_list(), agent_command_docker_create(),
 * agent_command_docker_use(), agent_command_docker_describe(),
 * agent_command_docker_stop(), and agent_command_docker_destroy(). */
static bool agent_docker_command_available(agent_worker *w, const char *op) {
    if (agent_docker_feature_available_worker(w)) return true;
    printf("%s failed: docker sandbox feature is not available\n", op);
    return false;
}

static void agent_string_array_free(char **v, int count) {
    if (!v) return;
    for (int i = 0; i < count; i++) free(v[i]);
    free(v);
}

static bool agent_json_parse_string_array(const char *json,
                                          char ***out_v,
                                          int *out_count) {
    if (out_v) *out_v = NULL;
    if (out_count) *out_count = 0;
    if (!json) return false;

    const unsigned char *p = (const unsigned char *)json;
    while (isspace(*p)) p++;
    if (*p != '[') return false;
    p++;

    char **items = NULL;
    int len = 0;
    int cap = 0;
    for (;;) {
        while (isspace(*p)) p++;
        if (*p == ']') {
            p++;
            break;
        }
        if (*p != '"') {
            agent_string_array_free(items, len);
            return false;
        }
        p++;
        agent_buf item = {0};
        while (*p && *p != '"') {
            if (*p == '\\') {
                p++;
                if (!*p) {
                    agent_string_array_free(items, len);
                    free(item.ptr);
                    return false;
                }
                char c = (char)*p;
                switch (c) {
                    case '"': case '\\': case '/':
                        agent_buf_append(&item, &c, 1);
                        break;
                    case 'b': c = '\b'; agent_buf_append(&item, &c, 1); break;
                    case 'f': c = '\f'; agent_buf_append(&item, &c, 1); break;
                    case 'n': c = '\n'; agent_buf_append(&item, &c, 1); break;
                    case 'r': c = '\r'; agent_buf_append(&item, &c, 1); break;
                    case 't': c = '\t'; agent_buf_append(&item, &c, 1); break;
                    case 'u':
                        /* Docker argv JSON here is expected to be plain ASCII. */
                        agent_string_array_free(items, len);
                        free(item.ptr);
                        return false;
                    default:
                        agent_string_array_free(items, len);
                        free(item.ptr);
                        return false;
                }
                p++;
                continue;
            }
            char c = (char)*p++;
            agent_buf_append(&item, &c, 1);
        }
        if (*p != '"') {
            agent_string_array_free(items, len);
            free(item.ptr);
            return false;
        }
        p++;
        if (len == cap) {
            cap = cap ? cap * 2 : 4;
            items = xrealloc(items, (size_t)cap * sizeof(items[0]));
        }
        items[len++] = agent_buf_take(&item);
        while (isspace(*p)) p++;
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        agent_string_array_free(items, len);
        return false;
    }

    if (out_v) *out_v = items;
    else agent_string_array_free(items, len);
    if (out_count) *out_count = len;
    return true;
}

/* What: build a stable string representing the Docker command, active
 * container, temp dir, and workspace roots used for sandbox mounts.
 * Why: mount refresh is expensive and destructive, so unchanged mount state
 * should be skipped.
 * Callers: agent_docker_refresh_mounts() and its mount-refresh unit test. */
static void agent_docker_mount_fingerprint(agent_worker *w,
                                           char *out, size_t out_len) {
    if (!out || !out_len) return;
    out[0] = '\0';
    if (!w || !w->cfg) return;
    agent_buf b = {0};
    agent_buf_puts(&b, "cmd=");
    agent_buf_puts(&b, w->cfg->docker_command && w->cfg->docker_command[0] ?
                   w->cfg->docker_command : "docker");
    agent_buf_puts(&b, "\ncontainer=");
    agent_buf_puts(&b, w->cfg->docker_container ? w->cfg->docker_container : "");
    agent_buf_puts(&b, "\ntemp=");
    agent_buf_puts(&b, w->cfg->temp_directory);
    agent_buf_puts(&b, "\nworkspaces=");
    for (int i = 0; i < w->working_directories.len; i++) {
        if (i) agent_buf_puts(&b, "\n");
        agent_buf_puts(&b, w->working_directories.v[i]);
    }
    snprintf(out, out_len, "%s", b.ptr ? b.ptr : "");
    free(b.ptr);
}

/* What: recreate labeled Docker sandboxes so their bind mounts match the
 * current workspace and temp-directory configuration.
 * Why: `/workspace +...` and `/workspace -...` change what tools may access;
 * existing containers must be rebuilt or they keep stale mounts.
 * Callers: run_agent() after workspace add/remove commands; tests cover the
 * unchanged-fingerprint fast path. */
static bool agent_docker_refresh_mounts(agent_worker *w,
                                        char *err, size_t err_len) {
    if (!w || !w->cfg) return true;
    if (!agent_docker_feature_available_cfg(w->cfg)) return true;
    if (!w->cfg->docker_container || !w->cfg->docker_container[0]) return true;
    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    char fingerprint[sizeof(w->docker_mount_fingerprint)];
    agent_docker_mount_fingerprint(w, fingerprint, sizeof(fingerprint));
    if (fingerprint[0] && !strcmp(w->docker_mount_fingerprint, fingerprint))
        return true;

    agent_buf names = {0};
    if (!agent_docker_list_sandbox_names(w->cfg, docker_command, &names)) {
        snprintf(err, err_len, "unable to list docker sandboxes");
        return false;
    }
    if (!names.len) {
        free(names.ptr);
        snprintf(w->docker_mount_fingerprint,
                 sizeof(w->docker_mount_fingerprint), "%s", fingerprint);
        return true;
    }

    char **name_v = NULL;
    int name_count = 0;
    int name_cap = 0;
    char *save = NULL;
    for (char *line = strtok_r(names.ptr, "\n", &save);
         line;
         line = strtok_r(NULL, "\n", &save)) {
        if (!line[0]) continue;
        if (name_count == name_cap) {
            name_cap = name_cap ? name_cap * 2 : 8;
            name_v = xrealloc(name_v, (size_t)name_cap * sizeof(name_v[0]));
        }
        name_v[name_count++] = line;
    }
    if (!name_count) {
        free(name_v);
        free(names.ptr);
        snprintf(w->docker_mount_fingerprint,
                 sizeof(w->docker_mount_fingerprint), "%s", fingerprint);
        return true;
    }

    agent_buf inspect = {0};
    if (!agent_docker_inspect_sandboxes_sync(w->cfg, docker_command, name_v,
                                             name_count, &inspect)) {
        snprintf(err, err_len, "unable to inspect docker sandboxes");
        free(inspect.ptr);
        free(name_v);
        free(names.ptr);
        return false;
    }

    int refreshed = 0;
    char *inspect_save = NULL;
    char *inspect_text = inspect.ptr ? inspect.ptr : "";
    for (char *row = strtok_r(inspect_text, "\n", &inspect_save);
         row;
         row = strtok_r(NULL, "\n", &inspect_save)) {
        char *newline = strchr(row, '\n');
        if (newline) *newline = '\0';
        char *name = row;
        if (name[0] == '/') name++;
        char *image = strchr(row, '\t');
        if (!image) {
            snprintf(err, err_len, "malformed docker inspect output");
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        *image++ = '\0';
        char *state = strchr(image, '\t');
        if (!state) {
            snprintf(err, err_len, "malformed docker inspect output for %s", name);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        *state++ = '\0';
        char *path = strchr(state, '\t');
        if (!path) {
            snprintf(err, err_len, "malformed docker inspect output for %s", name);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        *path++ = '\0';
        char *args_json = strchr(path, '\t');
        if (!args_json) {
            snprintf(err, err_len, "malformed docker inspect output for %s", name);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        *args_json++ = '\0';

        char **args = NULL;
        int arg_count = 0;
        if (!agent_json_parse_string_array(args_json, &args, &arg_count)) {
            snprintf(err, err_len, "unable to parse docker args for %s", name);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }

        int mount_count = w->working_directories.len;
        bool mount_temp = w->cfg->temp_directory[0] != '\0';
        int argv_cap = 16 + arg_count + (mount_count + (mount_temp ? 1 : 0)) * 2;
        char **argv = xmalloc((size_t)argv_cap * sizeof(char *));
        char **mount_args = xmalloc((size_t)(mount_count + (mount_temp ? 1 : 0) + 1) *
                                    sizeof(char *));
        int mount_args_len = 0;
        bool was_running = !strcmp(state, "running");

        if (was_running) {
            char *stop_argv[] = {(char *)docker_command, "stop", name, NULL};
            agent_buf stop_out = {0};
            if (!agent_docker_capture(w->cfg, docker_command, stop_argv, "docker stop",
                                      &stop_out)) {
                snprintf(err, err_len, "unable to stop docker sandbox: %s", name);
                free(stop_out.ptr);
                free(argv);
                free(mount_args);
                agent_string_array_free(args, arg_count);
                free(inspect.ptr);
                free(name_v);
                free(names.ptr);
                return false;
            }
            free(stop_out.ptr);
        }

        char *rm_argv[] = {(char *)docker_command, "rm", name, NULL};
        agent_buf rm_out = {0};
        if (!agent_docker_capture(w->cfg, docker_command, rm_argv, "docker rm", &rm_out)) {
            snprintf(err, err_len, "unable to remove docker sandbox: %s", name);
            free(rm_out.ptr);
            free(argv);
            free(mount_args);
            agent_string_array_free(args, arg_count);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        free(rm_out.ptr);

        int argc = 0;
        argv[argc++] = (char *)docker_command;
        argv[argc++] = was_running ? "run" : "create";
        if (was_running) argv[argc++] = "-d";
        argv[argc++] = "--name";
        argv[argc++] = name;
        argv[argc++] = "--label";
        argv[argc++] = "ds4:sandbox";
        for (int i = 0; i < mount_count; i++) {
            const char *root = w->working_directories.v[i];
            size_t n = strlen(root) * 2 + 2;
            char *mount = xmalloc(n);
            snprintf(mount, n, "%s:%s", root, root);
            mount_args[mount_args_len++] = mount;
            argv[argc++] = "-v";
            argv[argc++] = mount;
        }
        if (mount_temp) {
            size_t n = strlen(w->cfg->temp_directory) * 2 + 2;
            char *mount = xmalloc(n);
            snprintf(mount, n, "%s:%s", w->cfg->temp_directory,
                     w->cfg->temp_directory);
            mount_args[mount_args_len++] = mount;
            argv[argc++] = "-v";
            argv[argc++] = mount;
        }
        argv[argc++] = image;
        argv[argc++] = path;
        for (int i = 0; i < arg_count; i++)
            argv[argc++] = args[i];
        argv[argc] = NULL;

        agent_buf create_out = {0};
        bool ok = agent_docker_capture(w->cfg, docker_command, argv,
                                       was_running ? "docker run" : "docker create",
                                       &create_out);
        for (int i = 0; i < mount_args_len; i++) free(mount_args[i]);
        free(mount_args);
        free(argv);
        agent_string_array_free(args, arg_count);
        if (!ok) {
            snprintf(err, err_len, "unable to recreate docker sandbox: %s", name);
            free(create_out.ptr);
            free(inspect.ptr);
            free(name_v);
            free(names.ptr);
            return false;
        }
        free(create_out.ptr);
        refreshed++;
    }

    free(inspect.ptr);
    free(name_v);
    free(names.ptr);
    snprintf(w->docker_mount_fingerprint,
             sizeof(w->docker_mount_fingerprint), "%s", fingerprint);
    if (refreshed > 0)
        printf("updated docker sandbox mounts for %d container%s\n",
               refreshed, refreshed == 1 ? "" : "s");
    return true;
}

/* What: print all labeled DS4 Docker sandboxes with state, IP, image, tags, and
 * current-selection marker.
 * Why: users need a quick overview before switching, stopping, or destroying a
 * sandbox.
 * Callers: run_agent() slash-command dispatch for `/docker list`. */
static void agent_command_docker_list(agent_worker *w) {
    if (!agent_docker_command_available(w, "docker list")) return;
    const char *docker_command =
        (w && w->cfg && w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    agent_buf out = {0};
    if (!agent_docker_list_sandbox_names(w->cfg, docker_command, &out))
        return;

    if (!out.len) {
        printf("no ds4 sandbox containers found\n");
        free(out.ptr);
        return;
    }

    bool color = isatty(STDOUT_FILENO) != 0;
    int shown = 0;
    char *save = NULL;
    for (char *line = strtok_r(out.ptr, "\n", &save);
         line;
         line = strtok_r(NULL, "\n", &save)) {
        agent_buf inspect = {0};
        if (!agent_docker_inspect_sandbox(w->cfg, docker_command, line, &inspect))
            continue;
        char *name = inspect.ptr ? inspect.ptr : line;
        char *display = name;
        if (display[0] == '/') display++;
        char *labels_json = strchr(name, '\t');
        if (!labels_json) {
            free(inspect.ptr);
            continue;
        }
        *labels_json++ = '\0';
        char *image = strchr(labels_json, '\t');
        if (!image) {
            free(inspect.ptr);
            continue;
        }
        *image++ = '\0';
        char *state = strchr(image, '\t');
        if (!state) {
            free(inspect.ptr);
            continue;
        }
        *state++ = '\0';
        char *ip = strchr(state, '\t');
        if (!ip) {
            free(inspect.ptr);
            continue;
        }
        *ip++ = '\0';
        size_t ip_len = strlen(ip);
        while (ip_len > 0 && isspace((unsigned char)ip[ip_len - 1]))
            ip[--ip_len] = '\0';

        const char *state_color = "";
        const char *reset = "";
        if (color) {
            if (!strcmp(state, "running")) {
                state_color = "\x1b[32m";
                reset = "\x1b[0m";
            } else {
                state_color = "\x1b[90m";
                reset = "\x1b[0m";
            }
        }
        bool current = w && w->cfg && w->cfg->docker_container &&
                       !strcmp(w->cfg->docker_container, display);
        printf("%s%s%-20s %-10s%s ip=%-15s image=%s tags=%s%s\n",
               state_color,
               current ? "* " : "  ",
               display,
               state,
               reset,
               ip[0] ? ip : "-",
               image[0] ? image : "-",
               labels_json[0] ? labels_json : "{}",
               current ? " current" : "");
        shown++;
        free(inspect.ptr);
    }
    if (!shown) printf("no ds4 sandbox containers found\n");
    free(out.ptr);
}

/* What: create and select a new labeled Docker sandbox using the current
 * workspace/temp mounts and optional command.
 * Why: agent tools need a long-lived container with predictable bind mounts;
 * selecting it immediately makes subsequent tools run inside the sandbox.
 * Callers: run_agent() slash-command dispatch for `/docker create`. */
static void agent_command_docker_create(agent_worker *w, char *args) {
    if (!w || !w->cfg) {
        printf("docker create failed: worker configuration unavailable\n");
        return;
    }
    if (!agent_docker_command_available(w, "docker create")) return;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    if (!agent_command_in_path(docker_command)) {
        printf("docker create failed: docker command not found: %s\n",
               docker_command);
        return;
    }

    while (*args == ' ' || *args == '\t') args++;
    if (!args[0]) {
        printf("usage: /docker create <image> <name> [command]\n");
        return;
    }

    char *image = args;
    while (*args && *args != ' ' && *args != '\t') args++;
    if (*args) *args++ = '\0';
    while (*args == ' ' || *args == '\t') args++;
    if (!args[0]) {
        printf("usage: /docker create <image> <name> [command]\n");
        return;
    }

    char *name = args;
    while (*args && *args != ' ' && *args != '\t') args++;
    if (*args) *args++ = '\0';
    while (*args == ' ' || *args == '\t') args++;
    const char *command = args[0] ? args : "sleep infinity";

    int mount_count = w->working_directories.len;
    bool mount_temp = w->cfg->temp_directory[0] != '\0';
    int argv_cap = 16 + (mount_count + (mount_temp ? 1 : 0)) * 2;
    int mount_argc = mount_count + (mount_temp ? 1 : 0);
    char **argv = xmalloc((size_t)argv_cap * sizeof(char *));
    memset(argv, 0, (size_t)argv_cap * sizeof(char *));
    char **mount_args = mount_argc > 0 ?
        xmalloc((size_t)mount_argc * sizeof(char *)) : NULL;
    int mount_args_len = 0;
    int argc = 0;
    argv[argc++] = (char *)docker_command;
    argv[argc++] = "run";
    argv[argc++] = "-d";
    argv[argc++] = "--name";
    argv[argc++] = name;
    argv[argc++] = "--label";
    argv[argc++] = "ds4:sandbox";
    for (int i = 0; i < mount_count; i++) {
        const char *root = w->working_directories.v[i];
        size_t n = strlen(root) * 2 + 2;
        char *mount = xmalloc(n);
        snprintf(mount, n, "%s:%s", root, root);
        mount_args[mount_args_len++] = mount;
        argv[argc++] = "-v";
        argv[argc++] = mount;
    }
    if (mount_temp) {
        size_t n = strlen(w->cfg->temp_directory) * 2 + 2;
        char *mount = xmalloc(n);
        snprintf(mount, n, "%s:%s", w->cfg->temp_directory,
                 w->cfg->temp_directory);
        mount_args[mount_args_len++] = mount;
        argv[argc++] = "-v";
        argv[argc++] = mount;
    }
    argv[argc++] = image;
    argv[argc++] = "/bin/sh";
    argv[argc++] = "-lc";
    argv[argc++] = (char *)command;
    argv[argc] = NULL;
    if (w->cfg && w->cfg->docker_debug) {
        char *cmd = agent_docker_debug_command_text(argv);
        if (cmd) {
            bool color = isatty(STDOUT_FILENO) != 0;
            if (color) printf("\x1b[96m[docker debug] %s\x1b[0m\n\n", cmd);
            else printf("[docker debug] %s\n\n", cmd);
            fflush(stdout);
            free(cmd);
        }
    }

    /* Build argv_tail starting at "run" (skip docker_command at argv[0]) */
    const char *const *argv_tail = (const char *const *)(argv + 1);

    agent_buf out = {0};
    bool ok = agent_docker_exec(w, w->cfg, argv_tail, NULL, 0, false,
                                "docker create", &out, NULL);
    if (!ok) {
        printf("docker create failed:\n%s", out.ptr ? out.ptr : "");
        if (!out.len || (out.ptr && out.ptr[out.len - 1] != '\n')) printf("\n");
        free(out.ptr);
        for (int i = 0; i < mount_args_len; i++) free(mount_args[i]);
        free(mount_args);
        free(argv);
        return;
    }

    for (int i = 0; i < mount_args_len; i++) free(mount_args[i]);
    free(mount_args);
    free(argv);

    w->cfg->docker_container = xstrdup(name);
    printf("docker sandbox switched to %s\n", name);
    if (out.ptr && out.ptr[0]) {
        printf("%s", out.ptr);
        if (out.ptr[out.len - 1] != '\n') printf("\n");
    }
    free(out.ptr);

    /* Clear stale shell state and start a persistent shell */
    agent_docker_shell_stop(w);
    if (!agent_docker_shell_start(w)) {
        printf("warning: failed to start persistent shell for sandbox %s\n",
               name);
    }
}


/* What: show the current sandbox or switch to a named labeled sandbox, starting
 * it if needed.
 * Why: the agent must retarget file tools and bash jobs to the selected
 * container and replace any persistent shell from the previous sandbox.
 * Callers: run_agent() slash-command dispatch for `/docker use`. */
static void agent_command_docker_use(agent_worker *w, char *args) {
    if (!w || !w->cfg) {
        printf("docker use failed: worker configuration unavailable\n");
        return;
    }
    if (!agent_docker_command_available(w, "docker use")) return;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    if (!agent_command_in_path(docker_command)) {
        printf("docker use failed: docker command not found: %s\n",
               docker_command);
        return;
    }

    while (*args == ' ' || *args == '\t') args++;
    if (!args[0]) {
        if (!w->cfg->docker_container || !w->cfg->docker_container[0]) {
            printf("no current docker sandbox selected\n");
            return;
        }
        printf("current docker sandbox: %s", w->cfg->docker_container);
        printf("\n");
        return;
    }

    char *name = args;
    while (*args && *args != ' ' && *args != '\t') args++;
    if (*args) *args = '\0';
    char err[256];
    if (!agent_docker_activate_named_sandbox(w, w->cfg, name, true,
                                             err, sizeof(err))) {
        printf("docker use failed: %s\n", err);
    }
}

/* What: print detailed metadata for one labeled Docker sandbox.
 * Why: users need state/image/IP/tag information before choosing a sandbox or
 * debugging why a container is not eligible.
 * Callers: run_agent() slash-command dispatch for `/docker describe`. */
static void agent_command_docker_describe(agent_worker *w, char *args) {
    if (!w || !w->cfg) {
        printf("docker describe failed: worker configuration unavailable\n");
        return;
    }
    if (!agent_docker_command_available(w, "docker describe")) return;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    if (!agent_command_in_path(docker_command)) {
        printf("docker describe failed: docker command not found: %s\n",
               docker_command);
        return;
    }

    while (*args == ' ' || *args == '\t') args++;
    if (!args[0]) {
        printf("usage: /docker describe <name>\n");
        return;
    }

    char *name = args;
    while (*args && *args != ' ' && *args != '\t') args++;
    if (*args) *args = '\0';

    agent_buf out = {0};
    if (!agent_docker_inspect_sandbox(w->cfg, docker_command, name, &out)) {
        printf("docker describe failed: unable to inspect sandbox: %s\n", name);
        return;
    }

    char *line = out.ptr ? out.ptr : "";
    char *container_name = NULL, *labels = NULL, *image = NULL, *state = NULL, *ip = NULL;
    if (!agent_docker_parse_sandbox_row(line, &container_name, &labels, &image,
                                        &state, &ip)) {
        printf("docker describe failed: malformed docker inspect output\n");
        free(out.ptr);
        return;
    }

    if (!strstr(labels, "\"ds4:sandbox\"")) {
        printf("docker describe failed: container is not tagged ds4:sandbox: %s\n",
               name);
        free(out.ptr);
        return;
    }

    printf("name:   %s\n", container_name);
    printf("state:  %s\n", state[0] ? state : "unknown");
    printf("image:  %s\n", image[0] ? image : "-");
    printf("ip:     %s\n", ip[0] ? ip : "-");
    printf("tags:   %s\n", labels[0] ? labels : "{}");
    printf("active: %s\n",
           (w->cfg->docker_container &&
            !strcmp(w->cfg->docker_container, container_name)) ? "yes" : "no");
    free(out.ptr);
    return;
}

/* What: stop one labeled sandbox or all labeled sandboxes, clearing the active
 * selection if the current container is stopped.
 * Why: stopping a container invalidates the persistent shell and should prevent
 * later tools from accidentally writing to a dead sandbox.
 * Callers: run_agent() slash-command dispatch for `/docker stop`. */
static void agent_command_docker_stop(agent_worker *w, char *args) {
    if (!w || !w->cfg) {
        printf("docker stop failed: worker configuration unavailable\n");
        return;
    }
    if (!agent_docker_command_available(w, "docker stop")) return;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    if (!agent_command_in_path(docker_command)) {
        printf("docker stop failed: docker command not found: %s\n",
               docker_command);
        return;
    }

    while (*args == ' ' || *args == '\t') args++;

    agent_buf names = {0};
    if (!agent_docker_list_sandbox_names(w->cfg, docker_command, &names)) {
        printf("docker stop failed: unable to list sandboxes\n");
        return;
    }
    if (!names.len) {
        printf("no ds4 sandbox containers found\n");
        free(names.ptr);
        return;
    }

    char *targets[256];
    int target_count = 0;
    bool stopping_current = false;
    char *save = NULL;
    if (!args[0]) {
        for (char *line = strtok_r(names.ptr, "\n", &save);
             line && target_count < (int)(sizeof(targets) / sizeof(targets[0]));
             line = strtok_r(NULL, "\n", &save)) {
            targets[target_count++] = line;
            if (w->cfg->docker_container && !strcmp(w->cfg->docker_container, line))
                stopping_current = true;
        }
    } else {
        char *name = args;
        while (*args && *args != ' ' && *args != '\t') args++;
        if (*args) *args = '\0';

        for (char *line = strtok_r(names.ptr, "\n", &save);
             line;
             line = strtok_r(NULL, "\n", &save)) {
            if (strcmp(line, name) != 0) continue;
            targets[target_count++] = line;
            if (w->cfg->docker_container && !strcmp(w->cfg->docker_container, line))
                stopping_current = true;
            break;
        }
        if (target_count == 0) {
            printf("docker stop failed: sandbox not found: %s\n", name);
            free(names.ptr);
            return;
        }
    }

    if (stopping_current) {
        printf("warning: stopping the sandbox currently in use: %s\n",
               w->cfg->docker_container ? w->cfg->docker_container : "(unknown)");
    }

    /* Stop the persistent shell before stopping the container */
    if (stopping_current) {
        agent_docker_shell_stop(w);
    }

    /* Build docker stop argv_tail for agent_docker_exec: {"stop", name..., NULL} */
    int tail_cap = 2 + target_count + 1;
    const char **tail = xmalloc((size_t)tail_cap * sizeof(char *));
    int tail_idx = 0;
    tail[tail_idx++] = "stop";
    for (int i = 0; i < target_count; i++)
        tail[tail_idx++] = targets[i];
    tail[tail_idx] = NULL;

    agent_buf out = {0};
    bool ok = agent_docker_exec(w, w->cfg, tail, NULL, 0, false, "docker stop", &out, NULL);
    free(tail);
    if (!ok) {
        free(names.ptr);
        return;
    }

    if (out.ptr && out.ptr[0]) {
        printf("%s", out.ptr);
        if (out.ptr[out.len - 1] != '\n') printf("\n");
    }
    if (stopping_current) {
        free((char *)w->cfg->docker_container);
        w->cfg->docker_container = NULL;
        printf("cleared current docker sandbox selection\n");
    }
    if (!args[0]) {
        printf("stopped %d docker sandbox container%s\n",
               target_count, target_count == 1 ? "" : "s");
    }
    free(out.ptr);
    free(names.ptr);
}

static bool agent_prompt_yes_no_default_yes(const char *prompt) {
    char buf[32];
    for (;;) {
        printf("%s", prompt);
        fflush(stdout);
        int saved_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(STDIN_FILENO, F_SETFL, saved_flags & ~O_NONBLOCK);
        bool got_line = fgets(buf, sizeof(buf), stdin) != NULL;
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(STDIN_FILENO, F_SETFL, saved_flags);
        if (!got_line) return false;
        char *p = buf;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\n' || *p == '\0' || *p == 'y' || *p == 'Y') return true;
        if (*p == 'n' || *p == 'N') return false;
    }
}

/* What: confirm and remove a stopped, inactive labeled Docker sandbox.
 * Why: destructive removal should be limited to DS4 sandboxes, never the active
 * one, and only when Docker reports a removable stopped state.
 * Callers: run_agent() slash-command dispatch for `/docker destroy`. */
static void agent_command_docker_destroy(agent_worker *w, char *args) {
    if (!w || !w->cfg) {
        printf("docker destroy failed: worker configuration unavailable\n");
        return;
    }
    if (!agent_docker_command_available(w, "docker destroy")) return;

    const char *docker_command =
        (w->cfg->docker_command && w->cfg->docker_command[0]) ?
        w->cfg->docker_command : "docker";
    if (!agent_command_in_path(docker_command)) {
        printf("docker destroy failed: docker command not found: %s\n",
               docker_command);
        return;
    }

    while (*args == ' ' || *args == '\t') args++;
    if (!args[0]) {
        printf("usage: /docker destroy <name>\n");
        return;
    }

    char *name = args;
    while (*args && *args != ' ' && *args != '\t') args++;
    if (*args) *args = '\0';

    agent_buf out = {0};
    if (!agent_docker_inspect_sandbox(w->cfg, docker_command, name, &out)) {
        printf("docker destroy failed: unable to inspect sandbox: %s\n", name);
        return;
    }

    char *line = out.ptr ? out.ptr : "";
    char *container_name = NULL, *labels = NULL, *image = NULL, *state = NULL, *ip = NULL;
    if (!agent_docker_parse_sandbox_row(line, &container_name, &labels, &image,
                                        &state, &ip)) {
        printf("docker destroy failed: malformed docker inspect output\n");
        free(out.ptr);
        return;
    }
    if (!strstr(labels, "\"ds4:sandbox\"")) {
        printf("docker destroy failed: container is not tagged ds4:sandbox: %s\n",
               name);
        free(out.ptr);
        return;
    }
    if (w->cfg->docker_container &&
        !strcmp(w->cfg->docker_container, container_name)) {
        printf("docker destroy failed: sandbox is currently in use: %s\n",
               container_name);
        free(out.ptr);
        return;
    }
    if (strcmp(state, "exited") != 0 &&
        strcmp(state, "created") != 0 &&
        strcmp(state, "dead") != 0) {
        printf("docker destroy failed: sandbox must be stopped first: %s (state=%s)\n",
               container_name, state[0] ? state : "unknown");
        free(out.ptr);
        return;
    }

    char prompt[PATH_MAX + 64];
    snprintf(prompt, sizeof(prompt), "Destroy docker sandbox %s? (Y/n) ",
             container_name);
    if (!agent_prompt_yes_no_default_yes(prompt)) {
        printf("docker destroy cancelled\n");
        free(out.ptr);
        return;
    }

    char *argv[] = {
        (char *)docker_command,
        "rm",
        container_name,
        NULL,
    };
    agent_buf rm_out = {0};
    if (!agent_docker_capture(w->cfg, docker_command, argv, "docker destroy", &rm_out)) {
        printf("docker destroy failed: unable to remove sandbox: %s\n",
               container_name);
        free(out.ptr);
        return;
    }
    if (rm_out.ptr && rm_out.ptr[0]) {
        printf("%s", rm_out.ptr);
        if (rm_out.ptr[rm_out.len - 1] != '\n') printf("\n");
    }
    printf("destroyed docker sandbox %s\n", container_name);
    free(rm_out.ptr);
    free(out.ptr);
    return;
}

/* Initialize the worker, cache directory, sysprompt checkpoint path, trace file,
 * and model thread.  After this returns, all DS4 session mutation happens on
 * the worker thread. */
int agent_worker_init(agent_worker *w, ds4_engine *engine, agent_config *cfg) {
    memset(w, 0, sizeof(*w));
    w->engine = engine;
    w->cfg = cfg;
    w->wake_fd[0] = -1;
    w->wake_fd[1] = -1;
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->cond, NULL);
    w->docker_shell.stdin_fd = -1;
    w->docker_shell.stdout_fd = -1;
    pthread_mutex_init(&w->docker_shell.mu, NULL);
    bool created_skill_registry = false;
    if (cfg->skill_registry) {
        w->skill_registry = cfg->skill_registry;
        agent_skill_registry_acquire(w->skill_registry);
    } else {
        w->skill_registry = agent_skill_registry_create();
        cfg->skill_registry = w->skill_registry;
        created_skill_registry = true;
    }
    w->status.state = AGENT_WORKER_IDLE;
    w->status.think_mode = cfg->gen.think_mode;
    bool tools_explicit = cfg->default_tools[0];
    if (tools_explicit)
        ds4_agent_tool_policy_parse(cfg->default_tools, &w->tool_policy);
    else
        ds4_agent_tool_policy_parse("all", &w->tool_policy);
    for (int i = 0; i < cfg->working_directories.len; i++)
        agent_path_list_append(&w->working_directories, cfg->working_directories.v[i]);
    /* Startup directories populate the shared registry exactly once. */
    if (created_skill_registry) agent_skills_init(w);
    pthread_mutex_lock(&w->skill_registry->mu);
    w->skills_prompt_generation = w->skill_registry->generation;
    pthread_mutex_unlock(&w->skill_registry->mu);
    if (pipe(w->wake_fd) != 0) return -1;
    int old_flags;
    set_nonblock(w->wake_fd[0], true, &old_flags);
    set_nonblock(w->wake_fd[1], true, &old_flags);
    if (ds4_session_create(&w->session, engine, cfg->gen.ctx_size) != 0) {
        fprintf(stderr, "ds4-agent: session backend is required\n");
        return -1;
    }
    w->cache_dir = agent_default_cache_dir();
    if (!agent_mkdir_p(w->cache_dir)) {
        fprintf(stderr, "ds4-agent: failed to create %s: %s\n",
                w->cache_dir, strerror(errno));
        return -1;
    }
    ds4_web_config web_cfg = {
        .home_dir = getenv("HOME"),
        .port = 9333,
        .cdp_host = cfg->web_cdp_host[0] ? cfg->web_cdp_host : NULL,
        .cdp_port = cfg->web_cdp_port,
        .confirm = agent_web_confirm,
        .confirm_privdata = w,
        .log = agent_web_log,
        .log_privdata = w,
        .cancel = agent_web_cancel,
        .cancel_privdata = w,
    };
    w->web = ds4_web_create(&web_cfg);
    w->sysprompt_path = ds4_kvstore_path_join(w->cache_dir, "sysprompt.kv");
    if (cfg->gen.trace_path && cfg->gen.trace_path[0]) {
        w->trace = fopen(cfg->gen.trace_path, "ab");
        if (!w->trace) {
            fprintf(stderr, "ds4-agent: failed to open trace %s: %s\n",
                    cfg->gen.trace_path, strerror(errno));
            return -1;
        }
    }
    /* Start persistent Docker shell if a container is already selected. */
    if (agent_bash_use_docker_sandbox(w)) {
        if (!agent_docker_shell_start(w)) {
            fprintf(stderr, "ds4-agent: warning: failed to start persistent "
                    "Docker shell; bash/file ops will use one-shot exec\n");
        }
    }
    if (pthread_create(&w->thread, NULL, worker_main, w) != 0) return -1;
    return 0;
}

/* Shut down the worker and release owned resources, including any live bash
 * process groups. */
void agent_worker_free(agent_worker *w) {
    worker_stop(w);
    if (w->thread) pthread_join(w->thread, NULL);
    agent_bash_jobs_free(w);
    agent_docker_shell_stop(w);
    ds4_web_free(w->web);
    ds4_session_free(w->session);
    ds4_tokens_free(&w->transcript);
    free(w->cache_dir);
    free(w->sysprompt_path);
    free(w->session_title);
    free(w->legacy_session_path_to_delete);
    free(w->queued_user_drain_text);
    free(w->question_answer);
    agent_path_list_free(&w->working_directories);
    agent_skill_registry_release(w->skill_registry);
    w->skill_registry = NULL;
    agent_temp_files_cleanup_now(w);
    agent_temp_files_free(&w->temp_files);
    agent_path_list_free(&w->auto_allowed_paths);
    if (w->wake_fd[0] >= 0) close(w->wake_fd[0]);
    if (w->wake_fd[1] >= 0) close(w->wake_fd[1]);
    if (w->trace) fclose(w->trace);
    free(w->cmd_text);
    free(w->out);
    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->mu);
    pthread_mutex_destroy(&w->docker_shell.mu);
}

typedef enum {
    AGENT_YES_NO_AUTO_NONE,
    AGENT_YES_NO_AUTO_NO,
    AGENT_YES_NO_AUTO_YES,
} agent_yes_no_auto;

typedef struct {
    int timeout_sec;
    agent_yes_no_auto timeout_answer;
} agent_yes_no_options;

static const char *agent_yes_no_auto_name(agent_yes_no_auto answer) {
    switch (answer) {
    case AGENT_YES_NO_AUTO_NO: return "no";
    case AGENT_YES_NO_AUTO_YES: return "yes";
    default: return "";
    }
}

/* Shared y/n prompt.  By default it blocks forever like the historical helper;
 * callers that cannot safely stall the agent can request an automatic answer
 * after timeout_sec seconds. */
static bool agent_prompt_yes_no_ex(const char *prompt,
                                   const agent_yes_no_options *opts,
                                   bool *timed_out) {
    char buf[32];
    int timeout_sec = opts ? opts->timeout_sec : 0;
    agent_yes_no_auto auto_answer = opts ?
        opts->timeout_answer : AGENT_YES_NO_AUTO_NONE;
    bool use_timeout = timeout_sec > 0 && auto_answer != AGENT_YES_NO_AUTO_NONE;
    double deadline = use_timeout ? now_sec() + timeout_sec : 0.0;

    if (timed_out) *timed_out = false;
    for (;;) {
        printf("%s", prompt);
        if (use_timeout) {
            int rem = (int)(deadline - now_sec() + 0.999);
            if (rem < 0) rem = 0;
            printf("[auto-%s in %ds] ", agent_yes_no_auto_name(auto_answer), rem);
        }
        fflush(stdout);
        if (use_timeout) {
            double rem_sec = deadline - now_sec();
            if (rem_sec <= 0.0) {
                if (timed_out) *timed_out = true;
                printf("\n");
                return auto_answer == AGENT_YES_NO_AUTO_YES;
            }
            struct pollfd pfd = {.fd = STDIN_FILENO, .events = POLLIN};
            int timeout_ms = (int)(rem_sec * 1000.0) + 1;
            int rc;
            do {
                rc = poll(&pfd, 1, timeout_ms);
            } while (rc < 0 && errno == EINTR);
            if (rc == 0) {
                if (timed_out) *timed_out = true;
                printf("\n");
                return auto_answer == AGENT_YES_NO_AUTO_YES;
            }
            if (rc < 0) return false;
        }
        /* stdin may be in non-blocking mode (set by editor_start).
         * Temporarily switch to blocking so fgets can wait for input. */
        int saved_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK)) {
            fcntl(STDIN_FILENO, F_SETFL, saved_flags & ~O_NONBLOCK);
        }
        bool got_line = fgets(buf, sizeof(buf), stdin) != NULL;
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK)) {
            fcntl(STDIN_FILENO, F_SETFL, saved_flags);
        }
        if (!got_line) return false;
        char *p = buf;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == 'y' || *p == 'Y') return true;
        if (*p == 'n' || *p == 'N') return false;
    }
}

static bool agent_prompt_working_directory_choice(const char *prompt,
                                                  char options[3][PATH_MAX],
                                                  int option_count,
                                                  char choice[PATH_MAX],
                                                  bool *timed_out) {
    char buf[32];
    const int timeout_sec = 30;
    double deadline = now_sec() + timeout_sec;
    if (timed_out) *timed_out = false;
    if (choice) choice[0] = '\0';

    printf("%s\n", prompt ? prompt : "Choose working directory to allow:");
    for (int i = 0; i < option_count; i++)
        printf("  %d) %s\n", i + 1, options[i]);
    printf("  n) deny\n");
    fflush(stdout);

    for (;;) {
        double rem_sec = deadline - now_sec();
        int rem = rem_sec <= 0.0 ? 0 : (int)(rem_sec + 0.999);
        printf("\rSelect 1-%d or n/deny [auto-1 in %2ds] ", option_count, rem);
        fflush(stdout);
        if (rem_sec <= 0.0) {
            if (timed_out) *timed_out = true;
            printf("\n");
            if (option_count > 0 && choice)
                snprintf(choice, PATH_MAX, "%s", options[0]);
            return option_count > 0;
        }

        struct pollfd pfd = {.fd = STDIN_FILENO, .events = POLLIN};
        int timeout_ms = rem_sec > 1.0 ? 1000 : (int)(rem_sec * 1000.0) + 1;
        int rc;
        do {
            rc = poll(&pfd, 1, timeout_ms);
        } while (rc < 0 && errno == EINTR);
        if (rc == 0) continue;
        if (rc < 0) {
            printf("\n");
            return false;
        }

        int saved_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(STDIN_FILENO, F_SETFL, saved_flags & ~O_NONBLOCK);
        bool got_line = fgets(buf, sizeof(buf), stdin) != NULL;
        if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
            fcntl(STDIN_FILENO, F_SETFL, saved_flags);
        printf("\n");
        if (!got_line) return false;
        char *p = buf;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\n' || *p == '\0' || *p == 'y' || *p == 'Y') {
            if (option_count > 0 && choice)
                snprintf(choice, PATH_MAX, "%s", options[0]);
            return option_count > 0;
        }
        if (*p == 'n' || *p == 'N') return false;
        if (!strncasecmp(p, "deny", 4)) return false;
        if (!strncasecmp(p, "none", 4)) return false;
        if (*p >= '1' && *p <= '3') {
            int idx = *p - '1';
            if (idx >= 0 && idx < option_count) {
                if (choice) snprintf(choice, PATH_MAX, "%s", options[idx]);
                return true;
            }
        }
    }
}

static bool agent_prompt_read_line(char *buf, size_t len) {
    if (!buf || len == 0) return false;
    int saved_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
    if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
        fcntl(STDIN_FILENO, F_SETFL, saved_flags & ~O_NONBLOCK);
    bool got_line = fgets(buf, len, stdin) != NULL;
    if (saved_flags >= 0 && (saved_flags & O_NONBLOCK))
        fcntl(STDIN_FILENO, F_SETFL, saved_flags);
    if (!got_line) return false;
    size_t n = strlen(buf);
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r'))
        buf[--n] = '\0';
    return true;
}

static bool agent_prompt_ask_question(const char *prompt,
                                      char choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX],
                                      int choice_count,
                                      char answer[AGENT_ASK_QUESTION_ANSWER_MAX],
                                      bool *interrupted) {
    char buf[AGENT_ASK_QUESTION_ANSWER_MAX];
    if (answer) answer[0] = '\0';
    if (interrupted) *interrupted = false;

    printf("%s\n", prompt ? prompt : "Question:");
    if (choice_count <= 0) {
        printf("Answer: ");
        fflush(stdout);
        if (!agent_prompt_read_line(buf, sizeof(buf))) {
            if (interrupted) *interrupted = true;
            return false;
        }
        if (answer) snprintf(answer, AGENT_ASK_QUESTION_ANSWER_MAX, "%s", buf);
        return true;
    }

    int shown_count = choice_count;
    if (shown_count > AGENT_ASK_QUESTION_MAX_CHOICES)
        shown_count = AGENT_ASK_QUESTION_MAX_CHOICES;
    for (int i = 0; i < shown_count; i++)
        printf("  %d) %s\n", i + 1, choices[i]);
    int interrupt_choice = shown_count + 1;
    int something_else_choice = shown_count + 2;
    printf("  %d) Interrupt\n", interrupt_choice);
    printf("  %d) Something else\n", something_else_choice);

    for (;;) {
        printf("Select 1-%d: ", something_else_choice);
        fflush(stdout);
        if (!agent_prompt_read_line(buf, sizeof(buf))) {
            if (interrupted) *interrupted = true;
            return false;
        }
        char *p = buf;
        while (*p == ' ' || *p == '\t') p++;
        if (!*p) continue;
        if (!strcasecmp(p, "interrupt") || !strcasecmp(p, "i")) {
            if (interrupted) *interrupted = true;
            return false;
        }
        if (!strcasecmp(p, "something else") || !strcasecmp(p, "else") ||
            !strcasecmp(p, "other"))
        {
            printf("Answer: ");
            fflush(stdout);
            if (!agent_prompt_read_line(buf, sizeof(buf))) {
                if (interrupted) *interrupted = true;
                return false;
            }
            if (answer) snprintf(answer, AGENT_ASK_QUESTION_ANSWER_MAX, "%s", buf);
            return true;
        }
        char *end = NULL;
        long n = strtol(p, &end, 10);
        while (end && (*end == ' ' || *end == '\t')) end++;
        if (end && *end == '\0') {
            if (n >= 1 && n <= shown_count) {
                if (answer)
                    snprintf(answer, AGENT_ASK_QUESTION_ANSWER_MAX, "%s",
                             choices[n - 1]);
                return true;
            }
            if (n == interrupt_choice) {
                if (interrupted) *interrupted = true;
                return false;
            }
            if (n == something_else_choice) {
                printf("Answer: ");
                fflush(stdout);
                if (!agent_prompt_read_line(buf, sizeof(buf))) {
                    if (interrupted) *interrupted = true;
                    return false;
                }
                if (answer) snprintf(answer, AGENT_ASK_QUESTION_ANSWER_MAX, "%s", buf);
                return true;
            }
        }

        if (answer) snprintf(answer, AGENT_ASK_QUESTION_ANSWER_MAX, "%s", p);
        return true;
    }
}

bool agent_prompt_yes_no(const char *prompt) {
    return agent_prompt_yes_no_ex(prompt, NULL, NULL);
}

/* Ask before discarding a dirty user session.  Fresh sessions that contain only
 * the system prompt are deliberately ignored. */
static bool agent_maybe_save_before_leaving_session(agent_worker *w) {
    if (!agent_worker_needs_save(w)) return true;
    if (!agent_prompt_yes_no("Save current session? (y/n) ")) return true;
    char err[160] = {0};
    if (agent_worker_save_session(w, err, sizeof(err))) return true;
    printf("save failed: %s\n", err);
    return agent_prompt_yes_no("Continue anyway? (y/n) ");
}

typedef enum {
    AGENT_EXIT_CANCEL,
    AGENT_EXIT_CLEAN,
    AGENT_EXIT_NOW,
} agent_exit_save_result;

/* Process exit is different from /new or /switch: once the terminal is already
 * restored, declining the save can terminate immediately and let the OS reclaim
 * model/Metal resources instead of waiting for orderly teardown. */
static agent_exit_save_result agent_maybe_save_before_exiting(agent_worker *w) {
    if (!agent_worker_needs_save(w)) return AGENT_EXIT_CLEAN;
    if (!agent_prompt_yes_no("Save current session? (y/n) ")) return AGENT_EXIT_NOW;
    char err[160] = {0};
    if (agent_worker_save_session(w, err, sizeof(err))) return AGENT_EXIT_CLEAN;
    printf("save failed: %s\n", err);
    return agent_prompt_yes_no("Continue anyway? (y/n) ") ?
        AGENT_EXIT_NOW : AGENT_EXIT_CANCEL;
}

/* ============================================================================
 * Interactive Runtime Loop
 * ============================================================================
 */

static void agent_noninteractive_marker(const char *msg) {
    write_all(STDERR_FILENO, msg, strlen(msg));
    write_all(STDERR_FILENO, "\n", 1);
}

static int agent_read_stdin_available(agent_input_buf *in, bool *eof) {
    char buf[4096];
    for (;;) {
        ssize_t n = read(STDIN_FILENO, buf, sizeof(buf));
        if (n > 0) {
            agent_input_buf_append(in, buf, (size_t)n);
            continue;
        }
        if (n == 0) {
            *eof = true;
            return 0;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        perror("ds4-agent: read stdin");
        return -1;
    }
}

/* Headless mode is intentionally just another front-end for the same worker.
 * With -p/--prompt it is a one-shot execution.  Without -p it becomes a small
 * stdin protocol: announce readiness on stderr, collect bytes until stdin has
 * been quiet for 200 ms, submit that buffer as one prompt, and keep reading so
 * later input can be queued while the model is still working. */
static int run_agent_non_interactive(ds4_engine *engine, agent_config *cfg) {
    agent_worker worker;
    if (agent_worker_init(&worker, engine, cfg) != 0) return 1;

    const bool one_shot = cfg->gen.prompt != NULL;
    bool one_shot_submitted = false;
    bool stdin_eof = false;
    bool waiting_announced = false;
    bool stdin_nonblock = false;
    int old_stdin_flags = 0;
    agent_input_buf input = {0};
    agent_prompt_queue queue = {0};
    double quiet_deadline = 0.0;
    int rc = 0;

    if (!one_shot) {
        if (set_nonblock(STDIN_FILENO, true, &old_stdin_flags) != 0) {
            perror("ds4-agent: nonblocking stdin");
            agent_worker_free(&worker);
            return 1;
        }
        stdin_nonblock = true;
    }

    while (true) {
        bool initialized = worker_is_initialized(&worker, NULL);
        bool idle = worker_is_idle(&worker);

        if (one_shot && !one_shot_submitted && initialized) {
            if (worker_submit(&worker, cfg->gen.prompt))
                one_shot_submitted = true;
            idle = false;
        }

        if (!one_shot && queue.len && idle) {
            char *queued = agent_prompt_queue_take_all(&queue);
            if (worker_submit(&worker, queued)) {
                idle = false;
            } else {
                agent_prompt_queue_push_front(&queue, queued);
                queued = NULL;
            }
            free(queued);
        }

        if (!one_shot && initialized && idle && !queue.len &&
            input.len == 0 && !stdin_eof && !waiting_announced)
        {
            agent_noninteractive_marker("+DWARFSTAR_WAITING");
            waiting_announced = true;
        }

        int timeout_ms = -1;
        if (!one_shot && input.len > 0) {
            double rem = quiet_deadline - now_sec();
            timeout_ms = rem <= 0.0 ? 0 : (int)(rem * 1000.0) + 1;
        }

        struct pollfd pfd[2];
        int nfds = 0;
        int wake_idx = nfds;
        pfd[nfds++] = (struct pollfd){.fd = worker.wake_fd[0], .events = POLLIN};
        int stdin_idx = -1;
        if (!one_shot && initialized && !stdin_eof) {
            stdin_idx = nfds;
            pfd[nfds++] = (struct pollfd){.fd = STDIN_FILENO, .events = POLLIN};
        }

        int prc = poll(pfd, (nfds_t)nfds, timeout_ms);
        if (prc < 0) {
            if (errno == EINTR) continue;
            perror("ds4-agent: poll");
            rc = 1;
            break;
        }
        if (pfd[wake_idx].revents & POLLIN) drain_wake_fd(worker.wake_fd[0]);
        if (stdin_idx >= 0 && (pfd[stdin_idx].revents & (POLLIN | POLLHUP))) {
            size_t old_len = input.len;
            if (agent_read_stdin_available(&input, &stdin_eof) != 0) {
                rc = 1;
                break;
            }
            if (input.len != old_len) {
                quiet_deadline = now_sec() + 0.200;
                waiting_announced = false;
            }
        }

        char *out = NULL;
        size_t out_len = 0;
        agent_status st = {0};
        worker_consume(&worker, &out, &out_len, &st);
        if (out && out_len) {
            write_all(STDOUT_FILENO, out, out_len);
            fflush(stdout);
        }
        free(out);

        if (worker_take_queued_user_drain_request(&worker)) {
            char *queued = agent_prompt_queue_take_all(&queue);
            worker_answer_queued_user_drain(&worker, queued);
        }

        if (st.state == AGENT_WORKER_ERROR) {
            fprintf(stderr, "ds4-agent: %s\n",
                    st.error[0] ? st.error : "worker error");
            rc = 1;
            break;
        }

        if (!one_shot && input.len > 0 &&
            (stdin_eof || now_sec() >= quiet_deadline))
        {
            char *prompt = agent_input_buf_take(&input);
            if (worker_is_idle(&worker) && queue.len == 0) {
                if (!worker_submit(&worker, prompt)) {
                    agent_prompt_queue_push(&queue, prompt);
                    agent_noninteractive_marker("+DWARFSTAR_QUEUED");
                }
            } else {
                agent_prompt_queue_push(&queue, prompt);
                agent_noninteractive_marker("+DWARFSTAR_QUEUED");
            }
            free(prompt);
            waiting_announced = false;
        }

        if (one_shot && one_shot_submitted && worker_is_idle(&worker)) break;
        if (!one_shot && stdin_eof && input.len == 0 &&
            queue.len == 0 && worker_is_idle(&worker))
            break;
    }

    /* Drain anything published between the final status transition and the
     * loop exit.  This keeps stdout complete without adding another protocol. */
    char *out = NULL;
    size_t out_len = 0;
    worker_consume(&worker, &out, &out_len, NULL);
    if (out && out_len) {
        write_all(STDOUT_FILENO, out, out_len);
        fflush(stdout);
    }
    free(out);

    if (stdin_nonblock) fcntl(STDIN_FILENO, F_SETFL, old_stdin_flags);
    agent_input_buf_free(&input);
    agent_prompt_queue_free(&queue);
    agent_worker_free(&worker);
    return rc;
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
    if (cfg->default_tools[0])
        ds4_agent_tool_policy_parse(cfg->default_tools, &w->tool_policy);
    else
        ds4_agent_tool_policy_parse("all", &w->tool_policy);
}

typedef struct {
    agent_worker *worker;
    bool acquired;
} agent_gate_test_ctx;

static void *test_agent_gate_thread(void *arg) {
    agent_gate_test_ctx *ctx = arg;
    agent_worker_model_gate_lock(ctx->worker, AGENT_WORKER_IDLE);
    ctx->acquired = true;
    agent_worker_model_gate_unlock(ctx->worker);
    return NULL;
}

static void test_agent_worker_model_gate_serializes(void) {
    agent_config cfg = {0};
    agent_worker w;
    test_agent_fake_worker_init(&w, &cfg);
    pthread_mutex_t gate;
    pthread_mutex_init(&gate, NULL);
    w.model_gate = &gate;
    pthread_mutex_lock(&gate);

    agent_gate_test_ctx ctx = {.worker = &w};
    pthread_t thread;
    AGENT_TEST_ASSERT(pthread_create(&thread, NULL,
                                     test_agent_gate_thread, &ctx) == 0);
    usleep(20000);
    AGENT_TEST_ASSERT(!ctx.acquired);
    pthread_mutex_lock(&w.mu);
    AGENT_TEST_ASSERT(w.status.state == AGENT_WORKER_WAITING_MODEL);
    pthread_mutex_unlock(&w.mu);

    pthread_mutex_unlock(&gate);
    pthread_join(thread, NULL);
    AGENT_TEST_ASSERT(ctx.acquired);
    pthread_mutex_destroy(&gate);
    agent_worker_free(&w);
}
#endif

/* Main UI loop.  poll() multiplexes stdin with the worker wake pipe; all
 * terminal writes go through editor_write_async() so linenoise, status footer,
 * model output, and tool output never race each other. */
static int run_agent(ds4_engine *engine, agent_config *cfg) {
    ds4_agent_subagents *subagents = NULL;
    if (ds4_agent_subagents_create_for_agent(&subagents, engine, cfg) != 0) {
        fprintf(stderr, "ds4-agent: %s\n",
                ds4_agent_subagents_last_error(subagents));
        ds4_agent_subagents_destroy(subagents);
        return 1;
    }
    agent_worker *worker_ptr = ds4_agent_subagents_active_worker(subagents);
    agent_prompt_queue *queue_ptr = ds4_agent_subagents_active_queue(subagents);
    if (!worker_ptr || !queue_ptr) {
        ds4_agent_subagents_destroy(subagents);
        return 1;
    }
#define worker (*worker_ptr)
#define queue (*queue_ptr)

    char hist[PATH_MAX];
    const char *home = getenv("HOME");
    if (!home || !home[0]) home = ".";
    snprintf(hist, sizeof(hist), "%s/.ds4_agent_history", home);
    /* The agent uses ANSI scroll regions when possible: model/tool output
     * scrolls above the live linenoise prompt and status footer, so streaming
     * tokens do not require repainting the bottom rows.  Terminals without
     * scroll-region support fall back to the older prompt-below-output path. */
    linenoiseSetMultiLine(1);
    linenoiseHistorySetMaxLen(512);
    linenoiseHistoryLoad(hist);
    agent_completion_worker = &worker;
    linenoiseSetCompletionCallback(agent_switch_completion_callback);

    ds4_agent_subagents_update_worker_metadata(subagents);
    agent_status st;
    worker_get_status(&worker, &st);
    char prompt[160];
    char statusline[4096];
    build_prompt_text(&st, prompt, sizeof(prompt));
    build_footer_text(&st, subagents, NULL, 80, statusline, sizeof(statusline));

    agent_editor editor = {0};
    if (editor_start(&editor, prompt, statusline, NULL) != 0) {
        fprintf(stderr, "ds4-agent: failed to start line editor\n");
        ds4_agent_subagents_destroy(subagents);
        return 1;
    }
    editor_write_welcome_banner(&editor, cfg, prompt, statusline);

    char *initial_pending = cfg->gen.prompt && cfg->gen.prompt[0] ?
                            xstrdup(cfg->gen.prompt) : NULL;

    bool running = true;
    bool exit_save_handled = false;
    bool show_welcome_after_restart = false;
    bool force_status_redraw_after_restart = false;
    char *restore_line = NULL;
    while (running) {
        ds4_agent_subagents_update_worker_metadata(subagents);
        worker_ptr = ds4_agent_subagents_active_worker(subagents);
        queue_ptr = ds4_agent_subagents_active_queue(subagents);
        if (!worker_ptr || !queue_ptr) break;
        agent_completion_worker = worker_ptr;
        /* If a bash child process changed the terminal mode (e.g., from raw
         * to cooked), restore raw mode so linenoise continues to work. */
        if (ds4_agent_subagents_check_raw_mode_restore(subagents)) {
            linenoiseRestoreRawMode();
        }
        size_t subagent_fds = ds4_agent_subagents_worker_count(subagents);
        struct pollfd *pfd = xmalloc((1 + subagent_fds) * sizeof(pfd[0]));
        pfd[0] = (struct pollfd){.fd = STDIN_FILENO, .events = POLLIN};
        for (size_t i = 0; i < subagent_fds; i++) {
            pfd[1 + i] = (struct pollfd){
                .fd = ds4_agent_subagents_worker_fd_at(subagents, i),
                .events = POLLIN,
            };
        }
        bool alt_tab_prefix_waiting =
            editor_alt_tab_prefix_waiting(&editor, now_sec());
        int timeout = (!editor.paste_open && !editor.paste_start_pending &&
                       linenoiseEditQueuedInput(&editor.edit) > 0 &&
                       !alt_tab_prefix_waiting) ? 0 :
                      (alt_tab_prefix_waiting ? 10 : 100);
        int rc = poll(pfd, (nfds_t)(1 + subagent_fds), timeout);
        if (rc < 0 && errno != EINTR) {
            free(pfd);
            break;
        }

        if (agent_sigint) {
            agent_sigint = 0;
            if (worker_is_idle(&worker)) {
                editor_cancel_input_with_hint(&editor, prompt, statusline);
            } else {
                worker_interrupt(&worker);
            }
        }

        if (rc > 0 && (pfd[0].revents & POLLIN)) editor_read_stdin(&editor);

        /* Linenoise runs the terminal in raw mode, so Ctrl+C normally arrives
         * as byte 3 instead of SIGINT.  Handle it before worker output is
         * drained and repainted; otherwise a busy decoding stream can leave the
         * interrupt waiting behind a large terminal-output backlog. */
        if (editor_take_queued_byte(&editor, 3)) { /* Ctrl+C */
            if (!worker_is_idle(&worker)) {
                worker_interrupt(&worker);
            } else {
                editor_cancel_input_with_hint(&editor, prompt, statusline);
            }
        }

        if (rc > 0)
            ds4_agent_subagents_drain_wake_fds(subagents, pfd + 1, subagent_fds);
        free(pfd);

        char *out = NULL;
        size_t out_len = 0;
        char *notifications = NULL;
        ds4_agent_subagents_drain_outputs(subagents, &out, &out_len, &st,
                                         &notifications);
        build_prompt_text(&st, prompt, sizeof(prompt));
        int footer_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
        build_footer_text(&st, subagents, &queue, footer_cols, statusline, sizeof(statusline));
        if (notifications && notifications[0]) {
            editor_write_async(&editor, notifications, strlen(notifications),
                               prompt, statusline, true);
        }
        if (out && out_len) {
            bool force_show = st.state == AGENT_WORKER_IDLE ||
                              st.state == AGENT_WORKER_ERROR ||
                              st.state == AGENT_WORKER_STOPPED;
            editor_write_async(&editor, out, out_len, prompt, statusline, force_show);
        } else {
            editor_set_prompt_status(&editor, prompt, statusline);
            if (editor.hidden && (st.state == AGENT_WORKER_IDLE ||
                                  st.state == AGENT_WORKER_ERROR ||
                                  st.state == AGENT_WORKER_STOPPED))
                editor_show(&editor);
        }
        if (st.state == AGENT_WORKER_ERROR && st.error[0]) {
            char msg[320];
            int n = snprintf(msg, sizeof(msg), "\nds4-agent: %s\n", st.error);
            editor_write_async(&editor, msg, n > 0 ? (size_t)n : 0,
                               prompt, statusline, true);
            pthread_mutex_lock(&worker.mu);
            worker.status.state = AGENT_WORKER_IDLE;
            worker.status.prefill_tps = 0.0;
            worker.status.greedy_sampling = false;
            worker.status.error[0] = '\0';
            pthread_mutex_unlock(&worker.mu);
        }
        free(notifications);
        free(out);

        if (ds4_agent_subagents_take_queued_user_drain(subagents)) {
            continue;
        }

        char web_approval_msg[256];
        ds4_agent_subagent_id web_approval_id = {0};
        if (ds4_agent_subagents_take_web_approval(subagents, &web_approval_id,
                                                 web_approval_msg,
                                                 sizeof(web_approval_msg)))
        {
            char *saved_input = NULL;
            if (editor.active && editor.edit.buf && editor.edit.len)
                saved_input = xstrndup(editor.edit.buf, editor.edit.len);
            editor_stop(&editor);
            editor_restore_terminal_layout(&editor);
            agent_yes_no_options approval_opts = {
                .timeout_sec = 30,
                .timeout_answer = AGENT_YES_NO_AUTO_NO,
            };
            bool approval_timed_out = false;
            bool allow = agent_prompt_yes_no_ex(web_approval_msg,
                                                &approval_opts,
                                                &approval_timed_out);
            ds4_agent_subagents_answer_web_approval(subagents, web_approval_id,
                allow,
                approval_timed_out ? "Chrome browser start approval timed out" : NULL);
            worker_ptr = ds4_agent_subagents_active_worker(subagents);
            queue_ptr = ds4_agent_subagents_active_queue(subagents);
            worker_get_status(&worker, &st);
            build_prompt_text(&st, prompt, sizeof(prompt));
            int restart_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
            build_footer_text(&st, subagents, &queue, restart_cols, statusline, sizeof(statusline));
            editor_start(&editor, prompt, statusline, saved_input);
            free(saved_input);
            continue;
        }

        char path_approval_msg[PATH_MAX + 256];
        char path_approval_options[3][PATH_MAX];
        int path_approval_option_count = 0;
        ds4_agent_subagent_id path_approval_id = {0};
        if (ds4_agent_subagents_take_path_approval(subagents, &path_approval_id,
                                                  path_approval_msg,
                                                  sizeof(path_approval_msg),
                                                  path_approval_options,
                                                  &path_approval_option_count))
        {
            char *saved_input = NULL;
            if (editor.active && editor.edit.buf && editor.edit.len)
                saved_input = xstrndup(editor.edit.buf, editor.edit.len);
            editor_stop(&editor);
            editor_restore_terminal_layout(&editor);
            bool approval_timed_out = false;
            char approved_dir[PATH_MAX] = {0};
            bool allow = agent_prompt_working_directory_choice(
                path_approval_msg, path_approval_options,
                path_approval_option_count, approved_dir, &approval_timed_out);
            ds4_agent_subagents_answer_path_approval(subagents, path_approval_id,
                allow, approved_dir,
                approval_timed_out ? "working directory approval timed out" : NULL);
            worker_ptr = ds4_agent_subagents_active_worker(subagents);
            queue_ptr = ds4_agent_subagents_active_queue(subagents);
            worker_get_status(&worker, &st);
            build_prompt_text(&st, prompt, sizeof(prompt));
            int restart_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
            build_footer_text(&st, subagents, &queue, restart_cols, statusline, sizeof(statusline));
            editor_start(&editor, prompt, statusline, saved_input);
            free(saved_input);
            continue;
        }

        char question_msg[AGENT_ASK_QUESTION_TEXT_MAX];
        char question_choices[AGENT_ASK_QUESTION_MAX_CHOICES][AGENT_ASK_QUESTION_CHOICE_MAX];
        int question_choice_count = 0;
        ds4_agent_subagent_id question_id = {0};
        if (ds4_agent_subagents_take_question(subagents, &question_id,
                                             question_msg,
                                             sizeof(question_msg),
                                             question_choices,
                                             &question_choice_count))
        {
            char *saved_input = NULL;
            if (editor.active && editor.edit.buf && editor.edit.len)
                saved_input = xstrndup(editor.edit.buf, editor.edit.len);
            editor_stop(&editor);
            editor_restore_terminal_layout(&editor);
            bool question_interrupted = false;
            char answer[AGENT_ASK_QUESTION_ANSWER_MAX] = {0};
            bool answered = agent_prompt_ask_question(
                question_msg, question_choices, question_choice_count, answer,
                &question_interrupted);
            ds4_agent_subagents_answer_question(
                subagents, question_id, question_interrupted, answered ? answer : NULL,
                (!answered && !question_interrupted) ? "question prompt failed" : NULL);
            worker_ptr = ds4_agent_subagents_active_worker(subagents);
            queue_ptr = ds4_agent_subagents_active_queue(subagents);
            worker_get_status(&worker, &st);
            build_prompt_text(&st, prompt, sizeof(prompt));
            int restart_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
            build_footer_text(&st, subagents, &queue, restart_cols, statusline, sizeof(statusline));
            editor_start(&editor, prompt, statusline, saved_input);
            free(saved_input);
            continue;
        }

        ds4_agent_subagents_submit_ready(subagents);
        worker_ptr = ds4_agent_subagents_active_worker(subagents);
        queue_ptr = ds4_agent_subagents_active_queue(subagents);
        if (!worker_ptr || !queue_ptr) break;

        if (initial_pending && worker_is_idle(&worker)) {
            if (worker_submit(&worker, initial_pending)) {
                free(initial_pending);
                initial_pending = NULL;
            }
        }

        if (!initial_pending && queue.len && worker_is_idle(&worker)) {
            char *queued = agent_prompt_queue_take_all(&queue);
            if (worker_submit(&worker, queued)) {
                linenoiseHistoryAdd(queued);
                linenoiseHistorySave(hist);
            } else {
                agent_prompt_queue_push_front(&queue, queued);
                queued = NULL;
            }
            free(queued);
        }

        if (queue.len && editor_take_queued_byte(&editor, 24)) { /* Ctrl+X */
            char *queued = agent_prompt_queue_pop(&queue);
            editor_replace_input(&editor, queued);
            worker_get_status(&worker, &st);
            build_prompt_text(&st, prompt, sizeof(prompt));
            footer_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
            build_footer_text(&st, subagents, &queue, footer_cols, statusline, sizeof(statusline));
            editor_set_prompt_status(&editor, prompt, statusline);
            free(queued);
        }
        if (queue.len && !worker_is_idle(&worker) && editor_take_bare_escape(&editor)) {
            worker_interrupt(&worker);
        }

        if (editor_take_alt_tab(&editor)) {
            ds4_think_mode mode = worker_cycle_think_mode(&worker);
            worker_get_status(&worker, &st);
            build_prompt_text(&st, prompt, sizeof(prompt));
            footer_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
            build_footer_text(&st, subagents, &queue, footer_cols, statusline, sizeof(statusline));
            char msg[96];
            int n = snprintf(msg, sizeof(msg), "\n%s\n",
                             agent_think_mode_confirmation(mode));
            editor_write_async(&editor, msg, n > 0 ? (size_t)n : 0,
                               prompt, statusline, true);
            continue;
        }

        if (!editor.paste_open && !editor.paste_start_pending &&
            linenoiseEditQueuedInput(&editor.edit) > 0 &&
            !editor_alt_tab_prefix_waiting(&editor, now_sec()))
        {
            if (editor.hidden) {
                /* A user key while the model is in the middle of a partial
                 * output line means the prompt must become visible again. End
                 * the model line explicitly; otherwise linenoise would redraw
                 * on top of generated text. */
                editor_show(&editor);
            }
            errno = 0;
            char *line = linenoiseEditFeed(&editor.edit);
            if (line == linenoiseEditMore) {
                /* Still editing. */
            } else if (!line) {
                if (errno == EAGAIN) {
                    if (!worker_is_idle(&worker)) {
                        worker_interrupt(&worker);
                    } else {
                        editor_cancel_input_with_hint(&editor, prompt, statusline);
                    }
                } else {
                    running = false;
                }
            } else {
                char *cmd = line;
                while (*cmd == ' ' || *cmd == '\t' || *cmd == '\r' || *cmd == '\n') cmd++;
                char *end = cmd + strlen(cmd);
                while (end > cmd && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\r' || end[-1] == '\n')) end--;
                *end = '\0';

                bool was_below_output = editor.prompt_below_output;
                bool had_output_line_open = editor.output_line_open;
                int saved_output_col = editor.output_col;
                bool should_echo = cmd[0] != '\0' &&
                    !(cmd[0] == '/' && !agent_slash_command_known(cmd));
                if (should_echo) {
                    char *echo = agent_format_user_prompt_echo(cmd);
                    editor_write_async(&editor, echo, strlen(echo),
                                       prompt, statusline, true);
                    free(echo);
                }
                editor_stop(&editor);
                linenoiseHistoryAdd(cmd);
                linenoiseHistorySave(hist);
                bool busy = !worker_is_idle(&worker);
                if (!cmd[0]) {
                    /* Empty input: just reopen the editor. */
                } else if (!strcmp(cmd, "/help")) {
                    runtime_help();
                } else if (!strcmp(cmd, "/save")) {
                    if (busy) {
                        worker_request_save(&worker);
                        printf("save scheduled at next safe point\n");
                    } else {
                        char err[160] = {0};
                        if (!agent_worker_save_session(&worker, err, sizeof(err)))
                            printf("save failed: %s\n", err);
                    }
                } else if (!strcmp(cmd, "/compact")) {
                    worker_request_compact(&worker);
                    if (busy)
                        printf("compaction scheduled at next safe point\n");
                } else if (!strcmp(cmd, "/list")) {
                    agent_worker_list_sessions(&worker);
                } else if (!strcmp(cmd, "/strict_sandbox")) {
                    worker.cfg->strict_sandbox = true;
                    printf("strict sandbox enabled\n");
                } else if (!strcmp(cmd, "/no_strict_sandbox")) {
                    worker.cfg->strict_sandbox = false;
                    printf("strict sandbox disabled\n");
                } else if (!strncmp(cmd, "/command_output", 15) &&
                           (cmd[15] == '\0' || cmd[15] == ' ' || cmd[15] == '\t')) {
                    char *arg = cmd + 15;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!strcmp(arg, "on")) {
                        worker.cfg->command_output = true;
                        printf("command output enabled\n");
                    } else if (!strcmp(arg, "off")) {
                        worker.cfg->command_output = false;
                        printf("command output disabled\n");
                    } else {
                        printf("usage: /command_output on|off\n");
                    }
                } else if (!strcmp(cmd, "/preserve_agent_files")) {
                    worker.cfg->preserve_agent_files = true;
                    printf("agent temp files will be preserved\n");
                } else if (!strcmp(cmd, "/no_preserve_agent_files")) {
                    worker.cfg->preserve_agent_files = false;
                    printf("agent temp files will be deleted after 5 idle minutes\n");
                } else if (!strcmp(cmd, "/docker help")) {
                    runtime_docker_help();
                } else if (!strcmp(cmd, "/docker debug")) {
                    worker.cfg->docker_debug = !worker.cfg->docker_debug;
                    printf("docker debug %s\n",
                           worker.cfg->docker_debug ? "enabled" : "disabled");
                } else if (!strcmp(cmd, "/docker list")) {
                    agent_command_docker_list(&worker);
                } else if (!strncmp(cmd, "/docker create", 14) &&
                           (cmd[14] == '\0' || cmd[14] == ' ' || cmd[14] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 14;
                        agent_command_docker_create(&worker, arg);
                    }
                } else if (!strncmp(cmd, "/docker use", 11) &&
                           (cmd[11] == '\0' || cmd[11] == ' ' || cmd[11] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 11;
                        agent_command_docker_use(&worker, arg);
                    }
                } else if (!strncmp(cmd, "/docker describe", 16) &&
                           (cmd[16] == '\0' || cmd[16] == ' ' || cmd[16] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 16;
                        agent_command_docker_describe(&worker, arg);
                    }
                } else if (!strncmp(cmd, "/docker destroy", 15) &&
                           (cmd[15] == '\0' || cmd[15] == ' ' || cmd[15] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 15;
                        agent_command_docker_destroy(&worker, arg);
                    }
                } else if (!strncmp(cmd, "/docker stop", 12) &&
                           (cmd[12] == '\0' || cmd[12] == ' ' || cmd[12] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 12;
                        agent_command_docker_stop(&worker, arg);
                    }
                } else if (!strncmp(cmd, "/power", 6) &&
                           (cmd[6] == '\0' || cmd[6] == ' ' || cmd[6] == '\t')) {
                    char *arg = cmd + 6;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        printf("usage: /power <1..100>\n");
                    } else {
                        int power = 0;
                        if (!parse_power_percent(arg, &power)) {
                            printf("usage: /power <1..100>\n");
                        } else {
                            worker_request_power(&worker, power);
                        }
                    }
                } else if (!strncmp(cmd, "/thinking", 9) &&
                           (cmd[9] == '\0' || cmd[9] == ' ' || cmd[9] == '\t')) {
                    char *arg = cmd + 9;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        printf("usage: /thinking off|default|max\n");
                    } else if (!strcmp(arg, "off")) {
                        worker_set_think_mode(&worker, DS4_THINK_NONE);
                        printf("%s\n", agent_think_mode_confirmation(DS4_THINK_NONE));
                    } else if (!strcmp(arg, "default")) {
                        worker_set_think_mode(&worker, DS4_THINK_HIGH);
                        printf("%s\n", agent_think_mode_confirmation(DS4_THINK_HIGH));
                    } else if (!strcmp(arg, "max")) {
                        worker_set_think_mode(&worker, DS4_THINK_MAX);
                        printf("%s\n", agent_think_mode_confirmation(DS4_THINK_MAX));
                    } else {
                        printf("usage: /thinking off|default|max\n");
                    }
                } else if (!strncmp(cmd, "/workspace", 10) &&
                           (cmd[10] == '\0' || cmd[10] == ' ' || cmd[10] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 10;
                        while (*arg == ' ' || *arg == '\t') arg++;
                        if (!arg[0]) {
                            agent_worker_print_workspaces(&worker);
                        } else if (arg[0] == '+' || arg[0] == '-') {
                            char op = arg[0];
                            arg++;
                            while (*arg == ' ' || *arg == '\t') arg++;
                            if (!arg[0]) {
                                printf("usage: /workspace +/path/to or /workspace -/path/to\n");
                            } else {
                                char resolved[PATH_MAX] = {0};
                                char err[PATH_MAX + 160] = {0};
                                if (op == '+') {
                                    if (agent_worker_add_workspace(&worker, arg,
                                                                   resolved, sizeof(resolved),
                                                                   err, sizeof(err))) {
                                        printf("added workspace %s\n", resolved);
                                        char docker_err[256] = {0};
                                        if (!agent_docker_refresh_mounts(&worker,
                                                                         docker_err,
                                                                         sizeof(docker_err)))
                                            printf("docker sandbox mount update failed: %s\n",
                                                   docker_err);
                                    } else
                                        printf("workspace add failed: %s\n", err);
                                } else {
                                    if (agent_worker_remove_workspace(&worker, arg,
                                                                      resolved, sizeof(resolved),
                                                                      err, sizeof(err))) {
                                        printf("removed workspace %s\n", resolved);
                                        char docker_err[256] = {0};
                                        if (!agent_docker_refresh_mounts(&worker,
                                                                         docker_err,
                                                                         sizeof(docker_err)))
                                            printf("docker sandbox mount update failed: %s\n",
                                                   docker_err);
                                    } else
                                        printf("workspace remove failed: %s\n", err);
                                }
                            }
                        } else {
                            printf("usage: /workspace, /workspace +/path/to, /workspace -/path/to\n");
                        }
                    }
                } else if (!strcmp(cmd, "/purge_auto_files")) {
                    pthread_mutex_lock(&worker.mu);
                    int n = worker.temp_files.len;
                    if (n == 0) {
                        pthread_mutex_unlock(&worker.mu);
                        printf("no temp files to purge\n");
                    } else {
                        /* Copy list while holding the lock. */
                        char **paths = xmalloc((size_t)n * sizeof(char *));
                        for (int i = 0; i < n; i++)
                            paths[i] = xstrdup(worker.temp_files.v[i].path);
                        pthread_mutex_unlock(&worker.mu);

                        printf("temp files (%d):\n", n);
                        for (int i = 0; i < n; i++)
                            printf("  %d. %s\n", i + 1, paths[i]);

                        printf("\nDeletion will start in 5 seconds. Press Ctrl+C or type 'q' to abort...\n");
                        fflush(stdout);

                        bool aborted = false;
                        struct pollfd pfd = { .fd = STDIN_FILENO, .events = POLLIN };
                        int remaining_ms = 5000;
                        while (remaining_ms > 0) {
                            int rc = poll(&pfd, 1, remaining_ms);
                            if (rc < 0) {
                                if (errno == EINTR) {
                                    /* SIGINT/SIGTERM received — treat as abort. */
                                    aborted = true;
                                    break;
                                }
                                break;
                            }
                            if (rc == 0) break; /* timeout */
                            /* Data available on stdin — read a char. */
                            char ch = 0;
                            ssize_t nr = read(STDIN_FILENO, &ch, 1);
                            if (nr == 1 && (ch == 'q' || ch == 0x03)) {
                                aborted = true;
                                break;
                            }
                            break;
                        }
                        if (aborted) {
                            printf("aborted by user.\n");
                        } else {
                            printf("proceeding with deletion...\n");
                            pthread_mutex_lock(&worker.mu);
                            int deleted = 0;
                            int stale = 0;
                            int failed = 0;
                            for (int i = 0; i < n; i++) {
                                /* Skip entries where the file was already
                                 * deleted externally (e.g. OS tmp cleanup). */
                                if (access(paths[i], F_OK) != 0) {
                                    printf("  stale entry (already deleted): %s\n",
                                           paths[i]);
                                    agent_temp_files_remove(&worker.temp_files,
                                                            paths[i], false);
                                    agent_path_list_remove(&worker.auto_allowed_paths,
                                                           paths[i], false);
                                    stale++;
                                    continue;
                                }
                                if (unlink(paths[i]) == 0) {
                                    printf("  deleted: %s\n", paths[i]);
                                    agent_temp_files_remove(&worker.temp_files,
                                                            paths[i], false);
                                    agent_path_list_remove(&worker.auto_allowed_paths,
                                                           paths[i], false);
                                    deleted++;
                                } else {
                                    printf("  failed to delete: %s (%s)\n",
                                           paths[i], strerror(errno));
                                    failed++;
                                }
                            }
                            pthread_mutex_unlock(&worker.mu);
                            printf("\n%d file%s deleted, %d stale, %d failed.\n",
                                   deleted, deleted == 1 ? "" : "s", stale, failed);
                        }
	                        for (int i = 0; i < n; i++) free(paths[i]);
	                        free(paths);
	                    }
                } else if (!strncmp(cmd, "/allow", 6) &&
                           (cmd[6] == '\0' || cmd[6] == ' ' || cmd[6] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 6;
                        while (*arg == ' ' || *arg == '\t') arg++;
                        if (!arg[0]) {
                            printf("usage: /allow <tools> (e.g. /allow read,bash)\n");
                        } else {
                            char err[256];
                            if (!agent_worker_apply_tool_policy_command(
                                    &worker, true, arg, err, sizeof(err))) {
                                printf("error: %s\n", err);
                            } else {
                                char summary[256];
                                ds4_agent_tool_policy_format(&worker.tool_policy,
                                                             summary, sizeof(summary));
                                printf("tool-access policy updated: %s\n", summary);
                                char *reminder =
                                    agent_build_system_prompt_reminder(&worker);
                                agent_publish_system_status(
                                    &worker,
                                    "Tool policy changed; re-injecting system prompt...");
                                ds4_tokenize_rendered_chat(worker.engine, reminder,
                                                           &worker.transcript);
                                free(reminder);
                                agent_worker_note_system_prompt_seen(&worker);
                            }
                        }
                    }
                } else if (!strncmp(cmd, "/disallow", 9) &&
                            (cmd[9] == '\0' || cmd[9] == ' ' || cmd[9] == '\t')) {
                    if (busy) {
                        printf("command requires the model to be idle: %s\n", cmd);
                    } else {
                        char *arg = cmd + 9;
                        while (*arg == ' ' || *arg == '\t') arg++;
                        if (!arg[0]) {
                            printf("usage: /disallow <tools> (e.g. /disallow web or /disallow all)\n");
                        } else {
                            char err[256];
                            if (!agent_worker_apply_tool_policy_command(
                                    &worker, false, arg, err, sizeof(err))) {
                                printf("error: %s\n", err);
                            } else {
                                char summary[256];
                                ds4_agent_tool_policy_format(&worker.tool_policy,
                                                             summary, sizeof(summary));
                                printf("tool-access policy updated: %s\n", summary);
                                char *reminder =
                                    agent_build_system_prompt_reminder(&worker);
                                agent_publish_system_status(
                                    &worker,
                                    "Tool policy changed; re-injecting system prompt...");
                                ds4_tokenize_rendered_chat(worker.engine, reminder,
                                                           &worker.transcript);
                                free(reminder);
                                agent_worker_note_system_prompt_seen(&worker);
                            }
                        }
                    }
                } else if (agent_slash_command_with_args(cmd, "/skills")) {
                    /* Parse subcommand */
                    char *arg = cmd + 8;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        if (busy) printf("command requires the model to be idle: %s\n", cmd);
                        else printf("usage: /skills add <path>, /skills list, /skills del <name>\n");
                    } else if (!strncmp(arg, "add ", 4) || !strncmp(arg, "add\t", 4)) {
                        if (busy) {
                            printf("command requires the model to be idle: %s\n", cmd);
                        } else {
                            const char *path = arg + 4;
                            while (*path == ' ' || *path == '\t') path++;
                            if (!path[0]) {
                                printf("usage: /skills add <path>\n");
                            } else {
                                /* Resolve relative path */
                                char abs_path[PATH_MAX];
                                if (path[0] != '/') {
                                    char cwd[PATH_MAX];
                                    if (getcwd(cwd, sizeof(cwd))) {
                                        int n = snprintf(abs_path, sizeof(abs_path),
                                                         "%s/%s", cwd, path);
                                        if (n < 0 || (size_t)n >= sizeof(abs_path)) {
                                            printf("error: skill path too long\n");
                                            goto skills_done;
                                        }
                                    } else {
                                        printf("error: failed to get cwd\n");
                                        goto skills_done;
                                    }
                                } else {
                                    int n = snprintf(abs_path, sizeof(abs_path), "%s", path);
                                    if (n < 0 || (size_t)n >= sizeof(abs_path)) {
                                        printf("error: skill path too long\n");
                                        goto skills_done;
                                    }
                                }
                                char resolved_path[PATH_MAX];
                                if (!realpath(abs_path, resolved_path)) {
                                    printf("error: path not found: %s\n", abs_path);
                                    goto skills_done;
                                }
                                struct stat st;
                                if (stat(resolved_path, &st) != 0) {
                                    printf("error: path not found: %s\n", resolved_path);
                                    goto skills_done;
                                }
                                if (!agent_skill_path_allowed(&worker, resolved_path)) {
                                    printf("error: path outside allowed workspace dirs\n");
                                    goto skills_done;
                                }
                                int count = 0;
                                if (S_ISDIR(st.st_mode)) {
                                    count = agent_skill_register_dir(&worker, resolved_path);
                                } else {
                                    int ret = agent_skill_register(&worker, resolved_path);
                                    if (ret == 0) count = 1;
                                    else if (ret == -6) printf("error: duplicate skill name\n");
                                    else if (ret == -5) printf("error: path outside allowed workspace dirs\n");
                                    else if (ret == -4) printf("error: invalid skill name\n");
                                    else if (ret == -3) printf("error: non-UTF-8 content\n");
                                    else if (ret == -2) printf("error: no valid frontmatter\n");
                                    else printf("error: failed to register skill (code %d)\n", ret);
                                }
                                if (count > 0) {
                                    printf("registered %d skill(s)\n", count);
                                    char *reminder = agent_build_system_prompt_reminder(
                                        &worker);
                                    agent_publish_system_status(&worker,
                                        "Skills changed; re-injecting system prompt...");
                                    ds4_tokenize_rendered_chat(worker.engine, reminder,
                                                               &worker.transcript);
                                    free(reminder);
                                    agent_worker_note_system_prompt_seen(&worker);
                                    agent_worker_note_skills_prompt_seen(&worker);
                                }
                            }
                        }
                    } else if (!strcmp(arg, "list")) {
                        if (busy) {
                            printf("command requires the model to be idle: %s\n", cmd);
                        } else {
                            printf("Registered skills:\n");
                            agent_skill_registry *registry = worker.skill_registry;
                            pthread_mutex_lock(&registry->mu);
                            for (agent_skill *s = registry->head; s; s = s->next)
                                printf("  @%s\n    - %s\n    - path:%s)\n", s->name, s->description, s->path);
                            if (!registry->head) printf("  (none)\n");
                            pthread_mutex_unlock(&registry->mu);
                        }
                    } else if (!strncmp(arg, "del ", 4) || !strncmp(arg, "del\t", 4)) {
                        if (busy) {
                            printf("command requires the model to be idle: %s\n", cmd);
                        } else {
                            const char *name = arg + 4;
                            while (*name == ' ' || *name == '\t') name++;
                            if (!name[0]) {
                                printf("usage: /skills del <name>\n");
                            } else {
                                agent_skill_registry *registry = worker.skill_registry;
                                pthread_mutex_lock(&registry->mu);
                                bool found = false;
                                for (agent_skill *s = registry->head; s; s = s->next) {
                                    if (strcmp(s->name, name) == 0) { found = true; break; }
                                }
                                pthread_mutex_unlock(&registry->mu);
                                if (!found) {
                                    printf("error: skill not found: %s\n", name);
                                } else {
                                    agent_skill_delete(&worker, name);
                                    printf("deleted skill: %s\n", name);
                                    char *reminder = agent_build_system_prompt_reminder(
                                        &worker);
                                    agent_publish_system_status(&worker,
                                        "Skills changed; re-injecting system prompt...");
                                    ds4_tokenize_rendered_chat(worker.engine, reminder,
                                                               &worker.transcript);
                                    free(reminder);
                                    agent_worker_note_system_prompt_seen(&worker);
                                    agent_worker_note_skills_prompt_seen(&worker);
                                }
                            }
                        }
                    } else {
                        if (busy) printf("command requires the model to be idle: %s\n", cmd);
                        else printf("unknown /skills subcommand: %s\n", arg);
                    }
skills_done: ;
                } else if (ds4_agent_subagents_handle_command(subagents, cmd, busy)) {
                    worker_ptr = ds4_agent_subagents_active_worker(subagents);
                    queue_ptr = ds4_agent_subagents_active_queue(subagents);
                } else if (cmd[0] == '/' && !agent_slash_command_known(cmd)) {
                    ssize_t ignored = write(STDOUT_FILENO, "\a", 1);
                    (void)ignored;
                    restore_line = xstrdup(cmd);
                } else if (cmd[0] == '/' && busy) {
                    printf("command requires the model to be idle: %s\n", cmd);
                } else if (!strcmp(cmd, "/quit") || !strcmp(cmd, "/exit")) {
                    /* Stop the editor so raw mode and non-blocking stdin are
                     * disabled before we prompt the user.  Then restore the
                     * ANSI scroll region too; AGENT_EXIT_NOW exits directly. */
                    editor_stop(&editor);
                    editor_restore_terminal_layout(&editor);
                    agent_exit_save_result exit_save =
                        agent_maybe_save_before_exiting(&worker);
                    if (exit_save == AGENT_EXIT_NOW) {
                        exit(0);
                    } else if (exit_save == AGENT_EXIT_CLEAN) {
                        exit_save_handled = true;
                        running = false;
                    } else {
                        /* AGENT_EXIT_CANCEL: user declined to proceed after a
                         * save failure.  Reopen the editor and continue. */
                        editor_start(&editor, prompt, statusline, NULL);
                    }
                } else if (!strcmp(cmd, "/new")) {
                    editor_restore_terminal_layout(&editor);
                    if (agent_maybe_save_before_leaving_session(&worker)) {
                        char err[160] = {0};
                        if (!agent_worker_reset_to_sysprompt(&worker, err, sizeof(err))) {
                            printf("new session failed: %s\n", err);
                        } else {
                            show_welcome_after_restart = true;
                        }
                    }
                } else if (!strncmp(cmd, "/switch", 7) &&
                           (cmd[7] == '\0' || cmd[7] == ' ' || cmd[7] == '\t')) {
                    char *arg = cmd + 7;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        printf("usage: /switch <sha-prefix>\n");
                    } else {
                        editor_restore_terminal_layout(&editor);
                        if (agent_maybe_save_before_leaving_session(&worker)) {
                            char *sha = arg;
                            while (*arg && *arg != ' ' && *arg != '\t') arg++;
                            if (*arg) *arg = '\0';
                            char err[160] = {0};
                            if (!agent_worker_switch_session(&worker, sha,
                                                             AGENT_HISTORY_DEFAULT_TURNS,
                                                             err, sizeof(err)))
                                printf("switch failed: %s\n", err);
                            else
                                force_status_redraw_after_restart = true;
                        }
                    }
                } else if (!strncmp(cmd, "/del", 4) &&
                           (cmd[4] == '\0' || cmd[4] == ' ' || cmd[4] == '\t')) {
                    char *arg = cmd + 4;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        printf("usage: /del <sha-prefix>\n");
                    } else {
                        char *sha_arg = arg;
                        while (*arg && *arg != ' ' && *arg != '\t') arg++;
                        if (*arg) *arg = '\0';
                        char sha[41] = {0};
                        char err[160] = {0};
                        if (agent_worker_delete_session(&worker, sha_arg,
                                                        sha, err, sizeof(err)))
                            printf("deleted session %.8s\n", sha);
                        else
                            printf("delete failed: %s\n", err);
                    }
                } else if (!strncmp(cmd, "/strip", 6) &&
                           (cmd[6] == '\0' || cmd[6] == ' ' || cmd[6] == '\t')) {
                    char *arg = cmd + 6;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    if (!arg[0]) {
                        printf("usage: /strip <sha-prefix>\n");
                    } else {
                        char *sha_arg = arg;
                        while (*arg && *arg != ' ' && *arg != '\t') arg++;
                        if (*arg) *arg = '\0';
                        char sha[41] = {0};
                        uint32_t tokens = 0;
                        char err[160] = {0};
                        if (agent_worker_strip_session(&worker, sha_arg,
                                                       sha, &tokens,
                                                       err, sizeof(err)))
                            printf("stripped session %.8s (%u tokens)\n",
                                   sha, tokens);
                        else
                            printf("strip failed: %s\n", err);
                    }
                } else if (!strncmp(cmd, "/history", 8) &&
                           (cmd[8] == '\0' || cmd[8] == ' ' || cmd[8] == '\t')) {
                    char *arg = cmd + 8;
                    while (*arg == ' ' || *arg == '\t') arg++;
                    int history_turns = arg[0] ?
                        agent_parse_int_default(arg, AGENT_HISTORY_DEFAULT_TURNS,
                                                1, AGENT_HISTORY_MAX_TURNS) :
                        AGENT_HISTORY_DEFAULT_TURNS;
                    char err[160] = {0};
                    if (!agent_worker_show_history(&worker, history_turns,
                                                   err, sizeof(err)))
                        printf("history failed: %s\n", err);
                } else if (busy) {
                    agent_prompt_queue_push(&queue, cmd);
                } else {
                    linenoiseHistoryAdd(cmd);
                    linenoiseHistorySave(hist);
                    if (worker_submit(&worker, cmd)) {
                    } else {
                        restore_line = xstrdup(cmd);
                    }
                }
                linenoiseFree(line);

                if (running) {
                    worker_get_status(&worker, &st);
                    build_prompt_text(&st, prompt, sizeof(prompt));
                    int restart_cols = editor.edit.cols > 0 ? (int)editor.edit.cols : 80;
                    build_footer_text(&st, subagents, &queue, restart_cols, statusline, sizeof(statusline));
                    editor_start(&editor, prompt, statusline, restore_line);
                    if (!editor.scroll_region && was_below_output) {
                        editor.output_line_open = had_output_line_open;
                        editor.prompt_below_output = was_below_output;
                        editor.output_col = saved_output_col;
                    }
                    if (show_welcome_after_restart) {
                        editor_write_welcome_banner(&editor, cfg, prompt, statusline);
                        show_welcome_after_restart = false;
                    }
                    if (force_status_redraw_after_restart) {
                        editor_write_async(&editor, "", 0, prompt, statusline, true);
                        force_status_redraw_after_restart = false;
                    }
                    free(restore_line);
                    restore_line = NULL;
                }
            }
        }
    }

    free(initial_pending);
    free(restore_line);
    editor_stop(&editor);
    editor_restore_terminal_layout(&editor);
    linenoiseSetCompletionCallback(NULL);
    agent_completion_worker = NULL;
    worker_ptr = ds4_agent_subagents_active_worker(subagents);
    if (!exit_save_handled && worker_ptr) {
        agent_exit_save_result exit_save =
            agent_maybe_save_before_exiting(&worker);
        if (exit_save == AGENT_EXIT_NOW) exit(0);
    }
    ds4_agent_subagents_destroy(subagents);
#undef queue
#undef worker
    return 0;
}

#ifndef DS4_AGENT_TEST_NO_MAIN
int main(int argc, char **argv) {
    agent_config cfg = parse_options(argc, argv);
    char docker_version[128] = {0};
    const char *docker_command = cfg.docker_command ? cfg.docker_command : "docker";
    cfg.docker_available = false;
    bool color = isatty(STDOUT_FILENO) != 0;

    if ((agent_command_in_path(docker_command) || 
        agent_executable_exists(docker_command)) &&
        agent_read_docker_version(docker_command, docker_version,
                                  sizeof(docker_version))) {
        cfg.docker_available = true;
        fprintf(stdout, "ds4-agent: sandboxing via docker is available ");
        if (color) fprintf(stdout, "\x1b[38;5;81m");
        fprintf(stdout, "(docker %s)", docker_version);
        if (color) fprintf(stdout, "\x1b[0m");
        fputc('\n', stdout);
    } else {
        fprintf(stdout, "ds4-agent: docker sandbox feature is not available");
        if (cfg.docker_command && !agent_executable_exists(cfg.docker_command))
            fprintf(stdout, " (%s is not executable)", cfg.docker_command);
        else
            fprintf(stdout, " (%s unavailable)", docker_command);
        fputc('\n', stdout);
    }
    if (cfg.chdir_path && chdir(cfg.chdir_path) != 0) {
        fprintf(stderr, "ds4-agent: failed to chdir to %s: %s\n",
                cfg.chdir_path, strerror(errno));
        return 1;
    }
    char wd_err[PATH_MAX + 160];
    if (!agent_config_resolve_working_directory(&cfg, wd_err, sizeof(wd_err))) {
        fprintf(stderr, "ds4-agent: %s\n", wd_err);
        return 1;
    }
    agent_config_prepare_startup_docker_sandbox(&cfg);
    ds4_engine *engine = NULL;
    if (ds4_engine_open(&engine, &cfg.engine) != 0) return 1;
    log_context_memory(cfg.engine.backend,
                       cfg.gen.ctx_size,
                       cfg.engine.prefill_chunk);

    struct sigaction old_int;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = agent_sigint_handler;
    bool sigint_installed = !cfg.non_interactive &&
        sigaction(SIGINT, &sa, &old_int) == 0;

    int rc = cfg.non_interactive ?
        run_agent_non_interactive(engine, &cfg) :
        run_agent(engine, &cfg);

    if (sigint_installed) sigaction(SIGINT, &old_int, NULL);
    ds4_engine_close(engine);
    return rc;
}
#endif
