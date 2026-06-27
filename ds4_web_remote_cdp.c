#include "ds4_web_remote_cdp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *remote_cdp_xstrdup(const char *s) {
    if (!s) s = "";
    size_t n = strlen(s);
    char *p = malloc(n + 1);
    if (!p) {
        perror("ds4_web_remote_cdp: malloc");
        exit(1);
    }
    memcpy(p, s, n + 1);
    return p;
}

void ds4_web_cdp_endpoint_init(ds4_web_cdp_endpoint *ep,
                               const char *host,
                               int cdp_port,
                               int default_port) {
    if (!ep) return;
    memset(ep, 0, sizeof(*ep));
    ep->remote = (host && host[0]) || cdp_port > 0;
    snprintf(ep->host, sizeof(ep->host), "%s",
             host && host[0] ? host : "127.0.0.1");
    ep->port = cdp_port > 0 ? cdp_port : default_port;
    if (ep->port <= 0) ep->port = 9333;
}

bool ds4_web_cdp_endpoint_remote(const ds4_web_cdp_endpoint *ep) {
    return ep && ep->remote;
}

const char *ds4_web_cdp_endpoint_host(const ds4_web_cdp_endpoint *ep) {
    return ep && ep->host[0] ? ep->host : "127.0.0.1";
}

int ds4_web_cdp_endpoint_port(const ds4_web_cdp_endpoint *ep) {
    return ep && ep->port > 0 ? ep->port : 9333;
}

char *ds4_web_cdp_rewrite_ws_url(const ds4_web_cdp_endpoint *ep,
                                 const char *ws_url) {
    if (!ws_url) return remote_cdp_xstrdup("");
    if (!ds4_web_cdp_endpoint_remote(ep)) return remote_cdp_xstrdup(ws_url);
    if (strncmp(ws_url, "ws://", 5) != 0) return remote_cdp_xstrdup(ws_url);

    const char *path = strchr(ws_url + 5, '/');
    if (!path) return remote_cdp_xstrdup(ws_url);

    char prefix[320];
    snprintf(prefix, sizeof(prefix), "ws://%s:%d",
             ds4_web_cdp_endpoint_host(ep),
             ds4_web_cdp_endpoint_port(ep));
    size_t prefix_len = strlen(prefix);
    size_t path_len = strlen(path);
    char *out = malloc(prefix_len + path_len + 1);
    if (!out) {
        perror("ds4_web_remote_cdp: malloc");
        exit(1);
    }
    memcpy(out, prefix, prefix_len);
    memcpy(out + prefix_len, path, path_len + 1);
    return out;
}

char *ds4_web_cdp_page_ws_url(const ds4_web_cdp_endpoint *ep,
                              const char *target_id) {
    if (!target_id) target_id = "";
    int n = snprintf(NULL, 0, "ws://%s:%d/devtools/page/%s",
                     ds4_web_cdp_endpoint_host(ep),
                     ds4_web_cdp_endpoint_port(ep),
                     target_id);
    if (n < 0) return remote_cdp_xstrdup("");
    char *out = malloc((size_t)n + 1);
    if (!out) {
        perror("ds4_web_remote_cdp: malloc");
        exit(1);
    }
    snprintf(out, (size_t)n + 1, "ws://%s:%d/devtools/page/%s",
             ds4_web_cdp_endpoint_host(ep),
             ds4_web_cdp_endpoint_port(ep),
             target_id);
    return out;
}
