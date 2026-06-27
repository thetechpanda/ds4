#ifndef DS4_WEB_REMOTE_CDP_H
#define DS4_WEB_REMOTE_CDP_H

#include <stdbool.h>

typedef struct {
    char host[256];
    int port;
    bool remote;
} ds4_web_cdp_endpoint;

void ds4_web_cdp_endpoint_init(ds4_web_cdp_endpoint *ep,
                               const char *host,
                               int cdp_port,
                               int default_port);
bool ds4_web_cdp_endpoint_remote(const ds4_web_cdp_endpoint *ep);
const char *ds4_web_cdp_endpoint_host(const ds4_web_cdp_endpoint *ep);
int ds4_web_cdp_endpoint_port(const ds4_web_cdp_endpoint *ep);
char *ds4_web_cdp_rewrite_ws_url(const ds4_web_cdp_endpoint *ep,
                                 const char *ws_url);
char *ds4_web_cdp_page_ws_url(const ds4_web_cdp_endpoint *ep,
                              const char *target_id);

#endif
