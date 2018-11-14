#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "../common/chash.h"

#define NGX_SLOT_NUMBER 1024


typedef struct ngx_http_upstream_nodeid_peer_s   ngx_http_upstream_nodeid_peer_t;


typedef struct {
    ngx_http_upstream_nodeid_peer_t    *peers;
} ngx_http_upstream_nodeid_point_t;


typedef struct {
    ngx_uint_t                          number;
    ngx_http_upstream_nodeid_point_t    point[1];
} ngx_http_upstream_nodeid_points_t;


typedef struct {
    ngx_str_t                           name;
    ngx_uint_t                          nodeid;
} ngx_http_upstream_nodeid_server_t;

typedef struct {
    ngx_http_complex_value_t            key;
    ngx_array_t                        *nodeid_servers;                     /* ngx_http_upstream_nodeid_server_t */
    ngx_http_upstream_nodeid_points_t  *points;
    ngx_http_upstream_nodeid_points_t  *points_servers;
    ngx_pool_t                         *pool;
} ngx_http_upstream_nodeid_srv_conf_t;


typedef struct {
    /* the round robin data must be first */
    ngx_http_upstream_hash_peer_data_t    rrp;
    ngx_http_upstream_nodeid_srv_conf_t  *conf;
    ngx_uint_t                            nodeid;
    ngx_event_get_peer_pt                 get_rr_peer;
} ngx_http_upstream_nodeid_peer_data_t;


struct ngx_http_upstream_nodeid_peer_s {
    ngx_uint_t                            nodeid;
    ngx_str_t                             server;
    ngx_http_upstream_nodeid_peer_t      *next;
} ;


static ngx_int_t ngx_http_upstream_init_nodeid(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_init_nodeid_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_nodeid_peer(ngx_peer_connection_t *pc,
    void *data);

static void *ngx_http_upstream_nodeid_create_conf(ngx_conf_t *cf);
static char *ngx_http_upstream_nodeid(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_upstream_nodeid_server(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_http_upstream_nodeid_commands[] = {

    { ngx_string("nodeid"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE12,
      ngx_http_upstream_nodeid,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("nodeid_server"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE2,
      ngx_http_upstream_nodeid_server,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_upstream_nodeid_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_http_upstream_nodeid_create_conf,    /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};


ngx_module_t  ngx_http_upstream_nodeid_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_nodeid_module_ctx,    /* module context */
    ngx_http_upstream_nodeid_commands,       /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_upstream_init_nodeid(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                            i, slot;
    size_t                                size;
    ngx_http_upstream_nodeid_points_t    *points, *points_servers;
    ngx_http_upstream_nodeid_srv_conf_t  *hcf;
    ngx_http_upstream_nodeid_server_t    *server;
    ngx_http_upstream_nodeid_peer_t      *nodeid_peer;
    //if (ngx_http_upstream_init_round_robin(cf, us) != NGX_OK) {
    if (ngx_http_upstream_init_chash(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    us->peer.init = ngx_http_upstream_init_nodeid_peer;

    hcf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_nodeid_module);

    size = sizeof(ngx_http_upstream_nodeid_points_t)
           + sizeof(ngx_http_upstream_nodeid_point_t) * (NGX_SLOT_NUMBER - 1);

    points = ngx_pcalloc(cf->pool, size);
    if (points == NULL) {
        return NGX_ERROR;
    }
    points_servers = ngx_pcalloc(cf->pool, size);
    if (points_servers == NULL) {
        return NGX_ERROR;
    }

    points->number = NGX_SLOT_NUMBER;
    points_servers->number = NGX_SLOT_NUMBER;

    if (hcf->nodeid_servers) {
        server = hcf->nodeid_servers->elts;
        for (i = 0; i < hcf->nodeid_servers->nelts; i++) {

            slot = server[i].nodeid % points->number;
            nodeid_peer = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_nodeid_peer_t));
            if (nodeid_peer == NULL) {
                return NGX_ERROR;
            }
            nodeid_peer->nodeid = server[i].nodeid;
            nodeid_peer->server = server[i].name;

            nodeid_peer->next = points->point[slot].peers;
            points->point[slot].peers = nodeid_peer;

            slot = ngx_crc32_long(server[i].name.data, server[i].name.len) % points_servers->number;
            nodeid_peer = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_nodeid_peer_t));
            if (nodeid_peer == NULL) {
                return NGX_ERROR;
            }
            nodeid_peer->nodeid = server[i].nodeid;
            nodeid_peer->server = server[i].name;

            nodeid_peer->next = points_servers->point[slot].peers;
            points_servers->point[slot].peers = nodeid_peer;
            
        }
    }

    hcf->points = points;
    hcf->points_servers = points_servers;
    hcf->pool = cf->pool;

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_init_nodeid_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_nodeid_srv_conf_t   *hcf;
    ngx_http_upstream_nodeid_peer_data_t  *hp;
    ngx_str_t nodeid_str;

    hp = ngx_palloc(r->pool, sizeof(ngx_http_upstream_nodeid_peer_data_t));
    if (hp == NULL) {
        return NGX_ERROR;
    }

    r->upstream->peer.data = &hp->rrp;

    //if (ngx_http_upstream_init_round_robin_peer(r, us) != NGX_OK) {
    if (ngx_http_upstream_init_chash_peer(r, us) != NGX_OK) {
        return NGX_ERROR;
    }

    r->upstream->peer.get = ngx_http_upstream_get_nodeid_peer;

    hcf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_nodeid_module);

    if (ngx_http_complex_value(r, &hcf->key, &nodeid_str) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "upstream nodeid key:\"%V\"", &nodeid_str);

    hp->nodeid = ngx_atoi(nodeid_str.data, nodeid_str.len);
    hp->conf = hcf;
    //hp->get_rr_peer = ngx_http_upstream_get_round_robin_peer;
    hp->get_rr_peer = ngx_http_upstream_get_chash_peer;

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_get_nodeid_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_rr_peer_data_t       *hp = data;
    ngx_http_upstream_nodeid_peer_data_t   *hp_nodeid = data;

    time_t                                  now;
    ngx_http_upstream_rr_peer_t            *peer;
    ngx_http_upstream_nodeid_peer_t        *peer_nodeid;
    ngx_http_upstream_nodeid_points_t      *points;
    ngx_str_t                              *server;
    ngx_uint_t                              i, n;
    intptr_t                                m;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "get nodeid peer, try: %ui", pc->tries);

    now = ngx_time();

    pc->cached = 0;
    pc->connection = NULL;

    points = hp_nodeid->conf->points;
    peer_nodeid = points->point[hp_nodeid->nodeid % points->number].peers;
    for ( ; peer_nodeid; peer_nodeid = peer_nodeid->next) {
        if (peer_nodeid->nodeid == hp_nodeid->nodeid) {
            break;
        }
    }
    if (peer_nodeid == NULL) {
        return hp_nodeid->get_rr_peer(pc, hp);
    }
    server = &peer_nodeid->server;

    ngx_http_upstream_rr_peers_wlock(hp->peers);
    for ( ;; ) {


        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "nodeid peer:%uD, server:\"%V\"",
                       hp_nodeid->nodeid, server);

        for (peer = hp->peers->peer, i = 0;
             peer;
             peer = peer->next, i++)
        {
            n = i / (8 * sizeof(uintptr_t));
            m = (uintptr_t) 1 << i % (8 * sizeof(uintptr_t));

            if (hp->tried[n] & m) {
                continue;
            }

            if (peer->down) {
                continue;
            }

            if (peer->max_fails
                && peer->fails >= peer->max_fails
                && now - peer->checked <= peer->fail_timeout)
            {
                continue;
            }

            if (peer->max_conns && peer->conns >= peer->max_conns) {
                continue;
            }

            if (peer->server.len == server->len 
                    && ngx_strncmp(peer->server.data, server->data, server->len) == 0 ) {

                ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                        "get nodeid peer, value:%i, peer:%p", hp_nodeid->nodeid, peer);
                goto found;
            }
        }

        ngx_http_upstream_rr_peers_unlock(hp->peers);
        return hp_nodeid->get_rr_peer(pc, hp);
    }

found:
    hp->current = peer;

    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    peer->conns++;

    if (now - peer->checked > peer->fail_timeout) {
        peer->checked = now;
    }

    ngx_http_upstream_rr_peers_unlock(hp->peers);
    hp->tried[n] |= m;

    return NGX_OK;
}



static void *
ngx_http_upstream_nodeid_create_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_nodeid_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_nodeid_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    return conf;
}


static char *
ngx_http_upstream_nodeid(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_nodeid_srv_conf_t  *hcf = conf;

    ngx_str_t                         *value;
    ngx_http_upstream_srv_conf_t      *uscf;
    ngx_http_compile_complex_value_t   ccv;

    value = cf->args->elts;

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[1];
    ccv.complex_value = &hcf->key;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    if (uscf->peer.init_upstream) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                           "load balancing method redefined");
    }

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                  |NGX_HTTP_UPSTREAM_WEIGHT
                  |NGX_HTTP_UPSTREAM_MAX_CONNS
                  |NGX_HTTP_UPSTREAM_MAX_FAILS
                  |NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
                  |NGX_HTTP_UPSTREAM_DOWN;

    uscf->peer.init_upstream = ngx_http_upstream_init_nodeid;

    if (cf->args->nelts > 2) {
        return ngx_http_upstream_init_hash_key(cf, &value[2]);
    }
    else {
        return ngx_http_upstream_init_hash_key(cf, &value[1]);
    }
}

static char *
ngx_http_upstream_nodeid_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_nodeid_srv_conf_t  *uscf = conf;
    ngx_http_upstream_nodeid_server_t    *us;

    ngx_str_t                            *value;
    ngx_uint_t                            i;
    ngx_int_t                             nodeid;

    value = cf->args->elts;

    if (uscf->nodeid_servers == NULL) {
        uscf->nodeid_servers = ngx_array_create(cf->pool, 1,
                                         sizeof(ngx_http_upstream_nodeid_server_t));
        if (uscf->nodeid_servers == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    us = ngx_array_push(uscf->nodeid_servers);
    if (us == NULL) {
        return NGX_CONF_ERROR;
    }
    ngx_memzero(us, sizeof(ngx_http_upstream_nodeid_server_t));

    nodeid = 0;

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "nodeid=", 7) == 0) {

            nodeid = ngx_atoi(&value[i].data[7], value[i].len - 7);

            if (nodeid == NGX_ERROR || nodeid == 0) {
                goto invalid;
            }

            continue;
        }

    }
    us->name = value[1];
    us->nodeid = nodeid;

    return NGX_CONF_OK;

invalid:

    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                       "invalid parameter \"%V\"", &value[i]);

    return NGX_CONF_ERROR;

}


ngx_int_t
ngx_http_upstream_nodeid_dump_server(ngx_http_upstream_srv_conf_t *us, ngx_buf_t *b)
{
    ngx_http_upstream_nodeid_points_t       *points;
    ngx_http_upstream_nodeid_point_t        *point;
    ngx_http_upstream_nodeid_peer_t         *peer;
    ngx_http_upstream_nodeid_srv_conf_t     *hcf;
    ngx_uint_t                               i;

    hcf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_nodeid_module);
    points = hcf->points;
    if (points == NULL) {
        return NGX_OK;
    }
    point = points->point;

    for ( i = 0; i < points->number; i++ ) {
        for ( peer = point[i].peers; peer; peer = peer->next ) {
            b->last = ngx_snprintf(b->last, b->end - b->last, 
                    "        nodeid_server %V", &peer->server);
            b->last = ngx_snprintf(b->last, b->end - b->last, 
                    " nodeid=%d", peer->nodeid);

            b->last = ngx_snprintf(b->last, b->end - b->last, ";\n");
        }
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_upstream_nodeid_add_server(ngx_http_upstream_srv_conf_t *us, ngx_uint_t nodeid, u_char *server)
{
    ngx_pool_t                              *pool;
    ngx_http_upstream_nodeid_points_t       *points, *points_servers;
    ngx_http_upstream_nodeid_point_t        *point;
    ngx_http_upstream_nodeid_peer_t         *peer;
    ngx_http_upstream_nodeid_srv_conf_t     *hcf;
    u_char                                  *buff;
    ngx_uint_t                               slot;
    size_t                                   size;

    hcf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_nodeid_module);
    points = hcf->points;
    points_servers = hcf->points_servers;
    if (points == NULL || points_servers == NULL) {
        return NGX_OK;
    }
    pool = hcf->pool;
    point = points->point;
    slot = nodeid % points->number;
    peer = ngx_pcalloc(pool, sizeof(ngx_http_upstream_nodeid_peer_t));
    if (peer == NULL) {
        return NGX_ERROR;
    }
    peer->nodeid = nodeid;
    size = ngx_strlen(server);
    buff = ngx_palloc(pool, size);
    if (buff == NULL) {
        ngx_pfree(pool, peer);
        return NGX_ERROR;
    }
    ngx_memcpy(buff, server, size);
    peer->server.data = buff;
    peer->server.len = size;
    peer->next = point[slot].peers;
    point[slot].peers = peer;

    point = points_servers->point;
    slot = ngx_crc32_long(server, size) % points->number;
    peer = ngx_pcalloc(pool, sizeof(ngx_http_upstream_nodeid_peer_t));
    if (peer == NULL) {
        return NGX_ERROR;
    }
    peer->nodeid = nodeid;
    peer->server.data = buff;
    peer->server.len = size;
    peer->next = point[slot].peers;
    point[slot].peers = peer;


    return NGX_OK;
}


ngx_int_t
ngx_http_upstream_nodeid_del_server(ngx_http_upstream_srv_conf_t *us, u_char *server)
{
    ngx_pool_t                              *pool;
    ngx_http_upstream_nodeid_points_t       *points, *points_servers;
    ngx_http_upstream_nodeid_point_t        *point;
    ngx_http_upstream_nodeid_peer_t         *peer, *pre_peer, *del_peer;
    ngx_http_upstream_nodeid_srv_conf_t     *hcf;
    ngx_uint_t                               slot, nodeid;

    hcf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_nodeid_module);
    points = hcf->points;
    points_servers = hcf->points_servers;
    if (points == NULL || points_servers == NULL) {
        return NGX_OK;
    }

    pool = hcf->pool;
    nodeid = 0;
    point = points_servers->point;
    slot = ngx_crc32_long(server, ngx_strlen(server)) % points_servers->number;
    pre_peer = NULL; 
    for (peer = point[slot].peers; peer; peer = peer->next) {
        if (peer->server.len == ngx_strlen(server)
                && ngx_strncmp(peer->server.data, server, peer->server.len) == 0)
        {
            nodeid = peer->nodeid;

            if (pre_peer == NULL) {
                point[slot].peers = peer->next;
            }
            else {
                pre_peer->next = peer->next;
            }
            ngx_pfree(pool, peer);
            break;
        }
    }
    point = points->point;
    slot = nodeid % points->number;
    pre_peer = NULL; 
    for (peer = point[slot].peers; peer; ) {
        if (peer->nodeid == nodeid) {
            del_peer = peer;
            peer = peer->next;
            if (pre_peer == NULL) {
                point[slot].peers = peer;
            }
            else {
                pre_peer->next = peer;
            }
            ngx_pfree(pool, del_peer->server.data);
            ngx_pfree(pool, del_peer);
        }
        else {
            peer = peer->next;
            pre_peer = peer;
        }
    }

    return NGX_OK;
}

