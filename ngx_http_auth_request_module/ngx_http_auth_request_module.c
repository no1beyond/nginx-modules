
/*
 * Copyright (C) Maxim Dounin
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


typedef struct {
    ngx_str_t                 uri;
    ngx_array_t              *vars;
    ngx_uint_t                read_body;
    ngx_uint_t                return_body;
} ngx_http_auth_request_conf_t;


typedef struct {
    ngx_uint_t                done;
    ngx_uint_t                status;
    ngx_http_request_t       *subrequest;
} ngx_http_auth_request_ctx_t;


typedef struct {
    ngx_int_t                 index;
    ngx_http_complex_value_t  value;
    ngx_http_set_variable_pt  set_handler;
} ngx_http_auth_request_variable_t;


static ngx_int_t ngx_http_auth_request_handler(ngx_http_request_t *r);
//static void ngx_http_auth_request_rd_check_broken_connection(ngx_http_request_t *r);
//static void ngx_http_auth_request_wr_check_broken_connection(ngx_http_request_t *r);
//static void ngx_http_auth_request_check_broken_connection(ngx_http_request_t *r,
//    ngx_event_t *ev);
static void ngx_http_auth_request_body_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_auth_request_done(ngx_http_request_t *r,
    void *data, ngx_int_t rc);
static ngx_int_t ngx_http_auth_request_set_variables(ngx_http_request_t *r,
    ngx_http_auth_request_conf_t *arcf, ngx_http_auth_request_ctx_t *ctx);
static ngx_int_t ngx_http_auth_request_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
static void *ngx_http_auth_request_create_conf(ngx_conf_t *cf);
static char *ngx_http_auth_request_merge_conf(ngx_conf_t *cf,
    void *parent, void *child);
static ngx_int_t ngx_http_auth_request_init(ngx_conf_t *cf);
static char *ngx_http_auth_request(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_auth_request_set(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_http_auth_request_commands[] = {

    { ngx_string("auth_request"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE123,
      ngx_http_auth_request,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("auth_request_set"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE2,
      ngx_http_auth_request_set,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_auth_request_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_auth_request_init,            /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_auth_request_create_conf,     /* create location configuration */
    ngx_http_auth_request_merge_conf       /* merge location configuration */
};


ngx_module_t  ngx_http_auth_request_module = {
    NGX_MODULE_V1,
    &ngx_http_auth_request_module_ctx,     /* module context */
    ngx_http_auth_request_commands,        /* module directives */
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


static void 
ngx_http_auth_request_body_handler(ngx_http_request_t *r)
{
    r->main->count--;
    ngx_http_auth_request_handler(r);
    //ngx_connection_t     *c;

    //c = r->connection;

    //if (c->read->timer_set) {
    //    ngx_del_timer(c->read);
    //}

    //if (ngx_event_flags & NGX_USE_CLEAR_EVENT) {

    //    if (!c->write->active) {
    //        if (ngx_add_event(c->write, NGX_WRITE_EVENT, NGX_CLEAR_EVENT)
    //            == NGX_ERROR)
    //        {
    //            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    //            return;
    //        }
    //    }
    //}


    //r->read_event_handler = ngx_http_auth_request_rd_check_broken_connection;
    //r->write_event_handler = ngx_http_auth_request_wr_check_broken_connection;

}


//static void
//ngx_http_auth_request_rd_check_broken_connection(ngx_http_request_t *r)
//{
//    ngx_http_auth_request_check_broken_connection(r, r->connection->read);
//}
//
//
//static void
//ngx_http_auth_request_wr_check_broken_connection(ngx_http_request_t *r)
//{
//    ngx_http_auth_request_check_broken_connection(r, r->connection->write);
//}
//
//
//static void
//ngx_http_auth_request_check_broken_connection(ngx_http_request_t *r,
//    ngx_event_t *ev)
//{
//    int                  n;
//    char                 buf[1];
//    ngx_err_t            err;
//    ngx_int_t            event;
//    ngx_connection_t     *c;
//
//    c = r->connection;
//
//    if (c->error) {
//        if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && ev->active) {
//
//            event = ev->write ? NGX_WRITE_EVENT : NGX_READ_EVENT;
//
//            if (ngx_del_event(ev, event, 0) != NGX_OK) {
//                ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
//                return;
//            }
//        }
//
//        ngx_http_finalize_request(r, NGX_HTTP_CLIENT_CLOSED_REQUEST);
//
//        return;
//    }
//
//#if (NGX_HTTP_V2)
//    if (r->stream) {
//        return;
//    }
//#endif
//
//#if (NGX_HAVE_KQUEUE)
//
//    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT) {
//
//        if (!ev->pending_eof) {
//            return;
//        }
//
//        ev->eof = 1;
//        c->error = 1;
//
//        if (ev->kq_errno) {
//            ev->error = 1;
//        }
//        ngx_log_error(NGX_LOG_INFO, ev->log, ev->kq_errno,
//                      "kevent() reported that client prematurely closed "
//                      "connection");
//
//        ngx_http_finalize_request(r, NGX_HTTP_CLIENT_CLOSED_REQUEST);
//
//        return;
//    }
//
//#endif
//
//#if (NGX_HAVE_EPOLLRDHUP)
//
//    if ((ngx_event_flags & NGX_USE_EPOLL_EVENT) && ngx_use_epoll_rdhup) {
//        socklen_t  len;
//
//        if (!ev->pending_eof) {
//            return;
//        }
//
//        ev->eof = 1;
//        c->error = 1;
//
//        err = 0;
//        len = sizeof(ngx_err_t);
//
//        /*
//         * BSDs and Linux return 0 and set a pending error in err
//         * Solaris returns -1 and sets errno
//         */
//
//        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
//            == -1)
//        {
//            err = ngx_socket_errno;
//        }
//
//        if (err) {
//            ev->error = 1;
//        }
//
//        ngx_log_error(NGX_LOG_INFO, ev->log, err,
//                      "epoll_wait() reported that client prematurely closed "
//                      "connection");
//
//        ngx_http_finalize_request(r, NGX_HTTP_CLIENT_CLOSED_REQUEST);
//
//        return;
//    }
//
//#endif
//
//    n = recv(c->fd, buf, 1, MSG_PEEK);
//
//    err = ngx_socket_errno;
//
//    if (ev->write && (n >= 0 || err == NGX_EAGAIN)) {
//        return;
//    }
//
//    if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && ev->active) {
//
//        event = ev->write ? NGX_WRITE_EVENT : NGX_READ_EVENT;
//
//        if (ngx_del_event(ev, event, 0) != NGX_OK) {
//            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
//            return;
//        }
//    }
//
//    if (n > 0) {
//        return;
//    }
//
//    if (n == -1) {
//        if (err == NGX_EAGAIN) {
//            return;
//        }
//
//        ev->error = 1;
//
//    } else { /* n == 0 */
//        err = 0;
//    }
//
//    ev->eof = 1;
//    c->error = 1;
//
//    ngx_log_error(NGX_LOG_INFO, ev->log, err,
//                  "client prematurely closed connection");
//
//    ngx_http_finalize_request(r, NGX_HTTP_CLIENT_CLOSED_REQUEST);
//}


static ngx_int_t
ngx_http_auth_request_handler(ngx_http_request_t *r)
{
    ngx_table_elt_t               *h, *ho;
    ngx_http_request_t            *sr;
    ngx_http_post_subrequest_t    *ps;
    ngx_http_auth_request_ctx_t   *ctx;
    ngx_http_auth_request_conf_t  *arcf;

    arcf = ngx_http_get_module_loc_conf(r, ngx_http_auth_request_module);

    if (arcf->uri.len == 0) {
        return NGX_DECLINED;
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "auth request handler");

    ctx = ngx_http_get_module_ctx(r, ngx_http_auth_request_module);

    if (ctx != NULL) {
        if (!ctx->done) {
            return NGX_AGAIN;
        }

        /*
         * as soon as we are done - explicitly set variables to make
         * sure they will be available after internal redirects
         */

        if (ngx_http_auth_request_set_variables(r, arcf, ctx) != NGX_OK) {
            return NGX_ERROR;
        }

        /* return appropriate status */

        if (ctx->status == NGX_HTTP_FORBIDDEN) {
            return ctx->status;
        }

        if (ctx->status == NGX_HTTP_UNAUTHORIZED) {
            sr = ctx->subrequest;

            h = sr->headers_out.www_authenticate;

            if (!h && sr->upstream) {
                h = sr->upstream->headers_in.www_authenticate;
            }

            if (h) {
                ho = ngx_list_push(&r->headers_out.headers);
                if (ho == NULL) {
                    return NGX_ERROR;
                }

                *ho = *h;

                r->headers_out.www_authenticate = ho;
            }

            return ctx->status;
        }

        if (ctx->status >= NGX_HTTP_OK
            && ctx->status < NGX_HTTP_SPECIAL_RESPONSE)
        {
            return NGX_OK;
        }

        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "auth request unexpected status: %ui", ctx->status);

        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (arcf->read_body && r->headers_in.content_length_n > 0) {
        if (!r->reading_body && !r->request_body) {
            if (ngx_http_read_client_request_body(r , ngx_http_auth_request_body_handler) != NGX_OK) {
                return NGX_ERROR;
            }
            return NGX_AGAIN;
        }
    }

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_auth_request_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ps = ngx_palloc(r->pool, sizeof(ngx_http_post_subrequest_t));
    if (ps == NULL) {
        return NGX_ERROR;
    }

    ps->handler = ngx_http_auth_request_done;
    ps->data = ctx;

    if (ngx_http_subrequest(r, &arcf->uri, NULL, &sr, ps,
                            NGX_HTTP_SUBREQUEST_WAITED)
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    /*
     * allocate fake request body to avoid attempts to read it and to make
     * sure real body file (if already read) won't be closed by upstream
     */

    if (!arcf->read_body || r ->headers_in.content_length_n <= 0) {
        sr->request_body = ngx_pcalloc(r->pool, sizeof(ngx_http_request_body_t));
        if (sr->request_body == NULL) {
            return NGX_ERROR;
        }
    }

    //sr->header_only = 1;
    sr->header_only = !arcf->return_body;

    ctx->subrequest = sr;

    ngx_http_set_ctx(r, ctx, ngx_http_auth_request_module);

    return NGX_AGAIN;
}


static ngx_int_t
ngx_http_auth_request_done(ngx_http_request_t *r, void *data, ngx_int_t rc)
{
    ngx_http_auth_request_ctx_t   *ctx = data;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "auth request done s:%ui", r->headers_out.status);

    ctx->done = 1;
    ctx->status = r->headers_out.status;

    return rc;
}


static ngx_int_t
ngx_http_auth_request_set_variables(ngx_http_request_t *r,
    ngx_http_auth_request_conf_t *arcf, ngx_http_auth_request_ctx_t *ctx)
{
    ngx_str_t                          val;
    ngx_http_variable_t               *v;
    ngx_http_variable_value_t         *vv;
    ngx_http_auth_request_variable_t  *av, *last;
    ngx_http_core_main_conf_t         *cmcf;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "auth request set variables");

    if (arcf->vars == NULL) {
        return NGX_OK;
    }

    cmcf = ngx_http_get_module_main_conf(r, ngx_http_core_module);
    v = cmcf->variables.elts;

    av = arcf->vars->elts;
    last = av + arcf->vars->nelts;

    while (av < last) {
        /*
         * explicitly set new value to make sure it will be available after
         * internal redirects
         */

        vv = &r->variables[av->index];

        if (ngx_http_complex_value(ctx->subrequest, &av->value, &val)
            != NGX_OK)
        {
            return NGX_ERROR;
        }

        vv->valid = 1;
        vv->not_found = 0;
        vv->data = val.data;
        vv->len = val.len;

        if (av->set_handler) {
            /*
             * set_handler only available in cmcf->variables_keys, so we store
             * it explicitly
             */

            av->set_handler(r, vv, v[av->index].data);
        }

        av++;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_auth_request_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "auth request variable");

    v->not_found = 1;

    return NGX_OK;
}


static void *
ngx_http_auth_request_create_conf(ngx_conf_t *cf)
{
    ngx_http_auth_request_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_auth_request_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     conf->uri = { 0, NULL };
     */

    conf->vars = NGX_CONF_UNSET_PTR;

    return conf;
}


static char *
ngx_http_auth_request_merge_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_auth_request_conf_t *prev = parent;
    ngx_http_auth_request_conf_t *conf = child;

    ngx_conf_merge_str_value(conf->uri, prev->uri, "");
    ngx_conf_merge_ptr_value(conf->vars, prev->vars, NULL);

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_auth_request_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_auth_request_handler;

    return NGX_OK;
}


static char *
ngx_http_auth_request(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_auth_request_conf_t *arcf = conf;

    ngx_str_t        *value;
    ngx_uint_t         ix;

    if (arcf->uri.data != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;

    if (ngx_strcmp(value[1].data, "off") == 0) {
        arcf->uri.len = 0;
        arcf->uri.data = (u_char *) "";

        return NGX_CONF_OK;
    }

    arcf->uri = value[1];

    for (ix = 2; ix < cf->args->nelts; ix++) {
        if (ngx_strcmp(value[ix].data, "read_body") == 0) {
            arcf->read_body = 1;
            continue;
        }
        if (ngx_strcmp(value[ix].data, "return_body") == 0) {
            arcf->return_body = 1;
            continue;
        }
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_auth_request_set(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_auth_request_conf_t *arcf = conf;

    ngx_str_t                         *value;
    ngx_http_variable_t               *v;
    ngx_http_auth_request_variable_t  *av;
    ngx_http_compile_complex_value_t   ccv;

    value = cf->args->elts;

    if (value[1].data[0] != '$') {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid variable name \"%V\"", &value[1]);
        return NGX_CONF_ERROR;
    }

    value[1].len--;
    value[1].data++;

    if (arcf->vars == NGX_CONF_UNSET_PTR) {
        arcf->vars = ngx_array_create(cf->pool, 1,
                                      sizeof(ngx_http_auth_request_variable_t));
        if (arcf->vars == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    av = ngx_array_push(arcf->vars);
    if (av == NULL) {
        return NGX_CONF_ERROR;
    }

    v = ngx_http_add_variable(cf, &value[1], NGX_HTTP_VAR_CHANGEABLE);
    if (v == NULL) {
        return NGX_CONF_ERROR;
    }

    av->index = ngx_http_get_variable_index(cf, &value[1]);
    if (av->index == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }

    if (v->get_handler == NULL) {
        v->get_handler = ngx_http_auth_request_variable;
        v->data = (uintptr_t) av;
    }

    av->set_handler = v->set_handler;

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[2];
    ccv.complex_value = &av->value;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}
