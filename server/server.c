#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define PORT 5718

typedef struct server server_t;

typedef struct client {
    server_t *server;
    int id;
    int fd;
    int connected;
    int started;
    struct sockaddr_in addr;
    pthread_t thread;
    pthread_mutex_t send_lock;
    struct client *next;
} client_t;

struct server {
    int fd;
    pthread_mutex_t lock;
    client_t *clients;
    int next_id;
};

static volatile sig_atomic_t stop;

static void on_signal(int sig)
{
    (void)sig;
    stop = 1;
}

static client_t *client_add(server_t *s, int fd,
                            const struct sockaddr_in *addr)
{
    client_t *c = calloc(1, sizeof(*c));

    if (c == NULL) return NULL;
    c->server = s;
    c->fd = fd;
    c->addr = *addr;
    c->connected = 1;
    if (pthread_mutex_init(&c->send_lock, NULL) != 0) {
        free(c);
        return NULL;
    }

    pthread_mutex_lock(&s->lock);
    c->id = s->next_id++;
    c->next = s->clients;
    s->clients = c;
    pthread_mutex_unlock(&s->lock);
    return c;
}

static client_t *client_find(server_t *s, int id)
{
    client_t *c;

    pthread_mutex_lock(&s->lock);
    for (c = s->clients; c != NULL; c = c->next) {
        if (c->id == id && c->connected) {
            break;
        }
    }
    pthread_mutex_unlock(&s->lock);
    return c;
}

static int send_packet(client_t *c, uint8_t kind, uint8_t type,
                       uint8_t status, uint32_t seq,
                       const void *body, uint32_t len)
{
    int rc = -1;

    if (c == NULL) {
        return -1;
    }
    pthread_mutex_lock(&c->send_lock);
    if (c->connected && c->fd >= 0) {
        rc = proto_send_packet(c->fd, kind, type, status, seq, body, len);
    }
    pthread_mutex_unlock(&c->send_lock);
    return rc;
}

static void reply(client_t *c, const proto_message_t *req,
                  uint8_t status, const void *body, uint32_t len)
{
    send_packet(c, PROTO_RESPONSE, req->type, status, req->seq, body, len);
}

static void handle_request(client_t *c, const proto_message_t *req)
{
    uint8_t body[PROTO_MAX_BODY];

    printf("客户端 #%d: %s\n", c->id,
           req->type == PROTO_REQ_TIME ? "获取时间" :
           req->type == PROTO_REQ_NAME ? "获取名字" :
           req->type == PROTO_REQ_LIST ? "获取列表" :
           req->type == PROTO_REQ_MESSAGE ? "发送消息" : "未知请求");
    fflush(stdout);

    if (req->kind != PROTO_REQUEST) {
        reply(c, req, PROTO_ERR_BAD_REQUEST, NULL, 0);
        return;
    }

    switch (req->type) {
    case PROTO_REQ_TIME: {
        char text[64];
        time_t now = time(NULL);
        struct tm tmv;

        if (now == (time_t)-1 || localtime_r(&now, &tmv) == NULL ||
            strftime(text, sizeof(text), "%Y-%m-%d %H:%M:%S", &tmv) == 0) {
            reply(c, req, PROTO_ERR_INTERNAL, NULL, 0);
        } else {
            reply(c, req, PROTO_OK, text, (uint32_t)strlen(text));
        }
        break;
    }
    case PROTO_REQ_NAME: {
        char name[256];

        if (gethostname(name, sizeof(name)) != 0) {
            reply(c, req, PROTO_ERR_INTERNAL, NULL, 0);
        } else {
            name[sizeof(name) - 1] = '\0';
            reply(c, req, PROTO_OK, name, (uint32_t)strlen(name));
        }
        break;
    }
    case PROTO_REQ_LIST: {
        uint32_t count = 0;
        uint32_t net;
        uint8_t *p = body + 4;
        client_t *c2;

        pthread_mutex_lock(&c->server->lock);
        for (c2 = c->server->clients; c2 != NULL; c2 = c2->next) {
            if (c2->connected) {
                count++;
            }
        }
        if (4 + (size_t)count * sizeof(proto_client_info_t) > PROTO_MAX_BODY) {
            pthread_mutex_unlock(&c->server->lock);
            reply(c, req, PROTO_ERR_INTERNAL, NULL, 0);
            break;
        }
        net = htonl(count);
        memcpy(body, &net, 4);
        for (c2 = c->server->clients; c2 != NULL; c2 = c2->next) {
            proto_client_info_t info;
            if (!c2->connected) {
                continue;
            }
            info.id = htonl((uint32_t)c2->id);
            info.ip = c2->addr.sin_addr.s_addr;
            info.port = c2->addr.sin_port;
            info.state = htons(1);
            memcpy(p, &info, sizeof(info));
            p += sizeof(info);
        }
        pthread_mutex_unlock(&c->server->lock);
        reply(c, req, PROTO_OK, body, (uint32_t)(p - body));
        break;
    }
    case PROTO_REQ_MESSAGE: {
        uint32_t net;
        uint32_t target_id;
        size_t text_len;
        client_t *target;
        proto_msg_indication_t ind;

        if (req->body_len < 4) {
            reply(c, req, PROTO_ERR_BAD_REQUEST, NULL, 0);
            break;
        }
        memcpy(&net, req->body, 4);
        target_id = ntohl(net);
        text_len = (size_t)req->body_len - 4;
        if (text_len > PROTO_MAX_TEXT ||
            sizeof(ind) + text_len > PROTO_MAX_BODY ||
            target_id == (uint32_t)c->id) {
            reply(c, req, PROTO_ERR_BAD_REQUEST, NULL, 0);
            break;
        }
        target = client_find(c->server, (int)target_id);
        if (target == NULL) {
            printf("客户端 #%d: 目标 #%u 不存在\n", c->id, target_id);
            fflush(stdout);
            reply(c, req, PROTO_ERR_NOT_FOUND, "client not found",
                  (uint32_t)(sizeof("client not found") - 1));
            break;
        }

        ind.sender_id = htonl((uint32_t)c->id);
        ind.sender_ip = c->addr.sin_addr.s_addr;
        ind.sender_port = c->addr.sin_port;
        ind.reserved = 0;
        memcpy(body, &ind, sizeof(ind));
        memcpy(body + sizeof(ind), req->body + 4, text_len);
        if (send_packet(target, PROTO_INDICATION, PROTO_IND_MESSAGE, 0,
                        req->seq, body,
                        (uint32_t)(sizeof(ind) + text_len)) < 0) {
            printf("客户端 #%d: 转发给 #%u 失败\n", c->id, target_id);
            fflush(stdout);
            reply(c, req, PROTO_ERR_SEND_FAILED, NULL, 0);
        } else {
            printf("客户端 #%d: 已转发给 #%u\n", c->id, target_id);
            fflush(stdout);
            reply(c, req, PROTO_OK, NULL, 0);
        }
        break;
    }
    default:
        reply(c, req, PROTO_ERR_BAD_REQUEST, NULL, 0);
        break;
    }
}

static void *client_thread(void *arg)
{
    client_t *c = arg;
    proto_message_t msg;

    while (!stop && c->connected && proto_recv_packet(c->fd, &msg) == 0) {
        handle_request(c, &msg);
    }

    pthread_mutex_lock(&c->send_lock);
    c->connected = 0;
    if (c->fd >= 0) {
        close(c->fd);
        c->fd = -1;
    }
    pthread_mutex_unlock(&c->send_lock);
    return NULL;
}

static int server_run(server_t *s)
{
    struct sockaddr_in addr;
    int fd;
    int yes = 1;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(fd, SOMAXCONN) < 0) {
        perror("bind/listen");
        close(fd);
        return -1;
    }

    s->fd = fd;
    printf("服务器正在监听端口 %d\n", PORT);
    fflush(stdout);

    while (!stop) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        int client_fd = accept(fd, (struct sockaddr *)&peer, &peer_len);
        client_t *c;
        char ip[INET_ADDRSTRLEN];

        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        c = client_add(s, client_fd, &peer);
        if (c == NULL) {
            close(client_fd);
            continue;
        }
        if (pthread_create(&c->thread, NULL, client_thread, c) != 0) {
            perror("pthread_create");
            c->connected = 0;
            close(c->fd);
            c->fd = -1;
            continue;
        }
        c->started = 1;
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        printf("客户端 #%d 已连接: %s:%u\n",
               c->id, ip, ntohs(peer.sin_port));
    }
    return 0;
}

static void cleanup(server_t *s)
{
    client_t *c;
    client_t *next;

    if (s->fd >= 0) {
        close(s->fd);
    }
    for (c = s->clients; c != NULL; c = c->next) {
        pthread_mutex_lock(&c->send_lock);
        c->connected = 0;
        if (c->fd >= 0) {
            shutdown(c->fd, SHUT_RDWR);
        }
        pthread_mutex_unlock(&c->send_lock);
    }
    for (c = s->clients; c != NULL; c = c->next) {
        if (c->started) {
            pthread_join(c->thread, NULL);
        }
    }
    c = s->clients;
    while (c != NULL) {
        next = c->next;
        if (c->fd >= 0) {
            close(c->fd);
        }
        pthread_mutex_destroy(&c->send_lock);
        free(c);
        c = next;
    }
    pthread_mutex_destroy(&s->lock);
}

int main(void)
{
    server_t s = {0};
    struct sigaction sa = {0};

    s.fd = -1;
    s.next_id = 1;
    pthread_mutex_init(&s.lock, NULL);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    if (server_run(&s) < 0) {
        cleanup(&s);
        return 1;
    }
    cleanup(&s);
    return 0;
}
