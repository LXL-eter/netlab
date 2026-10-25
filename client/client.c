#include "protocol.h"
#include "net.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef TIME_REQUEST_COUNT
#define TIME_REQUEST_COUNT 1
#endif

#define CLIENT_QUEUE_SIZE 128
#define MAX_PEERS 64

typedef struct {
    proto_message_t items[CLIENT_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
} msg_queue_t;

typedef struct {
    uint32_t id;
    char ip[INET_ADDRSTRLEN];
    uint16_t port;
} peer_t;

typedef struct {
    int sockfd;
    volatile int connected;
    int running;
    volatile int recv_stopped;
    pthread_t recv_thread;
    uint32_t next_seq;
    int has_list;
    int peer_count;
    peer_t peers[MAX_PEERS];
    int time_sent;
    int time_recv;
    msg_queue_t queue;
    int wake_pipe[2];
} client_t;

static void print_menu(void);
static int read_line(char *buf, size_t size);

static void client_init(client_t *c);
static void client_cleanup(client_t *c);
static void client_run(client_t *c);
static void client_handle_choice(client_t *c);

static void client_connect(client_t *c);
static void client_disconnect(client_t *c);
static void client_notify(client_t *c);
static void client_send_request(client_t *c, uint8_t type,
                                const void *body, uint32_t body_len);

static void *client_receiver(void *arg);
static void client_process_queue(client_t *c);
static void client_handle_message(client_t *c, const proto_message_t *msg);
static void client_parse_list(client_t *c, const proto_message_t *msg);

static void client_get_time(client_t *c);
static void client_get_name(client_t *c);
static void client_get_list(client_t *c);
static void client_send_message(client_t *c);

static void queue_init(msg_queue_t *q);
static void queue_destroy(msg_queue_t *q);
static int queue_push(msg_queue_t *q, const proto_message_t *msg);
static int queue_pop(msg_queue_t *q, proto_message_t *msg);

static void print_menu(void)
{
    printf("\n===== 客户端菜单 =====\n");
    printf("1. 连接\n");
    printf("2. 断开连接\n");
    printf("3. 获取时间\n");
    printf("4. 获取名字\n");
    printf("5. 活动连接列表\n");
    printf("6. 发消息\n");
    printf("7. 退出\n");
    printf("请选择: ");
    fflush(stdout);
}

static int read_line(char *buf, size_t size)
{
    size_t len;

    if (fgets(buf, (int)size, stdin) == NULL) {
        return -1;
    }

    len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n') {
        buf[len - 1] = '\0';
    }
    return 0;
}

static void client_init(client_t *c)
{
    memset(c, 0, sizeof(*c));
    c->sockfd = -1;
    c->wake_pipe[0] = -1;
    c->wake_pipe[1] = -1;
    c->running = 1;
    queue_init(&c->queue);
    if (pipe(c->wake_pipe) != 0) {
        perror("pipe");
        c->running = 0;
        return;
    }
    fcntl(c->wake_pipe[0], F_SETFL, O_NONBLOCK);
}

static void client_cleanup(client_t *c)
{
    if (c->connected) {
        client_disconnect(c);
    }
    if (c->wake_pipe[0] >= 0) {
        close(c->wake_pipe[0]);
    }
    if (c->wake_pipe[1] >= 0) {
        close(c->wake_pipe[1]);
    }
    queue_destroy(&c->queue);
}

static void client_run(client_t *c)
{
    print_menu();
    while (c->running) {
        fd_set rfds;
        int maxfd;
        int n;

        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        if (c->wake_pipe[0] >= 0) {
            FD_SET(c->wake_pipe[0], &rfds);
        }
        maxfd = c->wake_pipe[0] > STDIN_FILENO ? c->wake_pipe[0]
                                               : STDIN_FILENO;

        n = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }

        if (c->wake_pipe[0] >= 0 && FD_ISSET(c->wake_pipe[0], &rfds)) {
            char drain[64];
            while (read(c->wake_pipe[0], drain, sizeof(drain)) > 0) {
            }
            client_process_queue(c);
            if (c->connected && c->recv_stopped) {
                printf("与服务器的连接已断开。\n");
                client_disconnect(c);
                print_menu();
            }
        }

        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            client_handle_choice(c);
            if (c->running) {
                print_menu();
            }
        }
    }
}

static void client_handle_choice(client_t *c)
{
    char line[64];
    int choice;

    if (read_line(line, sizeof(line)) < 0) {
        c->running = 0;
        return;
    }

    choice = atoi(line);
    if (!c->connected && choice != 1 && choice != 7) {
        printf("未连接时只能选择“连接”或“退出”。\n");
        return;
    }

    switch (choice) {
    case 1:
        client_connect(c);
        break;
    case 2:
        client_disconnect(c);
        break;
    case 3:
        client_get_time(c);
        break;
    case 4:
        client_get_name(c);
        break;
    case 5:
        client_get_list(c);
        break;
    case 6:
        client_send_message(c);
        break;
    case 7:
        if (c->connected) {
            client_disconnect(c);
        }
        c->running = 0;
        break;
    default:
        printf("无效选项。\n");
        break;
    }
}

static void client_connect(client_t *c)
{
    char ip[INET_ADDRSTRLEN];
    char portstr[16];
    struct sockaddr_in addr;
    int fd;
    int port;

    if (c->connected) {
        printf("已连接，请先断开。\n");
        return;
    }

    printf("服务器 IP: ");
    fflush(stdout);
    if (read_line(ip, sizeof(ip)) < 0 || ip[0] == '\0') {
        printf("输入无效。\n");
        return;
    }
    printf("服务器端口: ");
    fflush(stdout);
    if (read_line(portstr, sizeof(portstr)) < 0 || portstr[0] == '\0') {
        printf("输入无效。\n");
        return;
    }
    port = atoi(portstr);
    if (port <= 0 || port > 65535) {
        printf("端口号无效。\n");
        return;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        printf("IP 地址无效。\n");
        close(fd);
        return;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return;
    }

    c->sockfd = fd;
    c->connected = 1;
    c->recv_stopped = 0;
    c->next_seq = 1;
    c->has_list = 0;
    c->peer_count = 0;
    c->time_sent = 0;
    c->time_recv = 0;

    if (pthread_create(&c->recv_thread, NULL, client_receiver, c) != 0) {
        perror("pthread_create");
        close(fd);
        c->sockfd = -1;
        c->connected = 0;
        return;
    }

    printf("已连接到 %s:%d。\n", ip, port);
}

static void client_disconnect(client_t *c)
{
    proto_message_t tmp;

    if (!c->connected) {
        return;
    }

    c->connected = 0;
    if (c->sockfd >= 0) {
        shutdown(c->sockfd, SHUT_RDWR);
    }
    pthread_join(c->recv_thread, NULL);
    if (c->sockfd >= 0) {
        close(c->sockfd);
        c->sockfd = -1;
    }

    while (queue_pop(&c->queue, &tmp) == 0) {
    }

    c->has_list = 0;
    c->peer_count = 0;
    c->time_sent = 0;
    c->time_recv = 0;
    printf("已断开连接。\n");
}

static void client_notify(client_t *c)
{
    char b = 1;
    ssize_t n;

    if (c->wake_pipe[1] < 0) {
        return;
    }
    do {
        n = write(c->wake_pipe[1], &b, 1);
    } while (n < 0 && errno == EINTR);
}

static void client_send_request(client_t *c, uint8_t type,
                                const void *body, uint32_t body_len)
{
    uint32_t seq;

    if (!c->connected) {
        printf("尚未连接。\n");
        return;
    }

    seq = c->next_seq++;
    if (proto_send_packet(c->sockfd, PROTO_REQUEST, type, 0, seq,
                          body, body_len) < 0) {
        perror("send");
        printf("发送失败，连接已断开。\n");
        client_disconnect(c);
    }
}

static void *client_receiver(void *arg)
{
    client_t *c = arg;
    proto_message_t msg;

    while (c->connected) {
        if (proto_recv_packet(c->sockfd, &msg) < 0) {
            break;
        }
        if (queue_push(&c->queue, &msg) == 0) {
            client_notify(c);
        }
    }

    c->recv_stopped = 1;
    client_notify(c);
    return NULL;
}

static void client_process_queue(client_t *c)
{
    proto_message_t msg;

    while (queue_pop(&c->queue, &msg) == 0) {
        client_handle_message(c, &msg);
    }
}

static void client_handle_message(client_t *c, const proto_message_t *msg)
{
    proto_msg_indication_t ind;
    uint32_t id;
    uint16_t port;
    struct in_addr addr;
    char ip[INET_ADDRSTRLEN];
    const char *desc;

    if (msg->kind == PROTO_INDICATION &&
        msg->type == PROTO_IND_MESSAGE) {
        if (msg->body_len < sizeof(ind)) {
            return;
        }
        memcpy(&ind, msg->body, sizeof(ind));
        id = ntohl(ind.sender_id);
        addr.s_addr = ind.sender_ip;
        if (inet_ntop(AF_INET, &addr, ip, sizeof(ip)) == NULL) {
            strcpy(ip, "?");
        }
        port = ntohs(ind.sender_port);
        printf("\n[收到消息] 来自客户端 #%u (%s:%u): %.*s\n",
               id, ip, port,
               (int)(msg->body_len - sizeof(ind)),
               (char *)msg->body + sizeof(ind));
        return;
    }

    if (msg->kind != PROTO_RESPONSE) {
        return;
    }

    if (msg->status != PROTO_OK) {
        switch (msg->status) {
        case PROTO_ERR_BAD_REQUEST:
            desc = "请求错误";
            break;
        case PROTO_ERR_NOT_FOUND:
            desc = "找不到客户端";
            break;
        case PROTO_ERR_SEND_FAILED:
            desc = "发送失败";
            break;
        case PROTO_ERR_INTERNAL:
            desc = "服务器内部错误";
            break;
        default:
            desc = "未知错误";
            break;
        }
        printf("服务器返回错误(状态码 %u): %s\n", msg->status, desc);
        return;
    }

    switch (msg->type) {
    case PROTO_REQ_TIME:
        c->time_recv++;
        printf("服务器时间: %.*s\n", (int)msg->body_len, (char *)msg->body);
        if (c->time_sent > 0 && c->time_recv >= c->time_sent) {
            printf("共收到 %d 个时间响应。\n", c->time_recv);
        }
        break;
    case PROTO_REQ_NAME:
        printf("服务器名字: %.*s\n", (int)msg->body_len, (char *)msg->body);
        break;
    case PROTO_REQ_LIST:
        client_parse_list(c, msg);
        break;
    case PROTO_REQ_MESSAGE:
        printf("消息已发送成功。\n");
        break;
    default:
        break;
    }
}

static void client_parse_list(client_t *c, const proto_message_t *msg)
{
    uint32_t count;
    uint32_t i;
    const uint8_t *p;

    if (msg->body_len < 4) {
        printf("列表数据格式错误。\n");
        return;
    }
    memcpy(&count, msg->body, 4);
    count = ntohl(count);
    if (count > (msg->body_len - 4) / sizeof(proto_client_info_t)) {
        printf("列表数据格式错误。\n");
        return;
    }

    printf("当前活动连接列表（共 %u 个）:\n", count);
    printf("  编号    IP 地址                端口    状态\n");

    p = msg->body + 4;
    c->peer_count = 0;
    for (i = 0; i < count; i++) {
        proto_client_info_t info;
        struct in_addr a;
        char s[INET_ADDRSTRLEN];

        memcpy(&info, p, sizeof(info));
        p += sizeof(info);

        if (c->peer_count < MAX_PEERS) {
            c->peers[c->peer_count].id = ntohl(info.id);
            c->peers[c->peer_count].port = ntohs(info.port);
            a.s_addr = info.ip;
            if (inet_ntop(AF_INET, &a, s, sizeof(s)) != NULL) {
                snprintf(c->peers[c->peer_count].ip,
                         sizeof(c->peers[c->peer_count].ip), "%s", s);
            } else {
                snprintf(c->peers[c->peer_count].ip,
                         sizeof(c->peers[c->peer_count].ip), "?");
            }
            c->peer_count++;
        }

        a.s_addr = info.ip;
        if (inet_ntop(AF_INET, &a, s, sizeof(s)) == NULL) {
            strcpy(s, "?");
        }
        printf("  #%-5u %-22s %-7u %s\n",
               ntohl(info.id), s, ntohs(info.port),
               ntohs(info.state) == 1 ? "已连接" : "未知");
    }

    c->has_list = 1;
    printf("（已缓存 %d 条连接信息，可用于发消息）\n", c->peer_count);
}

static void client_get_time(client_t *c)
{
    int i;

    if (!c->connected) {
        printf("尚未连接。\n");
        return;
    }

    c->time_sent = TIME_REQUEST_COUNT;
    c->time_recv = 0;
    for (i = 0; i < TIME_REQUEST_COUNT; i++) {
        client_send_request(c, PROTO_REQ_TIME, NULL, 0);
        if (!c->connected) {
            break;
        }
    }
    if (c->connected && TIME_REQUEST_COUNT > 1) {
        printf("已发送 %d 个时间请求。\n", TIME_REQUEST_COUNT);
    }
}

static void client_get_name(client_t *c)
{
    client_send_request(c, PROTO_REQ_NAME, NULL, 0);
}

static void client_get_list(client_t *c)
{
    client_send_request(c, PROTO_REQ_LIST, NULL, 0);
}

static void client_send_message(client_t *c)
{
    char idstr[16];
    char text[PROTO_MAX_TEXT];
    uint8_t body[4 + PROTO_MAX_TEXT];
    uint32_t target_id;
    uint32_t net;
    size_t text_len;

    if (!c->connected) {
        printf("尚未连接。\n");
        return;
    }
    if (!c->has_list) {
        printf("请先选择 5 获取活动连接列表。\n");
        return;
    }

    printf("目标客户端编号: ");
    fflush(stdout);
    if (read_line(idstr, sizeof(idstr)) < 0 || idstr[0] == '\0') {
        printf("输入无效。\n");
        return;
    }
    target_id = (uint32_t)atoi(idstr);

    printf("消息内容: ");
    fflush(stdout);
    if (read_line(text, sizeof(text)) < 0 || text[0] == '\0') {
        printf("消息内容不能为空。\n");
        return;
    }
    text_len = strlen(text);
    if (text_len > PROTO_MAX_TEXT) {
        printf("消息过长。\n");
        return;
    }

    net = htonl(target_id);
    memcpy(body, &net, 4);
    memcpy(body + 4, text, text_len);
    client_send_request(c, PROTO_REQ_MESSAGE, body,
                        (uint32_t)(4 + text_len));
}

static void queue_init(msg_queue_t *q)
{
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mutex, NULL);
}

static void queue_destroy(msg_queue_t *q)
{
    pthread_mutex_destroy(&q->mutex);
}

static int queue_push(msg_queue_t *q, const proto_message_t *msg)
{
    int rc = -1;

    pthread_mutex_lock(&q->mutex);
    if (q->count < CLIENT_QUEUE_SIZE) {
        q->items[q->tail] = *msg;
        q->tail = (q->tail + 1) % CLIENT_QUEUE_SIZE;
        q->count++;
        rc = 0;
    }
    pthread_mutex_unlock(&q->mutex);
    return rc;
}

static int queue_pop(msg_queue_t *q, proto_message_t *msg)
{
    int rc = -1;

    pthread_mutex_lock(&q->mutex);
    if (q->count > 0) {
        *msg = q->items[q->head];
        q->head = (q->head + 1) % CLIENT_QUEUE_SIZE;
        q->count--;
        rc = 0;
    }
    pthread_mutex_unlock(&q->mutex);
    return rc;
}

int main(void)
{
    static client_t client;

    client_init(&client);
    client_run(&client);
    client_cleanup(&client);
    return 0;
}
