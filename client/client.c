#include "protocol.h"
#include "net.h"

/* 框架版：菜单、连接和接收线程骨架已给出，TODO 处补业务逻辑。 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#define CLIENT_QUEUE_SIZE 16
#define DEFAULT_IP        "127.0.0.1"
#define DEFAULT_PORT      "50007"   /* 临时端口，报告前改成学号后四位 */

/* 接收线程放入消息，主线程取出处理。 */
typedef struct {
    proto_message_t items[CLIENT_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
} msg_queue_t;

typedef struct {
    int sockfd;
    volatile int connected;
    volatile int running;

    pthread_t recv_thread;
    int recv_thread_started;

    uint32_t next_seq;
    int has_client_list;
    msg_queue_t queue;

    /* 接收线程写入一个字节，唤醒正在 select() 的主线程。 */
    int wake_pipe[2];
} client_t;

static void print_menu(void);
static int read_line(char *buf, size_t size);

static void queue_init(msg_queue_t *q);
static void queue_destroy(msg_queue_t *q);
static void queue_clear(msg_queue_t *q);
static int queue_push(msg_queue_t *q, const proto_message_t *msg);
static int queue_pop(msg_queue_t *q, proto_message_t *msg);

static void client_init(client_t *c);
static void client_cleanup(client_t *c);
static void client_run(client_t *c);
static void client_handle_choice(client_t *c);

static int client_connect(client_t *c, const char *ip, const char *port_text);
static void client_disconnect(client_t *c);
static int client_send_request(client_t *c, uint8_t type,
                               const void *body, uint32_t body_len);

static void *client_receiver(void *arg);
static void client_process_queue(client_t *c);
static void client_handle_message(client_t *c, const proto_message_t *msg);

static void client_get_time(client_t *c);
static void client_get_name(client_t *c);
static void client_get_list(client_t *c);
static void client_send_message(client_t *c);

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
    } else {
        int ch;
        while ((ch = getchar()) != '\n' && ch != EOF) {
            /* 丢弃过长行的剩余部分 */
        }
    }
    return 0;
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

static void queue_clear(msg_queue_t *q)
{
    pthread_mutex_lock(&q->mutex);
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    pthread_mutex_unlock(&q->mutex);
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

static void client_init(client_t *c)
{
    memset(c, 0, sizeof(*c));
    c->sockfd = -1;
    c->running = 1;
    c->wake_pipe[0] = -1;
    c->wake_pipe[1] = -1;
    queue_init(&c->queue);
    signal(SIGPIPE, SIG_IGN);

    if (pipe(c->wake_pipe) < 0) {
        perror("pipe");
        c->wake_pipe[0] = -1;
        c->wake_pipe[1] = -1;
    }
}

static void client_cleanup(client_t *c)
{
    client_disconnect(c);
    queue_destroy(&c->queue);

    if (c->wake_pipe[0] >= 0) {
        close(c->wake_pipe[0]);
    }
    if (c->wake_pipe[1] >= 0) {
        close(c->wake_pipe[1]);
    }
}

static int client_connect(client_t *c, const char *ip, const char *port_text)
{
    struct sockaddr_in addr;
    char *end = NULL;
    long port;
    int fd;

    if (c->connected) {
        printf("已经连接。\n");
        return 0;
    }
    if (c->recv_thread_started || c->sockfd >= 0) {
        client_disconnect(c);
    }

    port = strtol(port_text, &end, 10);
    if (end == port_text || *end != '\0' || port < 1 || port > 65535) {
        printf("端口不合法。\n");
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        printf("IP 地址不合法。\n");
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }

    c->sockfd = fd;
    c->connected = 1;
    c->has_client_list = 0;
    queue_clear(&c->queue);

    if (pthread_create(&c->recv_thread, NULL, client_receiver, c) != 0) {
        perror("pthread_create");
        c->connected = 0;
        close(fd);
        c->sockfd = -1;
        return -1;
    }
    c->recv_thread_started = 1;
    return 0;
}

static void client_disconnect(client_t *c)
{
    if (!c->connected && !c->recv_thread_started && c->sockfd < 0) {
        return;
    }

    c->connected = 0;
    c->has_client_list = 0;
    if (c->sockfd >= 0) {
        shutdown(c->sockfd, SHUT_RDWR);
    }
    if (c->recv_thread_started) {
        pthread_join(c->recv_thread, NULL);
        c->recv_thread_started = 0;
    }
    if (c->sockfd >= 0) {
        close(c->sockfd);
        c->sockfd = -1;
    }
    printf("已断开连接。\n");
}

static int client_send_request(client_t *c, uint8_t type,
                               const void *body, uint32_t body_len)
{
    if (!c->connected) {
        printf("尚未连接服务器。\n");
        return -1;
    }

    c->next_seq++;
    return proto_send_packet(c->sockfd, PROTO_REQUEST, type, 0,
                             c->next_seq, body, body_len);
}

static void *client_receiver(void *arg)
{
    client_t *c = (client_t *)arg;
    proto_message_t msg;

    while (c->connected) {
        if (proto_recv_packet(c->sockfd, &msg) < 0) {
            break;
        }

        if (queue_push(&c->queue, &msg) == 0) {
            char wake = 1;
            if (c->wake_pipe[1] >= 0) {
                if (write(c->wake_pipe[1], &wake, 1) < 0) {
                    /* 主线程可能已经退出，忽略即可。 */
                }
            }
        } else {
            printf("接收队列已满，丢弃一条消息。\n");
        }
    }

    c->connected = 0;
    if (c->wake_pipe[1] >= 0) {
        char wake = 1;
        if (write(c->wake_pipe[1], &wake, 1) < 0) {
            /* 主线程可能已经退出，忽略即可。 */
        }
    }
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
    printf("收到报文: kind=%u type=%u status=%u body_len=%u\n",
           msg->kind, msg->type, msg->status, msg->body_len);

    switch (msg->kind) {
    case PROTO_RESPONSE:
        /* TODO: 按 msg->type 打印时间、名字、客户端列表或发送结果。 */
        if (msg->type == PROTO_REQ_LIST && msg->status == PROTO_OK) {
            c->has_client_list = 1;
        }
        break;
    case PROTO_INDICATION:
        /* TODO: 打印服务器转发的消息，包括发送者编号、IP、端口和内容。 */
        break;
    default:
        break;
    }
}

static void client_get_time(client_t *c)
{
    /* 报告中的“自动发送 100 次时间请求”可以在这里循环调用。 */
    if (client_send_request(c, PROTO_REQ_TIME, NULL, 0) < 0) {
        printf("时间请求发送失败。\n");
    }
}

static void client_get_name(client_t *c)
{
    if (client_send_request(c, PROTO_REQ_NAME, NULL, 0) < 0) {
        printf("名字请求发送失败。\n");
    }
}

static void client_get_list(client_t *c)
{
    if (client_send_request(c, PROTO_REQ_LIST, NULL, 0) < 0) {
        printf("列表请求发送失败。\n");
    }
}

static void client_send_message(client_t *c)
{
    if (!c->has_client_list) {
        printf("请先获取客户端列表。\n");
        return;
    }
    /* TODO:
     * 1. 提示输入目标客户端编号和消息内容。
     * 2. 组装 body：target_id(4 字节，网络字节序) + 消息文本。
     * 3. 调用 client_send_request(c, PROTO_REQ_MESSAGE, body, body_len)。
     */
    printf("TODO: 发消息功能待实现。\n");
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
    case 1: {
        char ip[64];
        char port[16];

        if (c->connected) {
            printf("已经连接。\n");
            break;
        }

        printf("服务器 IP（默认 %s）: ", DEFAULT_IP);
        fflush(stdout);
        if (read_line(ip, sizeof(ip)) < 0) {
            c->running = 0;
            break;
        }
        if (ip[0] == '\0') {
            strcpy(ip, DEFAULT_IP);
        }

        printf("端口（默认 %s）: ", DEFAULT_PORT);
        fflush(stdout);
        if (read_line(port, sizeof(port)) < 0) {
            c->running = 0;
            break;
        }
        if (port[0] == '\0') {
            strcpy(port, DEFAULT_PORT);
        }

        if (client_connect(c, ip, port) == 0) {
            printf("连接成功。\n");
        } else {
            printf("连接失败。\n");
        }
        break;
    }
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
        c->running = 0;
        break;
    default:
        printf("无效选项。\n");
        break;
    }
}

static void client_run(client_t *c)
{
    print_menu();

    while (c->running) {
        fd_set readfds;
        int maxfd = STDIN_FILENO;
        int ret;

        FD_ZERO(&readfds);
        FD_SET(STDIN_FILENO, &readfds);

        if (c->wake_pipe[0] >= 0) {
            FD_SET(c->wake_pipe[0], &readfds);
            if (c->wake_pipe[0] > maxfd) {
                maxfd = c->wake_pipe[0];
            }
        }

        ret = select(maxfd + 1, &readfds, NULL, NULL, NULL);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }

        if (c->wake_pipe[0] >= 0 && FD_ISSET(c->wake_pipe[0], &readfds)) {
            char wake;
            if (read(c->wake_pipe[0], &wake, 1) < 0) {
                /* 没有数据可读，忽略。 */
            }
            client_process_queue(c);
        }

        if (FD_ISSET(STDIN_FILENO, &readfds)) {
            client_handle_choice(c);
            if (c->running) {
                print_menu();
            }
        }
    }
}

int main(void)
{
    client_t client;

    client_init(&client);
    client_run(&client);
    client_cleanup(&client);
    return 0;
}
