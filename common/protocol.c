#include "protocol.h"
#include "net.h"

#include <arpa/inet.h>
#include <stddef.h>
#include <string.h>

_Static_assert(sizeof(proto_header_t) == PROTO_HEADER_SIZE,
               "proto_header_t must be 16 bytes");
_Static_assert(sizeof(proto_client_info_t) == 12,
               "proto_client_info_t must be 12 bytes");
_Static_assert(sizeof(proto_msg_indication_t) == 12,
               "proto_msg_indication_t must be 12 bytes");

void proto_header_encode(proto_header_t *wire, uint8_t kind, uint8_t type,
                         uint8_t status, uint32_t seq, uint32_t body_len)
{
    memset(wire, 0, sizeof(*wire));
    wire->magic = htonl(PROTO_MAGIC);
    wire->version = PROTO_VERSION;
    wire->kind = kind;
    wire->type = type;
    wire->status = status;
    wire->seq = htonl(seq);
    wire->body_len = htonl(body_len);
}

int proto_header_decode(const proto_header_t *wire, proto_message_t *msg)
{
    uint32_t body_len;

    if (ntohl(wire->magic) != PROTO_MAGIC) {
        return -1;
    }
    if (wire->version != PROTO_VERSION) {
        return -1;
    }

    body_len = ntohl(wire->body_len);
    if (body_len > PROTO_MAX_BODY) {
        return -1;
    }

    msg->kind = wire->kind;
    msg->type = wire->type;
    msg->status = wire->status;
    msg->seq = ntohl(wire->seq);
    msg->body_len = body_len;
    return 0;
}

int proto_send_packet(int fd, uint8_t kind, uint8_t type, uint8_t status,
                      uint32_t seq, const void *body, uint32_t body_len)
{
    proto_header_t header;

    if (body_len > PROTO_MAX_BODY) {
        return -1;
    }
    if (body_len > 0 && body == NULL) {
        return -1;
    }

    proto_header_encode(&header, kind, type, status, seq, body_len);
    if (net_send_all(fd, &header, sizeof(header)) < 0) {
        return -1;
    }
    if (body_len > 0 && net_send_all(fd, body, body_len) < 0) {
        return -1;
    }
    return 0;
}

int proto_recv_packet(int fd, proto_message_t *msg)
{
    proto_header_t header;

    memset(msg, 0, sizeof(*msg));
    if (net_recv_all(fd, &header, sizeof(header)) < 0) {
        return -1;
    }
    if (proto_header_decode(&header, msg) < 0) {
        return -1;
    }
    if (msg->body_len > 0 &&
        net_recv_all(fd, msg->body, msg->body_len) < 0) {
        return -1;
    }
    return 0;
}
