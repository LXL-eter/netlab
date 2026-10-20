# Lab7

基于 TCP 和自定义二进制应用层协议的客户端/服务端通信程序，支持获取时间、获取主机名、活动连接列表和客户端消息转发。服务端监听 `5718`，每个客户端连接由独立线程处理，客户端列表和 socket 写入分别使用互斥锁保护。

## 目录

```text
lab7/
├── common/      公共协议和收发函数
├── client/      客户端
├── server/      服务端
├── Makefile
└── README.md
```

## 编译

Linux 或 WSL2：

```bash
make
```

生成的程序：

```text
client/client
server/server
```

MacBook M3 生成 Linux x86_64 静态 ELF：

```bash
docker run --rm --platform linux/amd64 \
  -v "$PWD":/src -w /src alpine:3.20 \
  sh -c "apk add --no-cache build-base && make clean all STATIC=1"
```

可以用 `file client/client server/server` 检查生成结果。

## 运行

```bash
./server/server
./client/client
```

服务端端口固定为 `5718`。

## 协议

报文头固定为 16 字节，多字节整数采用网络字节序：

```text
magic(4) | version(1) | kind(1) | type(1) | status(1) | seq(4) | body_len(4)
```

- `kind`：1 请求，2 响应，3 指示
- `type`：1 时间，2 名字，3 列表，4 发消息
- `status`：0 成功，1 请求错误，2 找不到客户端，3 发送失败，4 服务器内部错误

主要 body 格式：

- 时间、名字：UTF-8 文本
- 客户端列表：`count(4)` + `id(4) + ip(4) + port(2) + state(2)`
- 发消息请求：`target_id(4)` + UTF-8 文本
- 转发指示：`sender_id(4) + sender_ip(4) + sender_port(2) + reserved(2)` + UTF-8 文本

客户端编号单调递增且不复用，活动列表包含请求者自身；消息目标为其他已连接客户端。
