# Lab7 框架说明

## 目录

```
lab7/
├── common/          公共协议和收发函数
├── client/          客户端
├── server/          服务端
├── Makefile
└── README.md
```

## 编译

Linux 或 WSL2 下直接执行：

```bash
make
```

生成两个可执行文件：

```text
client/client
server/server
```

MacBook M3 要生成 Linux x86_64 ELF，使用 Docker：

```bash
docker run --rm --platform linux/amd64 \
  -v "$PWD":/src -w /src alpine:3.20 \
  sh -c "apk add --no-cache build-base && make clean all STATIC=1"
```

Docker Desktop 需要开启 Rosetta。这样得到的是 x86_64 静态 ELF，不依赖目标机的 glibc。最终提交前用 `file` 检查：

```bash
file client/client server/server
```

## 运行

```bash
./server/server 50007
./client/client
```

端口 `50007` 只是临时值，后面换成学号后四位；若第一位是 0，按文档规则写成 `1XXXX`。

## 协议

固定 16 字节报文头，多字节整数使用网络字节序：

```text
magic(4) | version(1) | kind(1) | type(1) | status(1) | seq(4) | body_len(4)
```

- `kind`：1 请求，2 响应，3 指示
- `type`：1 时间，2 名字，3 列表，4 发消息
- `status`：0 成功，1 请求错误，2 找不到客户端，3 发送失败，4 服务器内部错误
- `body_len`：后面的 body 字节数，最大 8192

body 约定：

- 时间/名字：UTF-8 文本
- 客户端列表：`count(4)` + `count` 条 `id(4) + ip(4) + port(2) + state(2)`
- 发消息请求：`target_id(4)` + UTF-8 消息文本
- 转发指示：`sender_id(4) + sender_ip(4) + sender_port(2) + reserved(2)` + UTF-8 消息文本
- 错误响应：可放 UTF-8 错误说明

约定：客户端编号单调递增、不复用；活动列表默认包含请求者自己；发消息的目标必须是其他已连接客户端，目标不存在或给自己发时返回错误。两端都只包含 `common/protocol.h`，不要各复制一份。

## 已给出和待完成

已给出：协议头、完整收发、客户端菜单和接收线程、消息队列、服务端 accept 循环、客户端列表和线程骨架。

待完成（代码中有 `TODO`）：

- 服务端时间、名字、客户端列表、消息转发处理
- 客户端响应和转发消息的解析打印
- 客户端“发消息”的输入和请求组装
- 报告要求的 100 次时间请求测试
