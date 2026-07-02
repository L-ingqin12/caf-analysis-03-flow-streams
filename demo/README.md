# CAF Flow Streams Demo

## 概述

本 Demo 演示 CAF Flow 子系统的四个核心概念：

1. **Observable/Observer 模式** — 手动订阅与自定义 Observer
2. **操作符管道** — `map`, `filter`, `take`, `scan`, `reduce` 的链式调用
3. **背压机制** — `on_backpressure_buffer` 与三种溢出策略（fail / drop_oldest / drop_newest）
4. **Flow 与 Actor 集成** — 跨 actor 的数据流桥接

## 架构说明

```
caf_main
  ├── Demo 1: Observable/Observer 模式
  │     └── 自定义 int_observer → 手动控制 request(n) 展现背压
  ├── Demo 2: 操作符管道 (map · filter · take · scan · reduce)
  │     ├── A) map(平方) → filter(>10) → take(5) → for_each
  │     ├── B) scan(运行总和)
  │     └── C) reduce(累乘)
  ├── Demo 3: 背压机制 (on_backpressure_buffer)
  │     ├── fail: 缓冲满则报错
  │     ├── drop_oldest: 丢弃最旧元素
  │     └── drop_newest: 丢弃最新元素
  └── Demo 4: Flow 与 Actor 集成
        ├── A) Actor 内嵌 Flow
        ├── B) observe_on 跨 Actor 桥接 (SPSC 无锁缓冲区)
        └── C) SPSC 手动桥接
```

## 演示核心概念

### Observable / Observer

- 生产者（`observable<T>`）通过 `subscribe()` 连接消费者
- 消费者（`observer<T>`）实现 `on_subscribe` / `on_next` / `on_complete` / `on_error` 四个回调
- `subscription::request(n)` 是背压信号，控制数据流速

### Steps 编译期熔合

- `map`, `filter`, `take` 等轻量变换被编译期熔合为单一 operator
- 链式调用 `map().filter().map()` 最终生成单一的 `from_generator` 类
- 零额外虚函数开销

### 背压策略

| 策略 | 行为 |
|------|------|
| `fail` | 缓冲区满时发出 `on_error` |
| `drop_oldest` | 丢弃缓冲区中最旧的元素 |
| `drop_newest` | 丢弃最新到达的元素（当缓冲区满时） |

### Actor 集成

| 方式 | 说明 |
|------|------|
| Actor 内嵌 Flow | 同一 actor 内部创建 flow pipeline |
| observe_on | 通过 SPSC 无锁缓冲区桥接两个不同 actor |
| SPSC 手动桥接 | 手动创建 `producer_resource` / `consumer_resource` |

## 编译与运行

### 前置条件

- CMake >= 3.14
- C++20 编译器
- Git（用于 FetchContent 拉取 CAF）

### 编译步骤

```bash
cd /root/caf-analysis-output/03-flow-streams/demo
mkdir -p build && cd build
cmake ..
cmake --build .
```

### 运行

```bash
./caf-flow-demo
```

### 预期输出

程序将按顺序输出四个 Demo 的运行结果：

```
============================================================
  Demo 1: Observable / Observer 模式
============================================================
--- 手动 Observer —— 从 vector 发射数据 ---
    [observer] on_subscribe -> request(3)
    [observer] on_next(10)  (buf=1)
    ...
    [PASS] 收到 5 个元素，与输入一致

============================================================
  Demo 2: 操作符管道
============================================================
--- A) map(平方) → filter(>10) → take(5) → for_each ---
      x = 16
      ...
```

## 关键代码

| 功能 | 行号 |
|------|------|
| 自定义 Observer（背压控制） | `main.cpp:72-113` |
| map-filter-take 链 | `main.cpp:157-181` |
| scan 累积扫描 | `main.cpp:183-199` |
| reduce 规约 | `main.cpp:201-217` |
| 背压策略演示 | `main.cpp:276-316` |
| Actor 内嵌 Flow | `main.cpp:332-343` |
| observe_on 跨 actor | `main.cpp:346-361` |
| SPSC 手动桥接 | `main.cpp:365-385` |
