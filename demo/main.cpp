// CAF Flow & Streams 模块综合演示
//
// 本文件演示 CAF Flow 子系统的四个核心概念：
//   1. Observable/Observer 模式 —— 手动订阅与自定义 Observer
//   2. 操作符管道 —— map, filter, take, scan, reduce 的链式调用
//   3. 背压机制    —— on_backpressure_buffer 与三种溢出策略
//   4. Flow 与 Actor 集成 —— 跨 actor 的数据流桥接

#include <array>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

// =============================================================================
// 1. 独立 Flow 上下文 (ScopedCoordinator) 所需头文件
// =============================================================================
#include "caf/flow/backpressure_overflow_strategy.hpp"
#include "caf/flow/observable.hpp"
#include "caf/flow/observable_builder.hpp"
#include "caf/flow/observer.hpp"
#include "caf/flow/scoped_coordinator.hpp"
#include "caf/flow/subscription.hpp"

// =============================================================================
// 2. Actor 集成所需头文件
// =============================================================================
#include "caf/actor_system.hpp"
#include "caf/caf_main.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/scheduled_actor/flow.hpp"

// =============================================================================
// 辅助：控制台彩色输出
// =============================================================================
namespace {

constexpr auto reset   = "\033[0m";
constexpr auto red_b   = "\033[1;31m";
constexpr auto grn_b   = "\033[1;32m";
constexpr auto ylw_b   = "\033[1;33m";
constexpr auto blu_b   = "\033[1;34m";
constexpr auto mag_b   = "\033[1;35m";
constexpr auto cyn_b   = "\033[1;36m";

void print_header(const std::string& title) {
  std::cout << '\n'
            << cyn_b << "============================================================"
            << reset << '\n'
            << cyn_b << "  " << title << reset << '\n'
            << cyn_b << "============================================================"
            << reset << '\n';
}

void print_subheader(const std::string& title) {
  std::cout << '\n' << ylw_b << "--- " << title << " ---" << reset << '\n';
}

} // namespace

// =============================================================================
// Demo 1: Observable / Observer 模式 —— 手动订阅
// =============================================================================
//
// 创建一个自定义 Observer，显式地实现 on_subscribe / on_next / on_complete /
// on_error 四个回调，并手动控制 request 数量来展现背压信号传递。
//
namespace demo1 {

// 带背压控制的整数 Observer
class int_observer : public caf::flow::observer_impl_base<int> {
public:
  explicit int_observer(caf::flow::coordinator* parent,
                        std::vector<int>& output)
      : parent_(parent), output_(output) {}

  caf::flow::coordinator* parent() const noexcept override {
    return parent_;
  }

  void on_subscribe(caf::flow::subscription sub) override {
    sub_ = sub;
    // 初始时只 demand 3 个——刻意限制，展示背压
    std::cout << "    [observer] on_subscribe -> request(3)\n";
    sub_.request(3);
  }

  void on_next(const int& item) override {
    output_.push_back(item);
    std::cout << "    [observer] on_next(" << item << ")  (buf=" << output_.size()
              << ")\n";
    // 每次消费后补充 1 个 demand
    sub_.request(1);
  }

  void on_complete() override {
    std::cout << "    [observer] on_complete()  (total=" << output_.size()
              << ")\n";
    sub_.release_later();
  }

  void on_error(const caf::error& what) override {
    std::cout << "    [observer] on_error: " << to_string(what) << '\n';
    sub_.release_later();
  }

private:
  caf::flow::coordinator* parent_;
  caf::flow::subscription sub_;
  std::vector<int>& output_;
};

void run() {
  print_subheader("手动 Observer —— 从 vector 发射数据");

  auto coord = caf::flow::make_scoped_coordinator();

  // 数据源
  auto inputs = std::vector{10, 20, 30, 40, 50};

  // 创建 observer（通过 coordinator 托管生命周期）
  std::vector<int> results;
  auto obs_ptr
      = coord->add_child(std::in_place_type<int_observer>, &results);
  caf::flow::observer<int> obs{obs_ptr};

  // 构建 pipeline 并订阅
  coord->make_observable()
      .from_container(inputs)
      .as_observable()
      .subscribe(obs);

  // 驱动执行 —— 背压控制自动生效
  coord->run();

  // 验证结果
  assert(results == inputs);
  std::cout << "    [PASS] 收到 " << results.size() << " 个元素，与输入一致\n";
}

} // namespace demo1

// =============================================================================
// Demo 2: 操作符管道  map / filter / take / scan / reduce
// =============================================================================
//
// 演示编译期熔合的 Steps 链式调用。所有 steps 累积到 tuple 中，
// materialize() 时产生单一 from_generator 类，零额外虚函数开销。
//
namespace demo2 {

void run() {
  print_subheader("操作符管道：map · filter · take · scan · reduce");

  auto coord = caf::flow::make_scoped_coordinator();

  // (A) map + filter + take + for_each
  {
    print_subheader("A) map(平方) → filter(>10) → take(5) → for_each");
    std::vector<int> results;
    // 生成 {1,2,3,...,20}
    coord->make_observable()
        .iota(1)
        .take(20) // 取前 20 个
        .map([](int x) { return x * x; }) // 平方
        .filter([](int x) { return x > 10; }) // 大于 10
        .take(5) // 只取满足条件的前 5 个
        .for_each([&results](int x) {
          results.push_back(x);
          std::cout << "      x = " << x << '\n';
        });
    coord->run();
    // 预期: 4^2=16, 5^2=25, 6^2=36, 7^2=49, 8^2=64
    assert(results.size() == 5);
    assert(results[0] == 16);
    assert(results[4] == 64);
    std::cout << "    [PASS] map-filter-take 链，共 " << results.size()
              << " 个结果\n";
  }

  // (B) scan (累积扫描)
  {
    print_subheader("B) scan (运行总和)");
    std::vector<int> results;
    coord->make_observable()
        .iota(1)   // 1, 2, 3, 4, 5
        .take(5)
        .scan(0, [](int acc, int x) { return acc + x; })
        .for_each([&results](int x) {
          results.push_back(x);
          std::cout << "      acc = " << x << '\n';
        });
    coord->run();
    // 期望: 1, 3, 6, 10, 15
    assert(results.size() == 5);
    assert(results.back() == 15);
    std::cout << "    [PASS] scan 累积和: last = " << results.back() << '\n';
  }

  // (C) reduce (规约为单值)
  {
    print_subheader("C) reduce (累乘)");
    int result = 0;
    coord->make_observable()
        .iota(1) // 1, 2, 3, 4, 5
        .take(5)
        .reduce(1, [](int acc, int x) { return acc * x; })
        .for_each([&result](int x) {
          result = x;
          std::cout << "      product = " << x << '\n';
        });
    coord->run();
    assert(result == 120);
    std::cout << "    [PASS] reduce 累乘 = " << result << '\n';
  }
}

} // namespace demo2

// =============================================================================
// Demo 3: 背压机制 — on_backpressure_buffer 与溢出策略
// =============================================================================
//
// 创建一个快速生产者 + 慢速消费者，通过 on_backpressure_buffer 插入缓冲，
// 并分别用 fail / drop_oldest / drop_newest 三种策略观察行为差异。
//
namespace demo3 {

// 慢速 Observer：初始只 request(2)，每次 on_next 后延迟补充，制造背压
class slow_observer : public caf::flow::observer_impl_base<int> {
public:
  explicit slow_observer(caf::flow::coordinator* parent,
                         std::vector<int>& output)
      : parent_(parent), output_(output) {}

  caf::flow::coordinator* parent() const noexcept override {
    return parent_;
  }

  void on_subscribe(caf::flow::subscription sub) override {
    sub_ = sub;
    // 仅初始 demand 2，之后生产者会填满 buffer
    std::cout << "    [slow_observer] initial request(2)\n";
    sub_.request(2);
  }

  void on_next(const int& item) override {
    output_.push_back(item);
    // 不立即补充 demand —— 制造背压 （每收到 3 个才补 1 个）
    static size_t count = 0;
    if (++count % 3 == 0) {
      std::cout << "    [slow_observer] replenish request(1) after 3 items\n";
      sub_.request(1);
    }
  }

  void on_complete() override {
    std::cout << "    [slow_observer] on_complete(), total="
              << output_.size() << '\n';
    sub_.release_later();
  }

  void on_error(const caf::error& what) override {
    std::cout << "    [slow_observer] on_error: " << to_string(what) << '\n';
    sub_.release_later();
  }

private:
  caf::flow::coordinator* parent_;
  caf::flow::subscription sub_;
  std::vector<int>& output_;
};

void demo_backpressure(caf::flow::backpressure_overflow_strategy strategy,
                       const std::string& label) {
  print_subheader(label);

  auto coord = caf::flow::make_scoped_coordinator();

  std::vector<int> results;
  auto obs_ptr
      = coord->add_child(std::in_place_type<slow_observer>, &results);
  caf::flow::observer<int> obs{obs_ptr};

  coord->make_observable()
      .iota(1)              // 快速生产者：无限递增
      .take(20)             // 取前 20 个
      .as_observable()
      .on_backpressure_buffer(5, strategy) // 缓冲 5 个，溢出按策略处理
      .subscribe(obs);

  coord->run();

  std::cout << "    [DONE] strategy=" << label << ", received="
            << results.size() << " items\n";
}

void run() {
  print_subheader("背压演示 ---- on_backpressure_buffer");

  // Strategy: fail —— 缓冲满时发出 on_error
  demo_backpressure(
      caf::flow::backpressure_overflow_strategy::fail,
      "fail (缓冲满则报错)");

  // Strategy: drop_oldest —— 丢弃最旧元素
  demo_backpressure(
      caf::flow::backpressure_overflow_strategy::drop_oldest,
      "drop_oldest (丢弃最旧)");

  // Strategy: drop_newest —— 丢弃最新元素
  demo_backpressure(
      caf::flow::backpressure_overflow_strategy::drop_newest,
      "drop_newest (丢弃最新)");
}

} // namespace demo3

// =============================================================================
// Demo 4: Flow 与 Actor 集成
// =============================================================================
//
// 展示三种集成方式：
//   A) Actor 内部 flow 管道
//   B) observe_on 跨 actor 桥接
//   C) SPSC 无锁缓冲区手动桥接
//
namespace demo4 {

// ---- A) Actor 内嵌 Flow ----
void actor_internal_flow(caf::event_based_actor* self) {
  print_subheader("A) Actor 内嵌 Flow —— 同一 actor 内数据管道");

  self->make_observable()
      .iota(1)
      .take(5)
      .map([](int x) { return x * 10; })
      .for_each([self](int x) {
        self->println("    [actor] internal flow: x = {}", x);
      });
}

// ---- B) observe_on 跨 Actor 桥接 ----
//
// src actor 通过 observe_on(snk) 将 flow 切换到 snk actor 上执行，
// 中间由 SPSC 无锁缓冲区透明桥接。
//
void actor_cross_observe_on(caf::event_based_actor* src,
                            caf::event_based_actor* snk) {
  print_subheader("B) observe_on 跨 Actor 桥接");

  src->make_observable()
      .from_container(std::vector{100, 200, 300})
      .map([](int x) { return x + 1; })
      .observe_on(snk) // 切换到 snk 的协调器
      .for_each([snk](int x) {
        snk->println("    [snk] received via observe_on: {}", x);
      });
}

// ---- C) SPSC 无锁缓冲区手动桥接 ----
//
// 手动创建 SPSC buffer 的两端，分别传给两个 actor。
// src 写入 producer_resource，snk 从 consumer_resource 消费。
//
void actor_spsc_bridge(caf::event_based_actor* src,
                       caf::event_based_actor* snk,
                       caf::async::producer_resource<int> push,
                       caf::async::consumer_resource<int> pull) {
  print_subheader("C) SPSC Buffer 手动桥接");

  // 源端：写入 resource
  src->make_observable()
      .from_container(std::vector{1, 2, 3, 4, 5})
      .subscribe(push);

  // 宿端：从 resource 读取
  snk->make_observable()
      .from_resource(std::move(pull))
      .map([](int x) { return x * x; })
      .for_each([snk](int x) {
        snk->println("    [snk] received via SPSC: {}", x);
      });
}

void caf_main(caf::actor_system& sys) {
  print_subheader("Actor 集成演示");

  // A) 单个 actor，内嵌 flow
  sys.spawn(actor_internal_flow);

  // B) observe_on 跨 actor
  {
    auto [snk, launch_snk] = sys.spawn_inactive();
    auto [src, launch_src] = sys.spawn_inactive();
    actor_cross_observe_on(src, snk);
    launch_src();
    launch_snk();
  }

  // C) SPSC 手动桥接
  {
    auto [pull, push] = caf::async::make_spsc_buffer_resource<int>();
    auto [snk, launch_snk] = sys.spawn_inactive();
    auto [src, launch_src] = sys.spawn_inactive();
    actor_spsc_bridge(src, snk, std::move(push), std::move(pull));
    launch_src();
    launch_snk();
  }
}

} // namespace demo4

// =============================================================================
// main: 顺序执行所有 Demo
// =============================================================================

void caf_main(caf::actor_system& sys) {
  print_header("Demo 1: Observable / Observer 模式");
  demo1::run();

  print_header("Demo 2: 操作符管道 (map · filter · take · scan · reduce)");
  demo2::run();

  print_header("Demo 3: 背压机制 (on_backpressure_buffer)");
  demo3::run();

  print_header("Demo 4: Flow 与 Actor 集成");
  demo4::caf_main(sys);
}

CAF_MAIN()
