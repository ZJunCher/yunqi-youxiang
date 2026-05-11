# 云栖优享

云栖优享是一个基于 Spring Boot 的本地生活服务平台后端项目，围绕商铺浏览、优惠券秒杀、用户互动、签到、关注、达人探店等场景展开，并重点实践了 Redis 缓存优化、Lua 原子校验、Kafka 异步下单、滑动窗口限流、多级缓存、超时关单等高并发业务方案。

## 项目定位

本项目是一个面向 Java 后端学习与实践的开源项目，前身参考了经典的黑马点评业务场景。在此基础上，我围绕秒杀高并发、缓存一致性、订单超时关单、接口限流、多级缓存、消息队列异步化等方向进行了持续优化和扩展，并将代码整理开源，方便大家交流学习、理解业务场景和实践后端核心技术。

## 项目亮点

- **优惠券秒杀**：使用 Redis + Lua 完成库存校验、一人一单校验和库存预扣，降低 MySQL 在秒杀入口的压力。
- **异步下单**：秒杀资格判断成功后发送 Kafka 消息，由消费者异步扣减 MySQL 库存并创建订单。
- **订单关单**：定时扫描超时未支付订单，使用乐观锁关闭订单并释放 MySQL/Redis 库存。
- **缓存优化**：使用缓存空值解决缓存穿透，使用逻辑过期解决热点 Key 缓存击穿。
- **多级缓存**：使用 Caffeine 本地缓存 + Redis 缓存，降低热点店铺数据对 Redis 的访问压力。
- **数据一致性**：更新数据库后删除缓存，删除失败时通过 Kafka 补偿重试，并配合 TTL 兜底。
- **接口限流**：基于自定义注解、AOP、Redis ZSet 和 Lua 实现滑动窗口限流，支持全局、用户、IP 维度。

## 技术栈

- Java
- Spring Boot
- MyBatis-Plus
- MySQL
- Redis / Redisson
- Kafka
- Caffeine
- Lua
- Hutool

## 核心模块

```text
src/main/java/com/yqyx
├── controller      接口层
├── service         业务接口
├── service/impl    业务实现
├── mapper          MyBatis-Plus Mapper
├── entity          数据实体
├── dto             数据传输对象
├── config          Spring 配置
├── limiter         接口限流
└── utils           Redis、缓存、登录态、常量等工具类
```

## 本地运行

### 1. 准备环境

请先启动以下服务：

- MySQL
- Redis
- Kafka

### 2. 初始化数据库

导入 SQL 文件：

```text
src/main/resources/db/yqyx.sql
```

建议创建数据库 `yunqi_youxiang`。如果你使用其他数据库名，请同步修改 `application.yaml` 中的 datasource 配置。

### 3. 修改配置

根据本地环境修改：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/yunqi_youxiang?useSSL=false&serverTimezone=UTC
    username: root
    password: 123
  redis:
    host: 192.168.150.101
    port: 6379
    password: 123321
  kafka:
    bootstrap-servers: 127.0.0.1:9092
```

### 4. 编译运行

```bash
mvn clean package -DskipTests
mvn spring-boot:run
```

默认端口：

```text
http://localhost:8081
```

## 常用接口

| 功能 | 方法 | 路径 |
| --- | --- | --- |
| 发送验证码 | POST | `/user/code` |
| 用户登录 | POST | `/user/login` |
| 查询店铺详情 | GET | `/shop/{id}` |
| 查询店铺分类 | GET | `/shop-type/list` |
| 查询店铺优惠券 | GET | `/voucher/list/{shopId}` |
| 秒杀优惠券 | POST | `/voucher-order/seckill/{id}` |
| 查询我的订单 | GET | `/voucher-order/of/me` |
| 支付成功回调模拟 | POST | `/voucher-order/pay/success/{id}` |
| 发布探店笔记 | POST | `/blog` |
| 查询热门笔记 | GET | `/blog/hot` |
| 每日签到 | POST | `/user/sign` |

## 项目说明

本项目适合作为 Java 后端高并发业务实践项目，重点关注秒杀、缓存、限流、消息队列、订单状态流转和数据一致性等后端核心能力。项目中的部分配置需要根据本地 MySQL、Redis、Kafka 环境进行调整后再运行。
