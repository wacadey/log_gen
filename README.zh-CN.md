# 项目目标

- 使用 AWS Fargate 创建日志生成器。
  - AWS Fargate：
    - 不需要自行创建或管理 EC2。
    - 使用注册在 ECR 的镜像，直接通过 ECS 执行容器，以「无服务器」方式获取执行环境。
    - 适合用较低的管理成本执行一次性工作。
  - 生成的数据：
    - 数据形式：将「请求 → 服务处理 → 响应」整合成最终应用程序日志；本项目不创建实际服务。
    - 可输出至：
      - S3：于后续章节扩展使用。
      - CloudWatch：存储 AWS 日志。
      - 本地文件。
      - 终端（console）。

# 基础设施组成

- `variables.tf`：设置区域、项目名称、CPU、内存等可调整参数。
- `version.tf`：指定 Terraform 与 Provider 版本需求。
- `provider.tf`：设置 AWS Provider、区域与共用标签。
- `locals.tf`：整理共用值、标签及整个项目的名称前缀。
- `vpc.tf`：创建专用网络，包括 VPC、Subnet、Internet Gateway、Route Table 与 Availability Zone 配置。
- `sg.tf`：创建 Security Group；没有设置 ingress，因此不接受外部主动连接。
- `iam.tf`：授予 Fargate 从 ECR 获取镜像及写入 CloudWatch Logs 的权限。
- `logs.tf`：指定 ECS 容器内日志生成器所使用的 CloudWatch Log Group。
- `ecr.tf`：创建存放日志生成器 Docker 镜像的 ECR Repository。
- `ecs.tf`：创建 ECS Cluster 与 Fargate Task Definition，提供一次性任务的执行环境，并非长期运行的服务。
- `outputs.tf`：输出创建后的各项 AWS 资源信息。

# 创建基础设施

在项目的 `infra` 目录执行：

```powershell
cd infra
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
```

各命令用途：

- `terraform init`：初始化项目并下载 AWS Provider。
- `terraform fmt`：整理 Terraform 代码格式。
- `terraform validate`：检查设置语法及结构。
- `terraform plan`：预览即将创建、修改或删除的资源。
- `terraform apply`：实际应用变更到 AWS。

# 日志生成器（`generator`）

## 共用日志格式

以下是服务处理完请求后，包含最终字段的日志示例：

```json
{
  "schema_version": "1.0",
  "record_type": "application_log",
  "event_id": "...",
  "trace_id": "...",
  "run_id": "...",
  "occurred_at": "2026-08-12T16:30:11.123+09:00",
  "generated_at_utc": "2026-08-12T07:30:11.123+00:00",
  "domain": "ecommerce",
  "event_type": "order_created",
  "service": {},
  "client": {},
  "request": {
    "method": "POST",
    "path": "/api/orders",
    "request_bytes": 1234
  },
  "response": {
    "status_code": 201,
    "latency_ms": 287,
    "response_bytes": 3590
  },
  "data": {}
}
```

`data` 会依不同领域使用不同的自订结构。

## 支援的领域与事件

- 通过 `DOMAIN` 选择数据类别。
- 每个领域会分别设置事件比例、不同时段的生成量、主要字段和延迟时间。
- 设计时会尽量反映真实领域的特性。

| DOMAIN | 主要事件示例 |
|---|---|
| `ecommerce` | `product_view`、`search`、`add_to_cart`、`checkout`、`order_created`、`payment_completed` |
| `finance` | `account_login`、`balance_inquiry`、`card_payment`、`transfer`、`deposit`、`withdrawal` |
| `smartfactory` | `sensor_reading`、`equipment_state`、`quality_inspection`、`alarm`、`maintenance_event` |
| `game` | `login`、`session_heartbeat`、`match_started`、`match_finished`、`item_purchase`、`quest_completed` |

# 模拟接近真实情况的生成间隔

会考虑以下条件：

- 不同时段的权重。
- 平日与周末的权重。
- 偶发流量提升（traffic booster）。
- 其他领域特有的流量变化。

例如：

- 电商：午餐和晚餐时段增加，周末减少。
- 金融：平日白天增加，周末减少。
- 智慧工厂：全天相对稳定，但可能因轮班或周末而改变。
- 游戏：晚上和深夜为高峰，周末增加。
- 可利用权重将尖峰流量放大为 5 倍或 10 倍。

# 脏数据

可以指定脏数据比例。例如 `0.03` 表示每个事件约有 3% 机率生成脏数据，供 ETL 阶段练习数据清理。

脏数据类型包括：

```text
missing_field
null_required
wrong_type
invalid_timestamp
numeric_outlier
invalid_enum
negative_latency
duplicate
malformed_json
...
```

# 日志生成器在数据管线中的位置

```text
              Data Source Simulator
              Fargate + Python/Faker
                       │
       ┌───────────────┼───────────────┐
       │               │               │
       ▼               ▼               ▼
      S3            Kinesis           Kafka
  File/Batch       AWS Stream      Event Stream
       │               │               │
       └───────────┬───┴───────────────┘
                   ▼
              Bronze Layer
                   │
           ETL / ELT Processing
       Pandas / Polars / Spark
                   │
                 Silver
                   │
                  Gold
                   │
       Athena / OpenSearch / BI
```

这个项目负责模拟数据来源。生成的数据之后可以进入 S3、Kinesis 或 Kafka，再经 ETL／ELT 处理成 Bronze、Silver、Gold 等数据层。

# 生成器结构与安装

## 目录结构

```text
generator/
├─ app/
│  ├─ domains/
│  │  └─ *.py          # 四个领域的主要日志生成函式
│  └─ *.py             # main.py 入口及其他功能
├─ Dockerfile          # 创建容器镜像，之后推送到 ECR
└─ requirements.txt    # Python 依赖依赖包
```

## 安装 Python 依赖包

在项目根目录执行：

```powershell
pip install -r generator/requirements.txt
```

# 操作脚本

## `run-local.bat`

在本机生成日志，适合部署前测试。

语法：

```powershell
scripts\run-local.bat [DOMAIN] [DURATION] [RPS] [CORRUPTION] [OUTPUT] [TIME_SCALE]
```

示例：

```powershell
scripts\run-local.bat finance 60 5 0.05 both 1
```

这个示例表示：

- 生成 `finance` 领域的日志。
- 执行 60 秒。
- 平均每秒生成 5 笔事件（5 RPS）。
- 生成约 5% 脏数据。
- 同时输出到终端及文件（`both`）。
- 文件存储在 `output` 目录。
- 时间倍数为 1。

`OUTPUT` 可使用 `stdout`、`file` 或 `both`。

## `setup.bat`

执行前需要先启动本机 Docker Desktop。

处理流程：

```text
Terraform init
      ↓
Terraform apply
      ↓
创建 ECR Repository、ECS 及 Fargate 基础设施
      ↓
登录 AWS ECR
      ↓
建置 Docker Image
      ↓
将 latest Image 推送到 ECR
```

执行方式：

```powershell
$env:AWS_PROFILE = "de-ai-12"
scripts\setup.bat ap-northeast-2
```

## `run-generator.bat`

这个脚本会在 AWS Fargate 执行日志生成器。

完整语法：

```powershell
scripts\run-generator.bat [DOMAIN] [DURATION] [BASE_RPS] [CORRUPTION_RATE] [TASK_COUNT] [REGION] [TIME_SCALE]
```

处理流程：

```text
在本机执行命令
      ↓
调用 aws ecs run-task
      ↓
在 ECS Cluster 启动 Fargate Task
      ↓
启动 Docker Container
      ↓
执行 Python 日志生成器
      ↓
将日志写入 CloudWatch Logs
```

示例：

```powershell
$env:AWS_PROFILE = "de-ai-12"
scripts\run-generator.bat ecommerce 2 5 0.05 1 ap-northeast-2 1
```

参数代表：

- `ecommerce`：数据领域。
- `2`：执行 2 秒。
- `5`：基础速度为每秒 5 笔事件。
- `0.05`：脏数据比例为 5%。
- `1`：启动一个 Fargate Task。
- `ap-northeast-2`：AWS 首尔区域。
- `1`：时间倍数。

成功启动后会显示类似以下信息：

```text
============================================================
Fargate synthetic log generator
============================================================
Domain          : ecommerce
Duration        : 2s
Base RPS        : 5
Time scale      : 1
Corruption rate : 0.05
Tasks           : 1
Region          : ap-northeast-2
Cluster         : de-ai-12-loggen-cluster
```

## 查看 CloudWatch 日志

启动 Task 后，可以使用以下命令持续查看日志：

```powershell
aws logs tail "/ecs/de-ai-12-loggen" --follow --region "ap-northeast-2" --profile de-ai-12
```

按 `Ctrl+C` 可以停止追踪；这只会停止本机显示，不会删除 CloudWatch 中已保存的日志。

# 建议的学习顺序

1. 使用 `run-local.bat` 在本机生成日志。
2. 阅读 `generator/app`，了解事件与脏数据如何生成。
3. 阅读 `infra` 中的 Terraform 文件。
4. 使用 `terraform plan` 理解 AWS 资源关系。
5. 使用 `setup.bat` 创建基础设施并推送镜像。
6. 使用 `run-generator.bat` 启动 Fargate Task。
7. 使用 AWS CLI 或 AWS Console 查看 CloudWatch Logs。

# 注意事项

- Terraform 与 AWS CLI 操作前，请确认使用的是正确 profile：`de-ai-12`。
- `terraform.tfstate` 包含资源对应信息，应保留在本机并避免提交到 GitHub。
- Fargate、CloudWatch Logs、ECR 和其他 AWS 资源可能生成费用。
- 不再使用资源时，可在确认 Terraform state 正确后执行 `terraform destroy`；此命令会删除由 Terraform 管理的 AWS 资源，执行前务必先查看计划。
