<!-- 本文件翻译自 README_silver.md。更新韩文原文时，请同步更新本文件。 -->

# 概述

- Bronze（保存原始数据，重点是收集单位）
  - log generator => kinesis => firehose(1Mib/60s) => s3 bronze（原始数据，GZIP）
- Silver（原始数据 -> 清洗/预处理等数据加工 -> 保存）
  - Streamming Ingestion => `Streamming Processing + Medalion Architecture`
  - step 1
    - log generator => kinesis(input) => flink（Java、Python（PyFlink）、pom.xml、Maven）=> kinesis(outut) => firehose => s3 silver
      - 创建 `flink、kinesis(outut)、firehose 这 3 个资源`
      - s3 silver：配置在现有存储桶下
        - 保存 jsonl -> 保存为 GZIP
        - 保存 parquet -> 配置 Glue Schema 后进行联动
  - step 2
    - log generator => kinesis(input) => lambda => kinesis(outut) => firehose => s3 silver

# Flink

- 从实时流数据中获取可执行的分析信息
  - 可以实时进行数据预处理、清洗等工作，几乎没有延迟
- 如果不要求实时处理，则使用 Airflow 或 Step Function（AWS）进行批处理
- 保存到 Silver 阶段的方法
  - [v] 实时 -> Flink、Lambda
  - 批处理 -> Airflow 或 Step Function（AWS）

- 特点
  - 实时流处理：持续读取并立即处理来自 Kinesis 的数据
  - Stateful Processing：记住之前事件的状态，可以进行聚合和判断
  - 支持 Event Time：可以按照数据实际产生的时间进行处理
  - Window 处理：适合以 1 分钟、5 分钟、1 小时为单位进行聚合
  - Checkpoint / 故障恢复：保存处理状态，发生故障后可以继续处理
  - 支持 Exactly-once：能够进行高可靠的流处理，最大限度减少重复或遗漏
  - `适合处理大规模数据`：对于持续高速的事件处理，比 Lambda 更有优势
  - 支持复杂处理：适用于过滤、转换、聚合、Join 和异常检测等场景

# 数据存储

```text
S3 Bucket
├── bronze/
│   └── year=.../month=.../day=.../hour=...
├── silver/
│   └── year=.../month=.../day=.../hour=...
├── flink/
│   └── applications/
│       └── flink-silver-xxxxx.zip <- 执行 Flink 处理的应用（*.py、pom.xml、*.jar）
└── errors/
    ├── bronze/
    │   └── ...
    └── silver/
        └── ...
```

# 架构

- 保留 Bronze，并进行部分修改
- 添加 Silver 层：Kinesis（Bronze）是生产者，Flink 是消费者

```text
日志生成器
    ↓
Kinesis Raw（向两个方向发送）
    ├────────────→ Firehose → S3 Bronze
    │
    └→ Flink
         ↓
     验证 / 清洗 / 转换
         ↓
   Kinesis Silver
         ↓
      Firehose
         ↓
      S3 Silver
```

# 基础设施修改

| 类型 | 文件 | 变更内容 |
| --- | --- | --- |
| **修改** | `locals.tf` | 添加 Silver Kinesis/Flink/Firehose 名称 |
| **修改** | `variables.tf` | 添加 Flink Runtime、Parallelism、Silver Shard 等变量 |
| **修改** | `firehose.tf` | 将 Bronze 错误路径整理为 `errors/bronze/` |
| **新增** | `iam-flink.tf` | Flink + Silver Firehose IAM |
| **新增** | `silver.tf` | 创建 Silver Kinesis + Silver Firehose |
| **新增** | `flink.tf` | Managed Flink + 将代码上传至 `flink/` |
| **新增** | `flink-logs.tf` | Flink CloudWatch 日志 |
| **修改** | `outputs.tf` | 添加 Silver/Flink 相关 Output |

# Flink 应用结构

- 结构

```text
flink/
├── app/
│   ├── main.py         : Flink 应用入口（执行部分），读取 Bronze Kinesis，
│   │                     加载 transform 模块执行 clean，并向 Silver Kinesis 发送处理后的数据
│   └── transform.py    : 执行数据清洗、预处理等数据处理工作
├── target/             : 构建后生成，是 Maven 的构建产物
│   └── *.zip           : 构建后生成的 Flink 应用
├── assembly/
│   └── assembly.xml    : 定义 Flink 应用（ZIP 文件）的组成（*.py、pyflink-dependencies.jar）
│
├── application_properties.json : 本地运行时的 input/output Kinesis 配置
├── pom.xml             : 下载依赖文件、生成 JAR、执行 ZIP 打包
├── README.md
└── .gitignore
```

- 构建前安装（Java、Maven）

```text
# Windows
winget search Microsoft.OpenJDK
winget install Microsoft.OpenJDK.11
  java -version

choco install maven or scoop install main/maven or 手动安装
访问 https://maven.apache.org/download.cgi?utm_source=chatgpt.com > 下载 apache-maven-3.9.16-bin.zip
将 bin 文件夹加入 PATH
  mvn -version

# macOS
brew install openjdk@11 maven
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH="$JAVA_HOME/bin:$PATH"
java -version
mvn -version
```

- 构建

```text
./scripts/build-flink.bat
sh ./scripts/build-flink.sh
```

# 设置

```text
# 配置基础设施
terraform -chdir=infra fmt
terraform -chdir=infra validate
terraform -chdir=infra plan

./scripts/setup.bat

# 进入 Flink 服务 -> 检查应用状态 -> 确认进入运行状态（绿灯）

# 发送日志 -> 大约 1 分钟后检查 s3/存储桶/silver 是否生成 -> 下级目录中存在日志 => 完成！
./scripts/run-generator.bat ecommerce 5 5 0.20 1 ap-northeast-2 1

============================================================
Fargate synthetic log generator
============================================================
Run ID          : loggen-10094606-7593
Domain          : ecommerce
Duration        : 5s
Base RPS        : 5
Time scale      : 1
Corruption rate : 0.20
Tasks           : 1
Region          : ap-northeast-2
Cluster         : de-ai-25-loggen-cluster

-----------------------------------------------------------------------------------------------------------
|                                                 RunTask                                                 |
+---------------------------------------------------------------------------------------------------------+
|  arn:aws:ecs:ap-northeast-2:827913617635:task/de-ai-25-loggen-cluster/f670ea4a0575429b861e182864f335c2  |
+---------------------------------------------------------------------------------------------------------+

Task started.
Follow generated logs:
  aws logs tail "/ecs/de-ai-25-loggen" --follow --region "ap-northeast-2"
```

# 故障排查

```text
# 查看已登记的资源
terraform state list

# AlreadyExists => 之前创建的资源仍然存在
# 如果资源登记中存在遗漏，则手动关联
# terraform import 资源名称 资源值
terraform import aws_ecr_repository.generator de-ai-25-loggen-repo
```

# 检查事项

- 从实时流数据处理的角度
  - 添加 Silver 层的部分
    - 检查 Flink 应用代码
      - 后续需要时升级代码（添加功能）
    - 删除污染数据（当前方式）=> 单独保管（保存）=> 保存下来，以便后续分析原因（扩展）
    - 保存数据：jsonl（gzip）=> 保存为 parquet（使用 Glue）
    - 使用 Flink 替代 Lambda（理解二者差异）
  - 从 Silver 添加 Gold
    - 可以使用 Flink 或 Lambda => 加工成最终目标格式
    - 中间过程相同
    - 最终保存到 S3（parquet）/ OpenSearch
  - Dashboard（连接 Prometheus、Grafana 等）
    - S3 => Athena 查询，OpenSearch 搜索查询 => 能够响应 Dashboard 上的实时监控数据查询

- 日志生成器 => Kinesis => Firehose => 保存至 S3（Bronze）
  - 批处理（基于 Airflow）+ 应用 Medallion Architecture
    - Airflow 按特定周期运行 => Bronze -> Silver -> Gold -> 生成最终产物（Dashboard、报告）

- Kafka（后续检查）
- ELK、EFK
- Step Function、EventBridge

# 检查 Flink 应用代码

- 保留 80% 的代码
  - 采用只添加所需功能的结构
  - 只需自定义函数中的核心部分（清洗、预处理）即可使用相关功能
  - 资源发生变化时调整配置值

# 删除污染数据（当前方式）=> 单独保管（保存）

## 概述

- 保存污染数据，以便后续分析原因（扩展）
- 架构

```text
Bronze Kinesis -> 通过批处理只提取污染数据并定期监控的方法
      ↓
    Flink
      │
      ├─ 正常 → Silver Kinesis → Firehose → S3 silver/
      │
      └─ 异常 → Rejected Kinesis
                       ↓
                    Firehose
                       ↓
                  S3 rejected/ => 后续通过批处理 => 解决根本问题
```

- 正常数据和异常数据在 Flink 中进行分流
  - 修改 SQL，把处理结果为 None 的数据发送至 Rejected Kinesis
- Rejected Kinesis -> Firehose -> S3 rejected/：基础设施配置

## 基础设施配置

- 添加和修改
  - 配置基础设施 -> 应用
    - 对象：新增 Kinesis、Kinesis -> Firehose、IAM、Flink、locals、variables、outputs
  - 修改 Flink -> 测试 -> 将污染数据比例提高到 30% -> 生成日志 -> 确认污染数据已经保存
- 执行步骤
  - 构建 Flink 应用 + 更新基础设施并部署应用

    ```text
    scripts\setup-flink.bat

    sh ./scripts/setup-flink.sh

    ----
    scripts\flink-status.bat
    ----------------------------------------------------------
    |                   DescribeApplication                  |
    +-------------------------------+--------------+---------+
    |             Name              |   Runtime    | Status  |
    +-------------------------------+--------------+---------+
    |  de-ai-25-loggen-silver-flink |  FLINK-1_20  |  READY  |
    +-------------------------------+--------------+---------+

    -----
    scripts\flink-start.bat

    scripts\flink-status.bat
    # READY -> STARTING -> RUNNING

    -----
    # 确认状态为 RUNNING 后 -> 生成日志，将污染比例提高到 30%
    scripts\run-generator.bat ecommerce 30 10 0.30 1 ap-northeast-2 1

    -----
    # 在 S3 中检查 reject 文件夹下的数据

    -----
    # 停止 Flink
    scripts\flink-stop.bat
    -----
    # 停止后确认状态
    scripts\flink-status.bat
    ----------------------------------------------------------
    |                   DescribeApplication                  |
    +-------------------------------+--------------+---------+
    |             Name              |   Runtime    | Status  |
    +-------------------------------+--------------+---------+
    ```

  - 启动日志生成器 -> 将污染比例提高到 30%，确保能够正常收集

# 保存为 parquet => 需要 Schema => 基于列查询 => 由 Glue 提供（提供数据库、表、Crawler、Job 等多种功能）

## Glue

- 与数据相关的集成服务
- 定义数据结构，并以此为基础执行查询；提供把数据提取成适合 Gold 层的格式（聚合、统计等分析）的基础能力
- 提供数据库、表、Schema、Crawler（查看数据并自动构建 Schema）、ETL Job 可视化配置等功能

## parquet

- 为了`高效保存大规模大数据`并进行`快速查询`而设计的`基于列（Column）的开源文件格式`
- 相同数据量占用的空间更小、搜索速度更快，并且几乎受到所有相关工具支持

## 基础设施修改/添加

- 修改 `silver.tf` 中 Firehose 保存部分
  - 通过 Silver Layer 保存的数据：当前为 GZIP 文件，改为 parquet 格式
  - `compression_format = "GZIP"` => `UNCOMPRESSED`
  - 使用 SNAPPY 负责压缩
- `glue-silver.tf`
  - 定义保存到 `s3 silver/....` 的 parquet 数据的 `Schema`
  - 将 JSON 转换为 parquet 时引用该 Schema
  - Athena 可以查询原始数据、parquet 等压缩数据；对相应文件执行 SQL 时，由 Glue 提供 Table/Schema
  - S3 按分区格式（year/month/day/hour）保存后，可以利用这些分区执行 SQL
- `iam-flink.tf`
  - 向现有 Role 添加 Glue 权限
- `outputs.tf`
  - 输出 Glue 相关名称和资源等信息

### silver.tf（修改）
