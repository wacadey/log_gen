# Silver kinesis -> lambda -> Gold Kinesis -> Firehose -> S3 Gold
# 필요한 모든 리소스, 권한등 하나의 tf에서 구성
# variables
variable "gold_kinesis_shard_count" {
  description = "KDS's shard count"
  type        = number
  default     = 1
}
# kinesis 에서 미전송된 데이터 보관기간
variable "gold_kinesis_retention_hour" {
  description = "KDS's retention period in hours"
  type        = number
  default     = 24
}

# locals
locals {
    # Gold Kinesis 리소스 이름
    gold_kinesis_stream_name = "${var.project_name}-gold-kinesis"
    # Gold Firehose 리소스 이름
    gold_firehose_name  = "${var.project_name}-gold-firehose"
    # Silver 데이터 -> Gold용을 구성(집계등) 처리하는 lambda 함수명
    gold_lambda_name    = "${var.project_name}-silver-to-gold"
    # 람다 함수 배포용 zip 경로 (테라폼 업로드)
    gold_lambda_zip     = "${path.module}/../lambda/gold/gold-lambda.zip"
}

# kinesis
resource "aws_kinesis_stream" "gold" {
  name             = local.gold_kinesis_stream_name
  shard_count      = var.gold_kinesis_shard_count
  retention_period = var.gold_kinesis_retention_hour

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    DataLayer = "gold"
    Purpose = "lambda-output"
  }
}

# IAM-ROLE
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-gold-lambda-role"
  # 위에서 만든 신뢰정책 반영하여 role 구성
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}
data "aws_iam_policy_document" "lambda" {
  # 1. CloudWatch Logs에 기록
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["arn:aws:logs:${var.aws_region}:*:*"]
  }
  # 2. kinesis 기본 필수 권한 (silver kinesis에서 데이터를 읽어오는 권한), 입력 권한
  statement {
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:ListStreams"
    ]

    resources = [aws_kinesis_stream.silver.arn]
  }
  # 3. kinesis 데이터 기록(집계 결과 전송), 출력 권한
  statement {
    effect = "Allow"

    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords"
    ]

    resources = [aws_kinesis_stream.gold.arn]
  }
}
# 기존 role에 새로운 정책(여러 권한 조합)을 부여
resource "aws_iam_role_policy" "lambda" {
  name   = "${var.project_name}-gold-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}



# Lambda
resource "aws_lambda_function" "silver_to_gold" {
  # 함수이름
  function_name = local.gold_lambda_name
  # 역활
  role = aws_iam_role.lambda.arn
  # 함수의 엔트리 포인트 =>  어떤 모듈의 어떤 함수를 호출하여 처리하는가
  handler = "main.lambda_handler"
  # 파이썬 작동 -> 서버리스 -> 환경 구성되어 있어야함 (PaaS) 형태로 -> python  버전 지정
  runtime = "python3.12"
  # 가동시 가용 메모리
  memory_size = 256 # MB
  # 처리 시간 timeout 설정
  timeout = 30
  # 배포한 zip 경로
  filename = local.gold_lambda_zip
  # 업데이트 감지 -> 해시값이 바뀌면 업데이트 된것으로(소스) 간주 => 새로 배포
  source_code_hash = filebase64sha256(local.gold_lambda_zip)
  # 환경변수 -> lambda 함수 작동시 외부에서 전달하는 값
  environment {
    variables = {
      GOLD_STREAM_NAME = aws_kinesis_stream.gold.name
      AWS_REGION_NAME  = var.aws_region
    }
  }
  # 의존성
  depends_on = [ aws_iam_role_policy.lambda ]
  # 태그
  tags = {
    DataLayer = "gold"
    Processor = "lambda"
  }
}

# Silver kinesis -> Lambda 입력원 연결
resource "aws_lambda_event_source_mapping" "silver_to_gold" {
  # 읽는 대상
  event_source_arn = aws_kinesis_stream.silver.arn
  # 실행할 함수 arn
  function_name = aws_lambda_function.silver_to_gold.arn
  # 어떤(순서) 데이터부터 읽어서 처리하는가 (LATEST, TRIM_H..)
  starting_position = "LATEST"
  # 한번에 lambda에서 호출하는 최대 레코드 개수
  batch_size = 100 # 100개 읽어서 처리 (실시간보다는 배치 작업성 높음 구성)
  # 트레픽 상승 대비하여 대기시간 부여 -> 5초 부여(가정) -> 테스트 후 조정
  maximum_batching_window_in_seconds = 5
  # 테라폼 구성 이후 활성화
  enabled = true
  # 실패한 레코드에 대해, 성공 레코드도 섞여 있을 경우, 다시 처리 할것인가? -> 다시 처리 않함
  function_response_types = [ "ReportBatchItemFailure" ]
  # 의존성
  depends_on = [ aws_iam_role_policy.lambda ]
}

# Firehose IAM Role
data "aws_iam_policy_document" "firehose_gold_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "firehose_gold" {
  name = "${var.project_name}-gold-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_gold_assume.json
}
data "aws_iam_policy_document" "firehose_gold" {
  # kinesis 읽기 권한 관련  
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards"
    ]
    resources = [
      aws_kinesis_stream.gold.arn
    ]
  }
  # s3 저장 권한 관련
  statement {
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.data.arn,       # 해당 버킷
      "${aws_s3_bucket.data.arn}/*" # 해당 버킷 이하 모든 경로
    ]
  }
  # Firehose JSON -> Parquet 변환 처리시 Glue Schema 읽어서 처리
  statement {
    effect = "Allow"
    actions = [
      "glue:GetTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions"
    ]
    resources = ["*"]
  }
}
resource "aws_iam_role_policy" "firehose_gold" {
  name   = "${var.project_name}-gold-firehose-policy"
  role   = aws_iam_role.firehose_gold.id
  policy = data.aws_iam_policy_document.firehose_gold.json
}


# Gold Firehose
resource "aws_kinesis_firehose_delivery_stream" "logs" {
  # 이름
  name        = local.firehose_name
  destination = "extended_s3"

  # 입력소스 (키네시스, 역활 설정)
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.logs.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  # 출력대상
  extended_s3_configuration {
    # 버킷
    bucket_arn = aws_s3_bucket.data.arn
    # 역활
    role_arn = aws_iam_role.firehose.arn

    # 버퍼 관련 용량, 시간 설정
    buffering_size     = var.firehose_buffer_size     # 1Mib
    buffering_interval = var.firehose_buffer_interval # 60초

    # 데이터를 모아둔상태(버퍼링)에서 기록 -> 포멧
    # 데이터 레코드 압축
    # compression_format = "UNCOMPRESSED" # 1차는 원본 지정, 활성화되지 않음
    compression_format = "GZIP" # GZIP으로 압축

    # S3 버킷 및 S3 오류 출력 접두사 시간대
    custom_time_zone = "Asia/Seoul"

    # 아래 처럼 구성 => partition pruning => Athena/opensearch/Glue/spark등 열기반으로 데이터 추출 유용
    # S3 버킷 접두사
    # bronze/year=2026/month=08/day=20/hour=11/.. 이렇게 파티션 가능 -> 검색 속도 빨라짐
    prefix = "bronze/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"

    # S3 버킷 오류 출력 접두사
    # 현재는 에러를 단독 구성, 브론즈/실버/골드등 계층 구분 하지 x => 필요시 구성 가능
    # 경로상에 에러애 대한 타입 지정 -> 유형별로 에러가 모이게 작성
    # [실버 수정]
    error_output_prefix = "errors/bronze/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
  }

  # 의존성
  depends_on = [
    # 해당 정책 입력/출력 엑세스 권한 생성된 후에 firehose 생성되도록 설정
    aws_iam_role_policy.firehose
  ]
}











# 편의상 실시간성을 고려하여 구성
# silver -> lambda -> gold kinesis -> firehose 
#        -> Glue Schema -> Parquet(SNAPPY) -> s3 gold/

# 파이썬 파일 -> ZIP 패키징 관련
# lambda python 소스코드를 AWS lambda에 배포할 ZIP 파일에 포함하여 자동 생성
# data "archive_file" "gold_lambda" {
#     type        = "zip"
#     source_file = "${path.module}/../lambda/lambda_function.py"
#     output_path = "${path.module}/../lambda/gold-lambda.zip"
# }