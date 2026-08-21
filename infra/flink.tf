# Managed Service for apache flink

locals {
  # flink 앱 경로. flink-silver.zip 파일은 스크립트에서 정의, flink 최종 산출물의 경로
  # ${path.module} => ~/infra (현재 디렉토리 위치)
  flink_artifact_path = "${path.module}/../flink/target/flink-silver.zip"
  
  # zip 내용에 MD5 해시를 계산하여 코드 변경 여부 식별 용도
  flink_artifact_hash = filemd5(local.flink_artifact_path)
}

# flink_app이란 flink-silver.zip이고, aws s3에 위치해야 함
resource "aws_s3_object" "flink_app" {
  bucket = aws_s3_bucket.data.id
  key    = "flink/applications/flink-silver-${local.flink_artifact_hash}.zip"

  source      = local.flink_artifact_path
  source_hash = local.flink_artifact_hash

  depends_on = [
    aws_s3_bucket_public_access_block.data
  ]
}

# flink 자체 내용
resource "aws_kinesisanalyticsv2_application" "silver" {
  name                   = local.flink_application_name
  description            = "Raw Kinesis events to Silver Kinesis using PyFlink"
  runtime_environment    = var.flink_runtime_environment
  service_execution_role = aws_iam_role.flink.arn
  start_application      = var.flink_start_application

  application_configuration {
    application_snapshot_configuration {
      snapshots_enabled = false
    }

    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.data.arn
          file_key   = aws_s3_object.flink_app.key
        }
      }

      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "InputStream"

        property_map = {
          "stream.arn"                 = aws_kinesis_stream.bronze.arn
          "aws.region"                 = var.aws_region
          "flink.source.init.position" = var.flink_source_init_position
        }
      }

      property_group {
        property_group_id = "OutputStream"

        property_map = {
          "stream.arn" = aws_kinesis_stream.silver.arn
          "aws.region" = var.aws_region
        }
      }

      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"

        property_map = {
          "python"  = "main.py"
          "jarfile" = "lib/pyflink-dependencies.jar"
          "pyFiles" = "transform.py"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = true
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = var.flink_parallelism_per_kpu
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  depends_on = [
    aws_iam_role_policy.flink,
    aws_s3_object.flink_app
  ]

  tags = {
    DataLayer = "silver"
    Processor = "flink"
  }
}