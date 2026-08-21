# 실버 레이어 에서 사용되는 kinesis
# flink에서 전송된 데이터를 획득 -> firehose로 전송
resource "aws_kinesis_stream" "silver" {
  name             = local.silver_kinesis_stream_name
  shard_count      = var.silver_kinesis_shard_count
  retention_period = var.silver_kinesis_retention_hour

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    DataLayer = "silver"
  }
}