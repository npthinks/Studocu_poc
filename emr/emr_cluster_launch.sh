#!/bin/bash
# ----------------------------------------------------------------------
# Studocu Historical Backfill — EMR Cluster Launch
# ----------------------------------------------------------------------
# One-time launch of an EMR cluster for the 12.4 TB historical backfill.
# Auto-terminates on step completion. Uses spot instances for cost.
# ----------------------------------------------------------------------

REGION="eu-west-1"
LOG_URI="s3://studocu-data-lake/emr-logs/"
SCRIPT_URI="s3://studocu-data-lake/scripts/emr_historical_backfill.py"

aws emr create-cluster \
  --region "${REGION}" \
  --name "studocu-historical-backfill" \
  --release-label "emr-7.0.0" \
  --applications Name=Spark \
  --log-uri "${LOG_URI}" \
  --service-role "EMR_DefaultRole" \
  --ec2-attributes "InstanceProfile=EMR_EC2_DefaultRole,KeyName=studocu-ops" \
  --instance-fleets \
    '[
      {
        "Name": "primary",
        "InstanceFleetType": "MASTER",
        "TargetOnDemandCapacity": 1,
        "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}]
      },
      {
        "Name": "workers",
        "InstanceFleetType": "CORE",
        "TargetOnDemandCapacity": 6,
        "TargetSpotCapacity": 14,
        "InstanceTypeConfigs": [
          {"InstanceType": "r5.4xlarge", "BidPriceAsPercentageOfOnDemandPrice": 100},
          {"InstanceType": "r5.2xlarge", "BidPriceAsPercentageOfOnDemandPrice": 100}
        ]
      }
    ]' \
  --managed-scaling-policy '{
    "ComputeLimits": {
      "UnitType": "Instances",
      "MinimumCapacityUnits": 5,
      "MaximumCapacityUnits": 50,
      "MaximumOnDemandCapacityUnits": 10,
      "MaximumCoreCapacityUnits": 50
    }
  }' \
  --auto-terminate \
  --steps "[
    {
      \"Name\": \"Historical Backfill\",
      \"ActionOnFailure\": \"TERMINATE_CLUSTER\",
      \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [
          \"spark-submit\",
          \"--conf\", \"spark.dynamicAllocation.enabled=true\",
          \"--conf\", \"spark.sql.shuffle.partitions=400\",
          \"${SCRIPT_URI}\",
          \"--source_path\", \"s3://studocu-data-lake/bronze/\",
          \"--silver_path\", \"s3://studocu-data-lake/silver/\",
          \"--rejected_path\", \"s3://studocu-data-lake/rejected/\",
          \"--start_date\", \"2023-01-01\",
          \"--end_date\", \"2026-05-31\"
        ]
      }
    }
  ]"