# AWS Glue PySpark Job Configuration

## Job Creation via AWS CLI

### 1. Create the Glue Job

```bash
aws glue create-job \
    --name "anime-data-processor-pyspark" \
    --role "arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://anime-mvp-data/scripts/glue_flatten.py",
        "PythonVersion": "3.9"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--enable-metrics": "",
        "--enable-spark-ui": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--S3_BUCKET": "anime-mvp-data",
        "--AWS_REGION": "us-east-2",
        "--input_bucket": "anime-mvp-data",
        "--output_bucket": "anime-mvp-data",
        "--raw_prefix": "raw",
        "--processed_prefix": "processed",
        "--write_mode": "overwrite"
    }' \
    --max-retries 1 \
    --timeout 60 \
    --glue-version "4.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --description "Process raw anime JSON data using PySpark for scalable ETL" \
    --tags '{
        "Project": "anime-mvp",
        "Environment": "production",
        "JobType": "PySpark"
    }'
```

### 2. Upload Script to S3

```bash
# Upload the PySpark script
aws s3 cp src/preprocessing/glue_flatten.py s3://anime-mvp-data/scripts/
```

### 3. Start Job Run

```bash
aws glue start-job-run --job-name "anime-data-processor-pyspark"
```

## Job Creation via AWS Console

### Step 1: IAM Role Setup
1. Go to IAM Console → Roles → Create Role
2. Select "Glue" as the service
3. Attach policies:
   - `AWSGlueServiceRole`
   - `AmazonS3FullAccess` (or custom S3 policy)
4. Name: `GlueServiceRole`

### Step 2: Create Glue Job
1. Go to AWS Glue Console → Jobs → Create Job
2. Job details:
   - **Name**: `anime-data-processor-pyspark`
   - **IAM Role**: `GlueServiceRole`
   - **Type**: `Spark`
   - **Glue Version**: `4.0`
   - **Python version**: `3.9`
   - **Worker type**: `G.1X` (1 DPU per worker)
   - **Number of workers**: `2` (minimum for PySpark)
   - **Timeout**: `60 minutes`
   - **Max retries**: `1`

3. Script settings:
   - **Script path**: `s3://anime-mvp-data/scripts/glue_flatten.py`
   - **Temporary directory**: `s3://anime-mvp-data/temp/`

4. Job parameters:
   - `--input_bucket`: `anime-mvp-data`
   - `--output_bucket`: `anime-mvp-data`
   - `--raw_prefix`: `raw`
   - `--processed_prefix`: `processed`
   - `--write_mode`: `overwrite`
   - `--enable-metrics`: (empty value)
   - `--enable-spark-ui`: `true`
   - `--enable-continuous-cloudwatch-log`: `true`

5. Advanced properties:
   - **Job bookmark**: `Disable` (for reprocessing all data)
   - **Security configuration**: (optional for encryption)
   - **Spark UI**: `Enable` for debugging

### Step 3: Spark Configuration
The job automatically configures optimal Spark settings:
- Adaptive Query Execution (AQE) enabled
- Dynamic partition coalescing
- Skew join optimization
- Kryo serialization for performance

## Worker Type Comparison

| Worker Type | vCPU | Memory | Storage | Cost/DPU-Hour | Use Case |
|-------------|------|--------|---------|---------------|----------|
| **G.1X** | 4 | 16 GB | 64 GB | $0.44 | Small to medium datasets |
| **G.2X** | 8 | 32 GB | 128 GB | $0.88 | Medium to large datasets |
| **G.025X** | 2 | 4 GB | 64 GB | $0.44 | Development/testing |

**Recommendation**: Start with G.1X and 2 workers for anime dataset.

## Job Monitoring

### CloudWatch Logs
- Log Group: `/aws-glue/jobs/output`
- Log Stream: `anime-data-processor-pyspark`

### Spark UI
- Access via Glue Console → Job runs → View Spark UI
- Monitor stages, tasks, and performance metrics

### Glue Console
- Monitor job runs in AWS Glue Console → Jobs → Job runs
- View execution details, duration, DPU hours consumed

## Cost Optimization

### PySpark Resource Management
- **Minimum**: 2 DPUs (1 driver + 1 executor)
- **Recommended**: 2-4 DPUs for anime dataset
- **Cost**: $0.44/DPU-hour
- **Typical run**: 5-10 minutes = ~$0.15-$0.30 per run

### Auto Scaling
```bash
# Enable auto scaling for dynamic workloads
aws glue put-workflow \
    --name "anime-processing-workflow" \
    --default-run-properties '{
        "--enable-auto-scaling": "true",
        "--enable-glue-datacatalog": "true"
    }'
```

### Resource Optimization Tips
1. **Coalesce partitions**: Reduces small file overhead
2. **Enable AQE**: Optimizes joins and aggregations
3. **Use appropriate worker types**: G.1X for small datasets
4. **Set appropriate timeout**: Avoid long-running idle jobs

## Scheduling

### Using AWS Glue Triggers
```bash
aws glue create-trigger \
    --name "anime-data-daily-processor-pyspark" \
    --type "SCHEDULED" \
    --schedule "cron(0 6 * * ? *)" \
    --actions '[{
        "JobName": "anime-data-processor-pyspark",
        "Timeout": 60,
        "Arguments": {
            "--date": "$(date +%Y-%m-%d)"
        }
    }]' \
    --description "Run PySpark anime data processing daily at 6 AM UTC"
```

### Using EventBridge (recommended)
```bash
aws events put-rule \
    --name "anime-pyspark-processing-schedule" \
    --schedule-expression "cron(0 6 * * ? *)" \
    --description "Trigger PySpark anime data processing daily"

aws events put-targets \
    --rule "anime-pyspark-processing-schedule" \
    --targets '[{
        "Id": "1",
        "Arn": "arn:aws:glue:us-east-2:YOUR_ACCOUNT_ID:job/anime-data-processor-pyspark",
        "RoleArn": "arn:aws:iam::YOUR_ACCOUNT_ID:role/EventBridgeGlueRole"
    }]'
```

## Performance Tuning

### Spark Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
spark.conf.set("spark.sql.files.openCostInBytes", "4MB")
```

### Data Layout Optimization
- **Partitioning**: By year for anime data
- **File format**: Parquet with Snappy compression
- **File size**: Target 128MB per file
- **Bucketing**: Consider for frequently joined tables

## Troubleshooting

### Common Issues
1. **IAM Permissions**: Ensure Glue role has S3 read/write access
2. **Script Path**: Verify script is uploaded to correct S3 location
3. **Worker Resources**: Insufficient memory/workers for large datasets
4. **Spark UI**: Use for debugging performance bottlenecks
5. **Executor Memory**: OOM errors with large JSON files
6. **Driver Memory**: Increase if collecting large datasets

### Debugging Steps
1. **Check CloudWatch Logs**: `/aws-glue/jobs/output`
2. **Enable Spark UI**: Monitor task execution and stages
3. **Verify Data Sources**: Ensure S3 paths exist and accessible
4. **Test Locally**: Run with smaller dataset first
5. **Check Partitioning**: Avoid too many small partitions

### Memory Optimization
```bash
# For large datasets, increase worker memory
aws glue update-job \
    --job-name "anime-data-processor-pyspark" \
    --job-update '{
        "WorkerType": "G.2X",
        "NumberOfWorkers": 4,
        "DefaultArguments": {
            "--conf": "spark.sql.shuffle.partitions=200"
        }
    }'
```

## Migration Notes

### From Python Shell to PySpark
- **Resource increase**: From 0.0625 DPU to 2+ DPUs minimum
- **Cost impact**: ~10-30x increase but better performance
- **Scalability**: Can handle much larger datasets
- **Monitoring**: Enhanced with Spark UI and metrics
- **Dependencies**: Same libraries (pandas, boto3, pyarrow) available

### Debugging
- Enable detailed logging by setting `LOG_LEVEL=DEBUG` in job parameters
- Check CloudWatch logs for detailed error messages
- Test script locally first using similar S3 structure

## Output Structure

The job creates the following Parquet files in S3:

```
s3://anime-mvp-data/processed/
├── anime/
│   └── anime.parquet                    # Main anime metadata
├── anime_genres/
│   └── anime_genres.parquet            # Anime-genre relationships
├── anime_studios/
│   └── anime_studios.parquet           # Anime-studio relationships
├── anime_producers/
│   └── anime_producers.parquet         # Anime-producer relationships
├── anime_themes/
│   └── anime_themes.parquet            # Anime-theme relationships
├── anime_demographics/
│   └── anime_demographics.parquet      # Anime-demographic relationships
├── anime_relations/
│   └── anime_relations.parquet         # Anime relationship graph
├── anime_statistics/
│   └── anime_statistics.parquet        # User viewing statistics
├── anime_recommendations/
│   └── anime_recommendations.parquet   # User recommendations
└── genres/
    └── genres.parquet                  # Master genres list
```

Each file is optimized for querying by agents and contains processed, clean data ready for analysis.