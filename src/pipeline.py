"""PySpark ingestion, quality checks, transformation and Parquet publishing."""
import argparse
from pathlib import Path


def run(input_file: Path, output_dir: Path):
    from pyspark.sql import SparkSession, functions as F, types as T
    from pyspark.sql.window import Window

    spark = SparkSession.builder.master('local[*]').appName('retail-orders-demo').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    schema = T.StructType([
        T.StructField('order_id', T.StringType(), True),
        T.StructField('order_date', T.StringType(), True),
        T.StructField('customer_id', T.StringType(), True),
        T.StructField('region', T.StringType(), True),
        T.StructField('amount', T.StringType(), True),
        T.StructField('status', T.StringType(), True),
    ])
    try:
        raw = spark.read.option('header', True).schema(schema).csv(str(input_file))
        standardized = raw.select(
            F.trim('order_id').alias('order_id'),
            F.to_date('order_date').alias('order_date'),
            F.trim('customer_id').alias('customer_id'),
            F.initcap(F.trim('region')).alias('region'),
            F.col('amount').cast('decimal(12,2)').alias('amount'),
            F.lower(F.trim('status')).alias('status'),
        )
        required = (
            F.col('order_id').isNotNull() & (F.length('order_id') > 0) &
            F.col('order_date').isNotNull() &
            F.col('customer_id').isNotNull() & (F.length('customer_id') > 0) &
            F.col('region').isin('North', 'South', 'East', 'West') &
            F.col('amount').isNotNull() & (F.col('amount') >= 0) &
            F.col('status').isin('completed', 'refunded', 'cancelled')
        )
        numbered = standardized.withColumn(
            'row_number', F.row_number().over(Window.partitionBy('order_id').orderBy('order_date'))
        )
        valid = numbered.filter(required & (F.col('row_number') == 1)).drop('row_number')
        rejected = numbered.filter(~required | (F.col('row_number') != 1)).drop('row_number')
        total, good, bad = raw.count(), valid.count(), rejected.count()
        if total != good + bad:
            raise RuntimeError('Data-quality reconciliation failed')
        if good == 0:
            raise ValueError('No valid records; refusing to publish empty dataset')

        output_dir.mkdir(parents=True, exist_ok=True)
        valid.withColumn('order_month', F.date_format('order_date', 'yyyy-MM')).write.mode(
            'overwrite'
        ).partitionBy('order_month').parquet(str(output_dir / 'silver_orders'))
        rejected.write.mode('overwrite').parquet(str(output_dir / 'rejected_orders'))
        daily = (
            valid.filter(F.col('status') == 'completed')
            .groupBy('order_date', 'region')
            .agg(F.count('*').alias('completed_orders'), F.sum('amount').alias('gross_revenue'))
        )
        daily.write.mode('overwrite').parquet(str(output_dir / 'gold_daily_sales'))
        print(f'Rows input={total}, accepted={good}, rejected={bad}')
        return total, good, bad
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default=Path('data/raw/orders.csv'))
    parser.add_argument('--output', type=Path, default=Path('data/processed'))
    args = parser.parse_args()
    run(args.input, args.output)
