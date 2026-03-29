use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;

use spark_connect_rs::dataframe::SaveMode;
use spark_connect_rs::functions::*;
use spark_connect_rs::spark::expression::Literal;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Spark Connect Rust PoC ===\n");

    // 1. Session with timeouts
    println!("1. Creating SparkSession with timeouts...");
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
        .app_name("spark-connect-rs-poc")
        .connect_timeout(Duration::from_secs(10))
        .request_timeout(Duration::from_secs(60))
        .build()
        .await?;

    let version = spark.version().await?;
    println!("   Connected to Spark {}\n", version);

    // 2. SQL execution
    println!("2. Basic SQL...");
    let df = spark
        .sql("SELECT 'hello from rust' AS message, 42 AS value")
        .await?;
    df.show(Some(10), None, None).await?;

    // 3. Parameterized SQL (named args)
    println!("\n3. Parameterized SQL (named args)...");
    let mut args = HashMap::new();
    args.insert("threshold".to_string(), Literal::from(50i32));
    let df = spark
        .sql_with_args(
            "SELECT * FROM VALUES (1, 'low'), (100, 'high') AS data(value, label) WHERE value > :threshold",
            args,
        )
        .await?;
    df.show(Some(10), None, None).await?;

    // 4. Parameterized SQL (positional args)
    println!("\n4. Parameterized SQL (positional args)...");
    let pos_args = vec![Literal::from(10i32), Literal::from("active".to_string())];
    let df = spark
        .sql_with_pos_args(
            "SELECT * FROM VALUES (5, 'inactive'), (20, 'active'), (15, 'active') AS data(score, status) WHERE score > ? AND status = ?",
            pos_args,
        )
        .await?;
    df.show(Some(10), None, None).await?;

    // 5. Create DataFrame from RecordBatch
    println!("\n5. Create DataFrame from RecordBatch...");
    let names: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana"]));
    let ages: ArrayRef = Arc::new(Int64Array::from(vec![30, 25, 35, 28]));
    let salaries: ArrayRef = Arc::new(Float64Array::from(vec![75000.0, 55000.0, 90000.0, 65000.0]));

    let batch = RecordBatch::try_from_iter(vec![
        ("name", names),
        ("age", ages),
        ("salary", salaries),
    ])?;

    let df = spark.create_dataframe(&batch)?;
    df.clone().show(Some(10), None, None).await?;

    // 6. DataFrame transformations
    println!("\n6. DataFrame transformations (filter, select, sort)...");
    let result = df
        .clone()
        .filter(col("age").gt(lit(27)))
        .select(vec![
            col("name"),
            col("age"),
            col("salary"),
            (col("salary") / lit(12.0)).alias("monthly_salary"),
        ])
        .sort([col("salary").desc()])
        .collect()
        .await?;
    print_batches(&[result])?;

    // 7. Aggregations
    println!("\n7. Aggregations...");
    let agg_result = df
        .clone()
        .agg(vec![
            count(col("name")).alias("total"),
            avg(col("age")).alias("avg_age"),
            sum(col("salary")).alias("total_salary"),
            min(col("salary")).alias("min_salary"),
            max(col("salary")).alias("max_salary"),
        ])
        .collect()
        .await?;
    print_batches(&[agg_result])?;

    // 8. Write and read Parquet
    println!("\n8. Write/Read Parquet...");
    df.clone()
        .write()
        .mode(SaveMode::Overwrite)
        .format("parquet")
        .save("file:///tmp/poc-employees.parquet")
        .await?;
    println!("   Written to /tmp/poc-employees.parquet");

    let df_read = spark
        .read()
        .format("parquet")
        .load(["file:///tmp/poc-employees.parquet"])?;
    df_read.show(Some(10), None, None).await?;

    // 9. to_local_iterator (batch-by-batch)
    println!("\n9. to_local_iterator (batch-by-batch)...");
    let batches = spark
        .sql("SELECT * FROM VALUES (1),(2),(3),(4),(5) AS data(id)")
        .await?
        .to_local_iterator()
        .await?;
    println!("   Received {} batch(es)", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        println!("   Batch {}: {} rows", i, batch.num_rows());
    }

    // 10. Higher-order functions
    println!("\n10. Higher-order functions (transform, filter, aggregate)...");
    let df_arrays = spark
        .sql("SELECT array(1, 2, 3, 4, 5) AS numbers")
        .await?;

    // transform: multiply each element by 10
    let transformed = df_arrays
        .clone()
        .select(vec![transform(
            col("numbers"),
            lvar("x") * lit(10),
            "x",
        )
        .alias("multiplied")])
        .collect()
        .await?;
    print_batches(&[transformed])?;

    // aggregate: sum all elements
    let aggregated = df_arrays
        .clone()
        .select(vec![aggregate(
            col("numbers"),
            lit(0),
            lvar("acc") + lvar("x"),
            "acc",
            "x",
        )
        .alias("sum")])
        .collect()
        .await?;
    print_batches(&[aggregated])?;

    // 11. Checkpoint
    println!("\n11. Checkpoint...");
    let df_check = spark.range(None, 100, 1, Some(4));
    let df_cached = df_check.checkpoint(true).await?;
    let row_count = df_cached.count().await?;
    println!("   Checkpointed DataFrame count: {}", row_count);

    // 12. Observe
    println!("\n12. Observe (metric collection)...");
    let df_obs = spark
        .sql("SELECT * FROM VALUES (1),(2),(3),(4),(5) AS data(value)")
        .await?
        .observe("my_metrics", vec![count(lit(1)).alias("cnt"), avg(col("value")).alias("avg_val")]);
    let result = df_obs.collect().await?;
    println!("   Observed DataFrame collected: {} rows", result.num_rows());

    // 13. Catalog operations
    println!("\n13. Catalog operations...");
    let catalog = spark.catalog();
    let db = catalog.clone().current_database().await?;
    println!("   Current database: {}", db);
    let tables = catalog.list_all_tables().await?;
    println!("   Tables: {} found", tables.num_rows());

    // 14. Session version and config
    println!("\n14. Runtime config...");
    let mut conf = spark.conf();
    let shuffle = conf.get_value("spark.sql.shuffle.partitions").await?;
    println!("   spark.sql.shuffle.partitions = {}", shuffle);

    // 15. Stop session
    println!("\n15. Stopping session...");
    spark.stop().await?;
    println!("   Session stopped.\n");

    println!("=== PoC Complete! All features working. ===");
    Ok(())
}
