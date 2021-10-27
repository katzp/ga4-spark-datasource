## Project in WIP
Utilizing the new Spark datasourceV2 API in Spark 3.0 and Google Analytics Data API (GA4) 
the goal of this project to extend Spark to easily pull reports into a
Spark dataframe such as:
```
spark.read
  .format("com.katzp.ga4.spark.datasource.GA4DataSource")
  .option("dimensions", "date,country")
  .option("metrics", "totalUsers")
  .option("propertyId", "XXXXXXXXX")
  .option("startDate", "2021-09-01")
  .option("endDate", "2021-10-20")
  .option("serviceAccount", service)
  .load()
```

## Implemented
- Schema inference
- Parallel reads based on Data API pagination (1 partition per page)
- Column pruning pushdown

## To Do
- Build out test suite
- Filter pushdown