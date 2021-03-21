# Spark Streaming 101

- Lambdas/UDFs can't be optimized by Spark

- Low level api => Dstreams == Discretized Streams

- Without checkpoints the writing to Kafka will fail

- Event Time :  The moment the record was generated

- Processing Time : The moment the record arrives at Spark

- Multiple streaming aggregations are not supported with streaming DataFrames/Datasets

- Watermarks : Handle late data in time-based aggregations
How far back we still consider records before dropping them

- Pivot() is an aggregation where one of the grouping columns values transposed into individual 
  columns with distinct data.
  
- nc -lk 9999 // start server port number in 9999 

```
P.S : A lot of information/codes are from RocktheJVM Spark Streaming Course.
(Strongly suggested)
```