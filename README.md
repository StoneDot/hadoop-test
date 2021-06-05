# Execute samples
Below commands execute samples on your cluster. Commands should be executed on the project's root directory.

## Word Count
Execute word counts against sample input.

```shell
hadoop jar target/hadoop-test-1.0-SNAPSHOT.jar com.stonedot.hadoop.WordCount file:///$(pwd)/samples/word-inputs file:///$(pwd)/output/word-count
```