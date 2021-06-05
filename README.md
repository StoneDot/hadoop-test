# Execute samples
Below commands execute samples on your cluster.

## Word Count
```shell
# On the project root directory
$ hadoop jar target/hadoop-test-1.0-SNAPSHOT.jar com.stonedot.hadoop.WordCount file:///$(pwd)/samples/word-inputs file:///$(pwd)/output/word-count
```