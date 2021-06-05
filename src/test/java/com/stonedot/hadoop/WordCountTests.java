package com.stonedot.hadoop;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class WordCountTests {

    @Nested
    class TokenizerMapperTests {
        @Mock
        private Mapper<Object, Text, Text, IntWritable>.Context mockContext;

        @Mock
        private Counter mockCounter;

        private WordCount.TokenizerMapper mapper;

        private WriteCaptor<Text, IntWritable, String, Integer> writeCaptor;

        @BeforeEach
        public void setUp() throws Exception {
            mapper = new WordCount.TokenizerMapper();
            writeCaptor = new WriteCaptor<>(in -> ImmutablePair.of(in.left.toString(), in.right.get()));

            doReturn(mockCounter).when(mockContext).getCounter(eq(WordCount.WordCountEnum.WORD_COUNT));
            doNothing().when(mockCounter).increment(anyLong());
            doAnswer(writeCaptor.capture()).when(mockContext).write(any(), any());
        }

        @Test
        void testWrittenValues() throws IOException, InterruptedException {
            mapper.map(0, new Text("One Two Two"), mockContext);

            verify(mockContext, times(3)).write(any(), any());

            List<ImmutablePair<String, Integer>> writeResult = writeCaptor.getAllResults();
            assertThat(writeResult.get(0).getKey()).isEqualTo("One");
            assertThat(writeResult.get(1).getKey()).isEqualTo("Two");
            assertThat(writeResult.get(2).getKey()).isEqualTo("Two");
            assertThat(writeResult.get(0).getValue()).isEqualTo(1);
            assertThat(writeResult.get(1).getValue()).isEqualTo(1);
            assertThat(writeResult.get(2).getValue()).isEqualTo(1);
        }

        @Test
        void testCounter() throws IOException, InterruptedException {
            Text l1 = new Text("This is a sample input for tests.");
            Text l2 = new Text("This should be handle correctly.");

            mapper.map(0, l1, mockContext);
            mapper.map(1, l2, mockContext);

            verify(mockCounter, times(12)).increment(1);

            List<ImmutablePair<String, Integer>> writeResult = writeCaptor.getAllResults();
            assertThat(writeResult.get(0).getKey()).isEqualTo("This");
            assertThat(writeResult.get(6).getKey()).isEqualTo("tests.");
            assertThat(writeResult.get(7).getKey()).isEqualTo("This");
        }

        @AfterEach
        public void tearDown() {
        }
    }

    @Nested
    class IntSumReducerTests {
        @Mock
        private Reducer<Text, IntWritable, Text, IntWritable>.Context mockContext;

        private WordCount.IntSumReducer reducer;

        private WriteCaptor<Text, IntWritable, String, Integer> writeCaptor;

        @BeforeEach
        public void setup() throws IOException, InterruptedException {
            reducer = new WordCount.IntSumReducer();
            writeCaptor = new WriteCaptor<>(in -> ImmutablePair.of(in.left.toString(), in.right.get()));
            doAnswer(writeCaptor.capture()).when(mockContext).write(any(), any());
        }

        @Test
        void testSummation() throws IOException, InterruptedException {
            IntWritable[] input = new IntWritable[]{
                    new IntWritable(1),
                    new IntWritable(1),
            };

            reducer.reduce(new Text("Two"), Arrays.asList(input), mockContext);

            List<ImmutablePair<String, Integer>> writeResult = writeCaptor.getAllResults();
            assertThat(writeResult.get(0).getKey()).isEqualTo("Two");
            assertThat(writeResult.get(0).getValue().intValue()).isEqualTo(2);
        }
    }
}
