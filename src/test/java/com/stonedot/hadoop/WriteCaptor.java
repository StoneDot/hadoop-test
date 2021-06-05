package com.stonedot.hadoop;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class WriteCaptor<K1, V1, K2, V2> {
    private final List<ImmutablePair<K2, V2>> writeList;
    Function<ImmutablePair<K1, V1>, ImmutablePair<K2, V2>> converter;

    public WriteCaptor(Function<ImmutablePair<K1, V1>, ImmutablePair<K2, V2>> converter) {
        this.writeList = new ArrayList<>();
        this.converter = converter;
    }

    public Answer<Void> capture() {
        return invocation -> {
            K1 key = invocation.getArgument(0);
            V1 value = invocation.getArgument(1);
            writeList.add(converter.apply(ImmutablePair.of(key, value)));
            return null;
        };
    }

    public List<ImmutablePair<K2, V2>> getAllResults() {
        return writeList;
    }
}
