package com.frg96.mapreduce.apps;

import com.frg96.mapreduce.api.*;

import java.util.List;

public class WordCountTaskFactory implements TaskFactory {

    @Override
    public Mapper createMapper() {
        return new WordCountMapper();
    }

    @Override
    public Reducer createReducer() {
        return new WordCountReducer();
    }

    private static final class WordCountMapper implements Mapper {
        @Override
        public void map(String inputLine, MapperContext context) {
            String[] words = inputLine.split("[\\s,.\"']+");

            for(String word: words) {
                if(!word.isEmpty())
                    context.emit(word, "1");
            }
        }
    }

    private static final class WordCountReducer implements Reducer {
        @Override
        public void reduce(String key, List<String> values, ReducerContext context) {
            int sum = 0;

            for(String value: values) {
                sum += Integer.parseInt(value);
            }

            context.emit(key, Integer.toString(sum));
        }
    }
}
