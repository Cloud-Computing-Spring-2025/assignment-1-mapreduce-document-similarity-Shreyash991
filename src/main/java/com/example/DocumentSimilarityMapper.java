package com.example.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class DocumentSimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        String[] words = value.toString().toLowerCase().split("\\s+");
        
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(new Text(word), new Text(fileName));
            }
        }
    }
}
