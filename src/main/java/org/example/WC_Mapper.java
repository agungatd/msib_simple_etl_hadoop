package org.example;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,LongWritable>{
    public void map(LongWritable key, Text value,OutputCollector<Text,LongWritable> output,
                    Reporter reporter) throws IOException{
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length >= 2) {
            output.collect(new Text(fields[0]), new LongWritable(Long.parseLong(fields[1])));
        }
    }
}
