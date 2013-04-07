package com.leopold.cassandra.demo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This sums the word count stored in the input_words_count ColumnFamily for the key "key-if-verse1".
 *
 * Output is written to a text file.
 */
public class WordCountCounters extends Configured implements Tool
{
    private static final Logger logger = Logger.getLogger(WordCountCounters.class);

    static final String COUNTER_COLUMN_FAMILY = "input_words_count";

    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count_counters";

    public static void main(String[] args)
        throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCountCounters(), args);
        System.exit(0);
    }

    public static class SumMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, Column>, Text, LongWritable>
    {
        public void map(ByteBuffer key, SortedMap<ByteBuffer, Column> columns, Context context)
            throws IOException, InterruptedException
        {
            long sum = 0;
            for (Column column : columns.values())
            {
                logger.debug("read " + key + ":" + column.name() + " from " + context.getInputSplit());
                sum += ByteBufferUtil.toLong(column.value());
            }
            context.write(new Text(ByteBufferUtil.string(key)), new LongWritable(sum));
        }
    }

    public int run(String[] args)
        throws Exception
    {
        Job job = new Job(getConf(), "wordcountcounters");
        job.setJarByClass(WordCountCounters.class);
        job.setMapperClass(SumMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));

        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "16.158.83.35");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.Murmur3Partitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE,
                WordCountCounters.COUNTER_COLUMN_FAMILY);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange()
                .setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER).setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER).setCount(100));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        job.waitForCompletion(true);
        return 0;
    }
}