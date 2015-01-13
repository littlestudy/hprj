package org.v1.statistics.single.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SinglePartitioner extends Partitioner<SingleKey, IntWritable>{

	@Override
	public int getPartition(SingleKey key, IntWritable value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
