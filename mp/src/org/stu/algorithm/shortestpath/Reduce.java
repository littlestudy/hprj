package org.stu.algorithm.shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>{

	public static enum PathCounter{
		TARGET_NODE_DISTANCE_COMPUTED, PATH
	}
	
	private Text outValue = new Text();	
	private String targetNode;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		targetNode = context.getConfiguration().get(Main.TARGET_NODE);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		int minDistance = Node.INFINITE;
		System.out.println("reduce input -> K[" + key + "]");
		Node shortestAdjacentNode = null;
		Node originalNode = null;
		
		for (Text textValue : values){
			System.out.println("reduce input -> V[" + textValue + "]");
			Node node = Node.fromMR(textValue.toString());
			if (node.containsAdjacentNodes())
				originalNode = node;
			if (node.getDistance() < minDistance){
				minDistance = node.getDistance();
				shortestAdjacentNode = node;
			}
		}
		if (shortestAdjacentNode != null){
			originalNode.setDistance(minDistance);
			originalNode.setBackpointer(shortestAdjacentNode.getBackpointer());
		}
		outValue.set(originalNode.toString());
		System.out.println("reduce output -> K[" + key + "], V[" + outValue + "]");
		context.write(key, outValue);
		
		if (minDistance != Node.INFINITE && targetNode.equals(key.toString())){
			Counter counter = context.getCounter(PathCounter.TARGET_NODE_DISTANCE_COMPUTED);
			counter.increment(minDistance);
			context.getCounter(PathCounter.PATH.toString(), 
								shortestAdjacentNode.getBackpointer()).increment(1);
		}
	}
}
