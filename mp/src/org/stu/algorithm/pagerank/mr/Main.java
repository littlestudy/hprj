package org.stu.algorithm.pagerank.mr;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String[] {
			"/home/ym/ytmp/algorithm/pagerank/webgraph.txt",
			"/home/ym/ytmp/output/pagerank/"	
		};
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		String inputFile = args[0];
		String outputDir = args[1];
		
		iterate(inputFile, outputDir);
		
		return 0;
	}
	
	public void iterate(String input, String output) throws Exception {
		Configuration conf = getConf();
		
		Path outputPath = new Path(output);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		outputPath.getFileSystem(conf).mkdirs(outputPath);
		
		Path inputPath = new Path(outputPath, "input.txt");
		
		int numNodes = createInputFile(new Path(input), inputPath);
		
		int iter = 1;
		double desiredConvergence = 0.01;
		
		while (true){
			Path jobOutputPath = new Path(outputPath, String.valueOf(iter));
			System.out.println("======================================");
			System.out.println("=  Iteration:    " + iter);
			System.out.println("=  Input path:   " + inputPath);
			System.out.println("=  Output path:  " + jobOutputPath);
			System.out.println("======================================");
			
			if(calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence){
				System.out.println("Convergence is below " + desiredConvergence + ", we're done.");
				break;
			}
			
			inputPath = jobOutputPath;
			iter++;
		}
	}

	public int createInputFile(Path file, Path targetFile) throws IOException{
		Configuration conf = getConf();
		FileSystem fs = file.getFileSystem(conf);
		
		int numNodes = getNumNodes(file);
		double initialPageRank = 1.0 / (double) numNodes;
		
		OutputStream os = fs.create(targetFile);
		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF-8");
		
		while(iter.hasNext()){
			String line = iter.nextLine();
			
			String[] parts = StringUtils.split(line);
			Node node = new Node()
								.setPageRank(initialPageRank)
								.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
			IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
		}
		os.close();
		return numNodes;
	}
	
	public int getNumNodes(Path file) throws IOException{
		Configuration conf = getConf();
		FileSystem fs = file.getFileSystem(conf);
		
		return IOUtils.readLines(fs.open(file), "UTF-8").size();
	}
	
	public double calcPageRank(Path inputPath, Path outputPath, int numNodes) throws Exception{
		Configuration conf = getConf();
		conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Main.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		if(!job.waitForCompletion(true))
			throw new Exception("Job failed.");
		
		long summedConvergence = job.getCounters().findCounter(Reduce.Counter.CONV_DELTAS).getValue();
		
		double convergence = ((double) summedConvergence / Reduce.CONVERGENCE_SCALING_FACTOR / (double) numNodes);
				
		System.out.println("======================================");
		System.out.println("=  Num nodes:           " + numNodes);
		System.out.println("=  Summed convergence:  " + summedConvergence);
		System.out.println("=  Convergence:         " + convergence);
		System.out.println("======================================");

		return convergence;
	}
}
