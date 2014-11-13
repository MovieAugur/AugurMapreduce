package augur.org;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AugurMapreduceTool extends Configured implements Tool {

	public static class Map extends Mapper<> {
		public void map() {
			
		}
	}
	
	public static class Reduce extends Reducer<> { 
		public void reduce() {
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AugurMapreduceTool(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "augurmr");
		
		job.setJarByClass(AugurMapreduceTool.class);
		
		
		job.waitForCompletion(true);
		return 0;
	}

}
