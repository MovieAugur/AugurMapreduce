package augur.org;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AugurMapreduceTool extends Configured implements Tool {

	public static class Map extends
			Mapper<Text, Text, Text, IndividualMetric> {
		public void map(Text movieName, Text comment, Context context) {
			NLP nlp = new NLP();
			int value = nlp.analyse(comment.toString());
		}
	}

	public static class Reduce extends
			Reducer<Text, IndividualMetric, Text, Text> {
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
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndividualMetric.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		return 0;
	}
}

enum metricType {
	TwitterComment, AudienceComment, CriticsComment, AudienceScore, CriticsScore, VideoLikes, VideoDislikes, VideoViews, BoxOfficeCollection
}

enum CommentSource {
	Wiki, Twitter, RottenTomatoes, YouTube
}

enum CommentType {
	Text, Numeric
}

enum ExtraCommentType {
	Audience, Critic, Likes, Dislikes, Views, BoxOffice
}

class MovieMetric implements Writable {

	metricType type;

	Map<metricType, DoubleWritable> metrics = new HashMap<metricType, DoubleWritable>();
	
	String ToString()
	{
		String str = new String();
		
		
		return str;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type = metricType.values()[in.readInt()];
		for (metricType key : metricType.values()) {
			metrics.put(key, new DoubleWritable(in.readDouble()));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(type.ordinal());
		for (metricType key : metricType.values()) {
			metrics.get(key).write(out);
		}
	}
}

class IndividualMetric implements Writable {
	IndividualMetric(MovieComment commentInfo, double num) {
		source = commentInfo.source;
		type = commentInfo.type;
		exType = commentInfo.exType;

		metricValue = new DoubleWritable(num);
	}

	CommentSource source;
	CommentType type;
	ExtraCommentType exType;

	DoubleWritable metricValue;

	@Override
	public void readFields(DataInput in) throws IOException {
		source = CommentSource.values()[in.readInt()];
		type = CommentType.values()[in.readInt()];
		exType = ExtraCommentType.values()[in.readInt()];
		metricValue.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(source.ordinal());
		out.writeInt(type.ordinal());
		out.writeInt(exType.ordinal());
		metricValue.write(out);
	}
}

class MovieComment implements Writable {

	MovieComment(CommentSource cSrc, CommentType cType,
			ExtraCommentType cExType, String comment, double num) {
		source = cSrc;
		type = cType;
		exType = cExType;

		if (cType == CommentType.Text) {
			textComment = new Text(comment);
			numComment = new DoubleWritable();
		} else if (cType == CommentType.Numeric) {
			textComment = new Text();
			numComment = new DoubleWritable(num);
		}
	}

	CommentSource source;
	CommentType type;
	ExtraCommentType exType;

	Text textComment;

	DoubleWritable numComment;

	@Override
	public void readFields(DataInput in) throws IOException {
		source = CommentSource.values()[in.readInt()];
		type = CommentType.values()[in.readInt()];
		exType = ExtraCommentType.values()[in.readInt()];
		textComment.readFields(in);
		numComment.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(source.ordinal());
		out.writeInt(type.ordinal());
		out.writeInt(exType.ordinal());
		textComment.write(out);
		numComment.write(out);
	}
}