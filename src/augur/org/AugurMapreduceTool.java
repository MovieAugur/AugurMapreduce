package augur.org;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AugurMapreduceTool extends Configured implements Tool {

	public static class Map extends Mapper<Text, Text, Text, IndividualMetric> {
		public void map(Text movieName, Text comment, Context context)
				throws IOException, InterruptedException {
			String type = comment.toString().substring(0, 3);
			String data = comment.toString().substring(4);
			NLP nlp = new NLP();
			double sentiment = 0;
			CommentSource src = CommentSource.None;
			CommentType typ = CommentType.None;
			ExtraCommentType exTyp = ExtraCommentType.None;
			if (type.charAt(0) == 'T') {
				typ = CommentType.Text;
				if (type.charAt(1) == 'R') {
					src = CommentSource.RottenTomatoes;
					if (type.charAt(2) == 'A') {
						exTyp = ExtraCommentType.Audience;
					}
					if (type.charAt(2) == 'C') {
						exTyp = ExtraCommentType.Critic;
					}
				}
				if (type.charAt(1) == 'Y') {
					src = CommentSource.YouTube;
				}
				if (type.charAt(1) == 'T') {
					src = CommentSource.Twitter;
				}
				sentiment = (double) nlp.analyse(data);
			}
			if (type.charAt(0) == 'N') {
				typ = CommentType.Numeric;
				if (type.charAt(1) == 'R') {
					src = CommentSource.RottenTomatoes;
					if (type.charAt(2) == 'A') {
						exTyp = ExtraCommentType.Audience;
					}
					if (type.charAt(2) == 'C') {
						exTyp = ExtraCommentType.Critic;
					}
				}
				if (type.charAt(1) == 'Y') {
					src = CommentSource.YouTube;
					if (type.charAt(2) == 'L') {
						exTyp = ExtraCommentType.Likes;
					}
					if (type.charAt(2) == 'D') {
						exTyp = ExtraCommentType.Dislikes;
					}
					if (type.charAt(2) == 'V') {
						exTyp = ExtraCommentType.Views;
					}
				}
				if (type.charAt(1) == 'T') {
					src = CommentSource.Twitter;
				}
				sentiment = Double.parseDouble(data);
			}
			context.write(movieName, new IndividualMetric(src, typ, exTyp,
					sentiment));
		}
	}

	public static class Reduce extends
			Reducer<Text, IndividualMetric, Text, Text> {
		public void reduce(Text movieName,
				Iterable<IndividualMetric> metricList, Context context)
				throws IOException, InterruptedException {
			MovieMetric movieMetric = new MovieMetric();
			int aCount = 0;
			int cCount = 0;
			int tCount = 0;
			int yCount = 0;
			for (IndividualMetric metric : metricList) {
				MetricType type = MetricType.None;
				if (metric.type == CommentType.Numeric) {
					if (metric.source == CommentSource.RottenTomatoes) {
						if (metric.exType == ExtraCommentType.Audience) {
							type = MetricType.AudienceScore;
						}
						if (metric.exType == ExtraCommentType.Critic) {
							type = MetricType.CriticsScore;
						}
					}
					if (metric.source == CommentSource.YouTube) {
						if (metric.exType == ExtraCommentType.Likes) {
							type = MetricType.VideoLikes;
						}
						if (metric.exType == ExtraCommentType.Dislikes) {
							type = MetricType.VideoDislikes;
						}
						if (metric.exType == ExtraCommentType.Views) {
							type = MetricType.VideoViews;
						}
					}
					movieMetric.metrics.put(type, new DoubleWritable(metric.metricValue.get()));
				}
				if (metric.type == CommentType.Text) {
					double tempMetricValue = 0;
					if (metric.source == CommentSource.RottenTomatoes) {
						if (metric.exType == ExtraCommentType.Audience) {
							tempMetricValue = (movieMetric.metrics
									.containsKey(MetricType.AudienceComment)) ? movieMetric.metrics
									.get(MetricType.AudienceComment).get()
									: 0.0;
							tempMetricValue += metric.metricValue.get();
							type = MetricType.AudienceComment;
							aCount++;
						}
						if (metric.exType == ExtraCommentType.Critic) {
							tempMetricValue = (movieMetric.metrics
									.containsKey(MetricType.CriticsComment)) ? movieMetric.metrics
									.get(MetricType.CriticsComment).get() : 0.0;
							tempMetricValue += metric.metricValue.get();
							type = MetricType.CriticsComment;
							cCount++;
						}
					}
					if (metric.source == CommentSource.Twitter) {
						tempMetricValue = (movieMetric.metrics
								.containsKey(MetricType.TwitterComment)) ? movieMetric.metrics
								.get(MetricType.TwitterComment).get() : 0.0;
						tempMetricValue += metric.metricValue.get();
						type = MetricType.TwitterComment;
						tCount++;
					}
					if (metric.source == CommentSource.YouTube) {
						tempMetricValue = (movieMetric.metrics
								.containsKey(MetricType.VideoComment)) ? movieMetric.metrics
								.get(MetricType.VideoComment).get() : 0.0;
						tempMetricValue += metric.metricValue.get();
						type = MetricType.VideoComment;
						yCount++;
					}
					movieMetric.metrics.put(type, new DoubleWritable(
							tempMetricValue));
				}
			}
			double Avg = movieMetric.metrics.get(MetricType.AudienceComment)
					.get() / aCount;
			movieMetric.metrics.put(MetricType.AudienceComment,
					new DoubleWritable(Avg));
			Avg = movieMetric.metrics.get(MetricType.CriticsComment).get()
					/ cCount;
			movieMetric.metrics.put(MetricType.CriticsComment,
					new DoubleWritable(Avg));
			Avg = movieMetric.metrics.get(MetricType.TwitterComment).get()
					/ tCount;
			movieMetric.metrics.put(MetricType.TwitterComment,
					new DoubleWritable(Avg));
			Avg = movieMetric.metrics.get(MetricType.VideoComment).get()
					/ yCount;
			movieMetric.metrics.put(MetricType.VideoComment,
					new DoubleWritable(Avg));

			context.write(movieName, new Text(movieMetric.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AugurMapreduceTool(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] argsFull) throws Exception {
		Job job = Job.getInstance(getConf());

		job.addArchiveToClassPath(new Path(
				"s3://augurframework/include/ejml-0.23.jar"));
		job.addArchiveToClassPath(new Path(
				"s3://augurframework/include/stanford-corenlp-3.4.1.jar"));
		job.addArchiveToClassPath(new Path(
				"s3://augurframework/include/stanford-corenlp-3.4.1-models.jar"));
		job.addFileToClassPath(new Path(
				"s3://augurframework/include/nlp_file.properties"));
		
		String[] args = new GenericOptionsParser(argsFull).getRemainingArgs();

		job.setJarByClass(AugurMapreduceTool.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndividualMetric.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}
}

enum MetricType {
	None, TwitterComment, AudienceComment, CriticsComment, AudienceScore, CriticsScore, VideoComment, VideoLikes, VideoDislikes, VideoViews, BoxOfficeCollection
}

enum CommentSource {
	None, Wiki, Twitter, RottenTomatoes, YouTube
}

enum CommentType {
	None, Text, Numeric
}

enum ExtraCommentType {
	None, Audience, Critic, Likes, Dislikes, Views, BoxOffice
}

class MovieMetric implements Writable {
	Map<MetricType, DoubleWritable> metrics = new HashMap<MetricType, DoubleWritable>();
	String[] MetricTypeString = { "None", "TwitterComment", "AudienceComment",
			"CriticsComment", "AudienceScore", "CriticsScore", "VideoComment",
			"VideoLikes", "VideoDislikes", "VideoViews", "BoxOfficeCollection" };

	public String toString() {
		String str = new String();
		for (MetricType key : MetricType.values()) {
			if (metrics.containsKey(key)) {
				str = str + MetricTypeString[key.ordinal()] + "=" + metrics.get(key).toString() + " ";
			}
		}
		return str;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		for (MetricType key : MetricType.values()) {
			metrics.put(key, new DoubleWritable(in.readDouble()));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (MetricType key : MetricType.values()) {
			metrics.get(key).write(out);
		}
	}
}

class IndividualMetric implements Writable {
	IndividualMetric() {
		source = CommentSource.None;
		type = CommentType.None;
		exType = ExtraCommentType.None;
		metricValue = new DoubleWritable();
	}

	IndividualMetric(CommentSource cSrc, CommentType cType,
			ExtraCommentType cExType, double num) {
		source = cSrc;
		type = cType;
		exType = cExType;

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

// class MovieComment implements Writable {
//
// MovieComment(CommentSource cSrc, CommentType cType,
// ExtraCommentType cExType, String comment, double num) {
// source = cSrc;
// type = cType;
// exType = cExType;
//
// if (cType == CommentType.Text) {
// textComment = new Text(comment);
// numComment = new DoubleWritable();
// } else if (cType == CommentType.Numeric) {
// textComment = new Text();
// numComment = new DoubleWritable(num);
// }
// }
//
// CommentSource source;
// CommentType type;
// ExtraCommentType exType;
//
// Text textComment;
//
// DoubleWritable numComment;
//
// @Override
// public void readFields(DataInput in) throws IOException {
// source = CommentSource.values()[in.readInt()];
// type = CommentType.values()[in.readInt()];
// exType = ExtraCommentType.values()[in.readInt()];
// textComment.readFields(in);
// numComment.readFields(in);
// }
//
// @Override
// public void write(DataOutput out) throws IOException {
// out.writeInt(source.ordinal());
// out.writeInt(type.ordinal());
// out.writeInt(exType.ordinal());
// textComment.write(out);
// numComment.write(out);
// }
// }