package commonfriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CommonFriends {
  public static class CommonFriendsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text namepair = new Text(); // store output values
		private Text namelists = new Text();// store output keys

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {

			String line = new String();
			String[] line1 = new String[2];

			line = value.toString(); // put the values into
			line1 = line.split(" ", 2); // split one string into two parts
			StringTokenizer itr = new StringTokenizer(line1[1]);
			String pair = "";
			while (itr.hasMoreTokens()) {

				String v1 = itr.nextToken();
				if (line1[0].compareTo(v1) > 0) { // compare the capital letter
													// of name, put the bigger
													// one ahead
					pair = v1 + "," + line1[0];
				} else {
					pair = line1[0] + "," + v1;
				}
				namepair.set(pair);

				namelists.set(line1[1]);
				System.out.println("pair:" + namepair);
				System.out.println("friends:" + namelists);
				output.collect(namepair, namelists);
			}
		}
	}

	public static class CommonFriendsReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text keypair = new Text();
		private Text friendslist = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			ArrayList<String> list = new ArrayList<String>();
			ArrayList<String> l1 = new ArrayList<String>();
			ArrayList<String> l2 = new ArrayList<String>();
			String list1 = " ";
			String list2 = " ";

			while (values.hasNext()) {
				list.add(values.next().toString());
			}
			if (list.size() > 1) {
				list1 = list.get(0);
				list2 = list.get(1);
			}
			int i=0;
			StringTokenizer itr1 = new StringTokenizer(list1);
			while (itr1.hasMoreTokens()) {
			  l1.add(itr1.nextToken());
			 }
		    StringTokenizer itr2 = new StringTokenizer(list2);
		    while (itr2.hasMoreTokens()) {
	        	l2.add(itr2.nextToken());
			  }
			 l1.retainAll(l2);
			System.out.println(l1);
			keypair = key;
			String list3 = " ";			
			for (i = 0; i < l1.size(); i++) {
				if (i == l1.size() - 1) {
					list3 = list3 + l1.get(i);
				} else {
					list3 = list3 + l1.get(i) + ",";
				}
			}
			friendslist.set(list3);
			output.collect(keypair, friendslist);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CommonFriends.class);
		conf.setJobName("CommonFriends");
		conf.setMapperClass(CommonFriendsMapper.class);
		conf.setReducerClass(CommonFriendsReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		JobClient.runJob(conf);
	}
}
