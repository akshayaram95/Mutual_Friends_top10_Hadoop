package com.utdallas.bigdata.assignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MutualFriends2 {
	public static class Map extends Mapper<LongWritable, Text, UserKeyWritable, FriendsWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input[] = value.toString().split("\t");
			long userId = Long.parseLong(input[0]);
			if(input.length > 1){
				String friendsList[] = input[1].split(",");
				for(String friend:friendsList){
					UserKeyWritable userKey = new UserKeyWritable();
					FriendsWritable userValue = new FriendsWritable();
					userKey.set(userId, Long.parseLong(friend));
					userValue.set(friendsList);
					context.write(userKey,userValue);
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<UserKeyWritable,FriendsWritable,Text,Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void reduce(UserKeyWritable key, Iterable<FriendsWritable> values, Context context) throws IOException, InterruptedException{
			int size = 0;
			List<String> users = new ArrayList<String>();
			for(FriendsWritable mutualFriends:values){
				users.add(mutualFriends.getFriends().toString());
				size++;
			}
			if(size == 2){
				String userFriends1 = users.get(0);
				String userFriends2 = users.get(1);
				List<String> mutualFriends = intersectionList(userFriends1, userFriends2);
				if(mutualFriends.size() > 0){
					outputValue.set(mutualFriends.toString());
					outputKey.set(key.getUserid1()+","+key.getUserid2());
					context.write(outputKey, outputValue);
				}
			}
			users = null;
		}
	
		public List<String> intersectionList(String list1, String list2){
			list1 = list1.substring(1,list1.length()-1).replaceAll("[^0-9,]", "");
			list2 = list2.substring(1,list2.length()-1).replaceAll("[^0-9,]", "");
			List<String> userList1 = Arrays.asList(list1.split(","));
			List<String> userList2 = Arrays.asList(list2.split(","));
			List<String> intersection = new ArrayList<String>();
			for(String id:userList1){
				if(userList2.contains(id)){
					intersection.add(id);
				}
			}
			return intersection;
		}
	
	}		
	public static class Map1 extends Mapper<LongWritable, Text, Text, TopMututalFriends>{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String input[] = value.toString().split("\t");
				String userIds[] = input[0].split(",");
				String friendsList[] = input[1].split(",");
				int count=friendsList.length;
				Text userKey = new Text();
				TopMututalFriends userValue = new TopMututalFriends();
				userKey.set("TopMututalFriends");
				userValue.set(input[0], count, input[1]);
				//System.out.println(userKey);
				//System.out.println(userValue);
				context.write(userKey,userValue);
			}
		}
	
		public static class Reduce1 extends Reducer<Text,TopMututalFriends,Text,Text> {
			private Text outputKey = new Text();
			private Text outputValue = new Text();
			public void reduce(Text key, Iterable<TopMututalFriends> values, Context context) throws IOException, InterruptedException{
				List<TopMututalFriends> mutualFriendsList = new ArrayList<TopMututalFriends>();
				for(TopMututalFriends temp:values) {
					mutualFriendsList.add(new TopMututalFriends(temp));
				}
				Collections.sort(mutualFriendsList);
				for(int i=0;i<10;i++) {
					String userIds[] = mutualFriendsList.get(i).getUserId().split(",");
					outputKey.set(userIds[0]+"\t"+userIds[1]);
					outputValue.set(mutualFriendsList.get(i).getMutualFriends());
					context.write(outputKey,outputValue);
				}
			}
		}
	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	if (otherArgs.length != 3) {
	  	System.err.println("Usage: Min_Max <in> <out>");
	  	System.exit(2);
	  	}
	  	Job job = new Job(conf, "MutualFriends");
	  	job.setJarByClass(MutualFriends2.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	job.setMapOutputKeyClass(UserKeyWritable.class);
	  	job.setMapOutputValueClass(FriendsWritable.class);
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	
	  	job.waitForCompletion(true);
	  	
	 	Configuration conf2 = new Configuration();
	  	String[] otherArgs1 = new GenericOptionsParser(conf2, args).getRemainingArgs();
	 
	  	Job job2 = new Job(conf, "MutualFriends2");
	  	job2.setJarByClass(MutualFriends2.class);
	  	job2.setMapperClass(Map1.class);
	  	job2.setReducerClass(Reduce1.class);
	  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(Text.class);
	  	job2.setMapOutputKeyClass(Text.class);
	  	job2.setMapOutputValueClass(TopMututalFriends.class);
	  	FileInputFormat.addInputPath(job2, new Path(otherArgs1[1]));
	  	FileOutputFormat.setOutputPath(job2, new Path(otherArgs1[2]));
	  	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  	}
}