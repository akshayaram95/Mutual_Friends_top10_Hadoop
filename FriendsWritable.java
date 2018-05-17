package com.utdallas.bigdata.assignment;
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FriendsWritable implements Writable{
	private List<Long> friends = new ArrayList<Long>();

	public FriendsWritable(){
	}

	public void write(DataOutput out) throws IOException{
		String data = friends.toString();
		out.writeChars(data.substring(1, data.length()-1));
	}

	public void readFields(DataInput in) throws IOException{
		String data = in.readLine();
		this.friends.clear();
		if(data != null){
			data = data.replaceAll("[^0-9,]", "");
			for(String id:data.split(",")){
				friends.add(Long.valueOf(id));
			}
		}
	}

	public List<Long> getFriends(){
		return friends;
	}

	public void set(List<Long> friends){
		this.friends = friends;	
	}
	
	public void set(String[] friends){
		this.friends.clear();
		for(String friend:friends){
			this.friends.add(Long.valueOf(friend));
		}
	}
}