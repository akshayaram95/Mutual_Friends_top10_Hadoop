package com.utdallas.bigdata.assignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TopMututalFriends implements Writable, Comparable{
	private String mututalFriends;
	private long count = 0;
	private String userId;

	public TopMututalFriends(){
	}
	
	public TopMututalFriends(TopMututalFriends o){
		this.mututalFriends = o.getMutualFriends();
		this.count = o.getCount();
		this.userId = o.getUserId();
	}

	public void write(DataOutput out) throws IOException{
		out.writeLong(count);
		out.writeChars(userId+";");
		out.writeChars(mututalFriends);
	}

	public void readFields(DataInput in) throws IOException{
		count = in.readLong();
		String line[] = in.readLine().split(";");
		userId = line[0];
		mututalFriends = line[1];
	}

	public String getMutualFriends() {
		return mututalFriends;
	}

	public void setMutualFriends(String mututalFriends) {
		this.mututalFriends = mututalFriends;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public void set(String userId, long count, String mututalFriends) {
		this.userId = userId;
		this.count = count;
		this.mututalFriends = mututalFriends;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		TopMututalFriends obj = (TopMututalFriends)o;
		return (int)(obj.getCount()-count);
	}

}