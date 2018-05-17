package com.utdallas.bigdata.assignment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

public class UserKeyWritable implements WritableComparable<UserKeyWritable>{
	private LongWritable userId1 = new LongWritable();
	private LongWritable userId2 = new LongWritable();

	public UserKeyWritable(){
	}

	public void write(DataOutput out) throws IOException{
		userId1.write(out);
		userId2.write(out);
	}

	public void readFields(DataInput in) throws IOException{
		userId1.readFields(in);
		userId2.readFields(in);
	}

	public void set(long id1, long id2){
		if(id1 < id2){
			this.userId1.set(id1);
			this.userId2.set(id2);
		}else{
			this.userId1.set(id2);
			this.userId2.set(id1);
		}
	}

	public LongWritable getUserid1() {
		return userId1;
	}

	public LongWritable getUserid2() {
		return userId2;
	}

	@Override
	public int compareTo(UserKeyWritable o) {
		// TODO Auto-generated method stub
		int cmp = userId1.compareTo(o.userId1);
		if(cmp != 0)
			return cmp;
		return userId2.compareTo(o.userId2);
	}
	
}