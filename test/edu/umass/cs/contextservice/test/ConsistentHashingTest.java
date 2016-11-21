package edu.umass.cs.contextservice.test;

import com.google.common.hash.Hashing;

import edu.umass.cs.contextservice.utils.Utils;

public class ConsistentHashingTest 
{
	public static void main(String[] args)
	{
		int[] bucketArray = new int[200];
		
		for(int i=0;i<10000; i++)
		{
			String guidName = "GUID"+i;
			String GUID = Utils.getSHA1(guidName);
			int ret = Hashing.consistentHash(GUID.hashCode(), 200);
			bucketArray[ret] = bucketArray[ret] + 1;
		}
		
		for(int i=0; i<200; i++)
		{
			System.out.println("i "+i+" "+bucketArray[i]);
		}
	}
}