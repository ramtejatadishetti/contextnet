package edu.umass.cs.contextservice.test;

import org.json.JSONArray;
import org.json.JSONException;

public class SimpleJSONTester 
{
	public static void main(String[] args) throws JSONException
	{
		JSONArray jsoArr = new JSONArray();
		jsoArr.put(5, 1);
		jsoArr.put(4, 2);
		jsoArr.put(3, 3);
		jsoArr.put(2, 4);
		jsoArr.put(1, 5);
		
		System.out.println("length "+jsoArr.length());
		
		for(int i=0; i<jsoArr.length(); i++)
		{
			boolean isNull = jsoArr.isNull(i);
			if(isNull)
			{
				System.out.println("curr null i "+i);
				continue;
			}
			
			Integer curr = jsoArr.getInt(i);
			if(curr == null)
			{
				System.out.println("curr null i "+i);
			}
			else
			{
				System.out.println("curr "+curr+" i "+i);
			}
		}
	}
}