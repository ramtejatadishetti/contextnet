package edu.umass.cs.contextservice.test;

import java.util.LinkedList;
import java.util.List;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

public class PermutationGeneratorTest 
{
	public static void main(String[] args)
	{
		int partitions = 5;
		int numAttrs = 3;
		
		Integer[] partitionNumArray = new Integer[partitions];
		for(int j = 0; j<partitionNumArray.length; j++)
		{
			partitionNumArray[j] = new Integer(j);
		}
		
		// Create the initial vector of 2 elements (apple, orange)
		ICombinatoricsVector<Integer> originalVector 
									= Factory.createVector(partitionNumArray);

		// Create the generator by calling the appropriate method in the Factory class. 
		// Set the second parameter as 3, since we will generate 3-elemets permutations
		Generator<Integer> gen 
			= Factory.createPermutationWithRepetitionGenerator(originalVector, numAttrs);
		
		
		for( ICombinatoricsVector<Integer> perm : gen )
		{
			System.out.println(perm.getVector());
			//ContextServiceLogger.getLogger().fine("perm.getVector() "+perm.getVector());
		}
	}
}