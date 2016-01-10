package edu.umass.cs.contextservice.queryparsing.functions;

import java.sql.ResultSet;
import java.util.Vector;

import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public class DistanceFunction extends AbstractFunction 
{
	public DistanceFunction(String funcName, String[] funcArguments) 
	{
		super(funcName, funcArguments);
		
	}

	@Override
	public Vector<ProcessingQueryComponent> getProcessingQueryComponents() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean checkDBRecordAgaistFunction(ResultSet rs) {
		// TODO Auto-generated method stub
		return false;
	}

}
