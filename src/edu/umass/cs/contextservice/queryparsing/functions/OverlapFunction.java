package edu.umass.cs.contextservice.queryparsing.functions;

import java.sql.ResultSet;
import java.util.Vector;

import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public class OverlapFunction extends AbstractFunction 
{

	public OverlapFunction(String funcName, String[] funcArguments) {
		super(funcName, funcArguments);
		// TODO Auto-generated constructor stub
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