package edu.umass.cs.contextservice.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ContextServiceLogger
{
	private final static Logger LOGGER = Logger.getLogger(ContextServiceLogger.class.getName());
	
	public static Logger getLogger()
	{
		LOGGER.setLevel(Level.OFF);
		return LOGGER;
		
		// set the LogLevel to Severe, only severe Messages will be written
		/*LOGGER.setLevel(Level.SEVERE);
		LOGGER.severe("Info Log");
		LOGGER.warning("Info Log");
		LOGGER.info("Info Log");
		LOGGER.finest("Really not important");
		
		// set the LogLevel to Info, severe, warning and info will be written
		// finest is still not written
		LOGGER.setLevel(Level.INFO);
		LOGGER.severe("Info Log");
		LOGGER.warning("Info Log");
		LOGGER.info("Info Log");
		LOGGER.finest("Really not important");*/
	}
}