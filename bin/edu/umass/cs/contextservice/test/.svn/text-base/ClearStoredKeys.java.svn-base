package edu.umass.cs.contextservice.test;

import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import edu.umass.cs.gns.client.util.KeyPairUtils;

public class ClearStoredKeys 
{
	private static Preferences  userPreferences = Preferences.userRoot().node(KeyPairUtils.class.getName());
	public static void main(String[] args) throws BackingStoreException
	{
		userPreferences.clear();
	}
}