package org.ohdsi.utilities;

import org.ohdsi.rabbitInAHat.RabbitInAHatMain;

import java.util.Arrays;
import java.util.ArrayList;

/* Adapted from code found on:
 * http://silentdevelopment.blogspot.com/2010/03/how-to-set-or-increase-xmx-heap-memory.html
 */
public class RabbitInAHatLauncher {
	private final static int	MIN_HEAP	= 1200;

	public static void main(String[] args) throws Exception {

		float heapSizeMegs = (Runtime.getRuntime().maxMemory() / 1024) / 1024;

		if (heapSizeMegs > MIN_HEAP) {
			System.out.println("Launching with current VM");
			RabbitInAHatMain.main(args);
		} else {
			System.out.println("Starting new VM");
			String pathToJar = RabbitInAHatMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			ArrayList<String> command = new ArrayList<String>();
			command.addAll(Arrays.asList("java", "-Xmx" + MIN_HEAP + "m", "-classpath", pathToJar, "org.ohdsi.rabbitInAHat.RabbitInAHatMain"));
			command.addAll(Arrays.asList(args));
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.start();
		}
	}
}
