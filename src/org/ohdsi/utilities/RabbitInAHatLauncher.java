package org.ohdsi.utilities;

import org.ohdsi.rabbitInAHat.RabbitInAHatMain;

/* Adapted from code found on:
 * http://silentdevelopment.blogspot.com/2010/03/how-to-set-or-increase-xmx-heap-memory.html
 */
public class RabbitInAHatLauncher {
	private final static int	MIN_HEAP	= 1500;

	public static void main(String[] args) throws Exception {

		float heapSizeMegs = (Runtime.getRuntime().maxMemory() / 1024) / 1024;

		if (heapSizeMegs > MIN_HEAP) {
			System.out.println("Launching with current VM");
			RabbitInAHatMain.main(args);
		} else {
			System.out.println("Starting new VM");
			String pathToJar = RabbitInAHatMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			ProcessBuilder pb = new ProcessBuilder("java", "-Xmx" + MIN_HEAP + "m", "-classpath", pathToJar, "org.ohdsi.rabbitInAHat.RabbitInAHatMain");
			pb.start();
		}
	}
}
