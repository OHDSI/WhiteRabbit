package org.ohdsi.utilities;

import org.ohdsi.whiteRabbit.WhiteRabbitMain;

/* Adapted from code found on:
 * http://silentdevelopment.blogspot.com/2010/03/how-to-set-or-increase-xmx-heap-memory.html
 */
public class WhiteRabbitLauncher {
	private final static int	MIN_HEAP	= 1200;

	public static void main(String[] args) throws Exception {

		float heapSizeMegs = (Runtime.getRuntime().maxMemory() / 1024) / 1024;

		if (heapSizeMegs > MIN_HEAP || args.length > 0) {
			System.out.println("Launching with current VM");
			WhiteRabbitMain.main(args);
		} else {
			System.out.println("Starting new VM");
			String pathToJar = WhiteRabbitMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			ProcessBuilder pb = new ProcessBuilder("java", "-Xmx" + MIN_HEAP + "m", "-classpath", pathToJar, "org.ohdsi.whiteRabbit.WhiteRabbitMain");
			pb.start();
		}
	}
}
