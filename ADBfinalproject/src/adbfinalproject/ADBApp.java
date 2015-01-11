package adbfinalproject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author Vangie Shue
 **/
public class ADBApp {

	private static Scanner input;

	public static void main(String[] args) {

		// Some Constant Variables given by problem
		final int num_sites = 10;
		final int num_vars = 20;

		TransactionManager manager = new TransactionManager(num_sites, num_vars, true);

		// Choose either to read the file or wait for stdin
		String request;

		System.out.println("Argument Length: " + args.length);
		if (args.length == 1) {
			System.out.println("File passed: " + args[0]);
			File file = new File(args[0]);
			try {
				input = new Scanner(file);
				while (input.hasNextLine()) {
					request = input.nextLine();
					System.out.println("===============PROCESSING");

					manager.parseLine(request, true);

					System.out.println("=========================");
					if (request.equals("STOP")) {
						break;
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("No file passed, so reading from stdin.");
			input = new Scanner(System.in);
			while (input.hasNext()) {
				request = input.nextLine();
				System.out.println("===============PROCESSING");

				manager.parseLine(request, true);

				System.out.println("=========================");
				if (request.equals("STOP")) {
					break;
				}
			}
		}
	}

}
