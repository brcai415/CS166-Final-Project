/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;


/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1 Add Plane: Ask user for details of a plane and add it into the DB
		try{
			//System.out.print("Please enter Plane ID: ");
			//int input_pid = Integer.parseInt(in.readLine()); // Plane ID
			System.out.print("Please enter Plane Make: ");
			String input_pmake = in.readLine(); //Plane make
			System.out.print("Please enter Plane Model: ");
			String input_pmodel = in.readLine(); //Plane model
			System.out.print("Please enter Plane Age: ");
			String input_page = in.readLine(); //Plane Age
			System.out.print("Please enter Plane Seats: "); 
			String input_pseats = in.readLine(); //Plane Seats
			
			//Generate Plane ID			
			String generate_plane_id = "SELECT COUNT(*) FROM Plane";
			String last_plane_id = esql.executeQueryAndReturnResult(generate_plane_id).get(0).get(0);
			int input_pid = Integer.parseInt(last_plane_id) + 1;

			
			String query = "INSERT INTO Plane VALUES(" 
					+input_pid+ ",'" 
					+input_pmake+ "','"  
					+input_pmodel+ "'," 
					+input_page+ "," 
					+input_pseats+ ");"; 			
			
			System.out.print("\n");
			System.out.print("Is this information correct? (Y/N)\n");
			System.out.print("Plane ID: " +input_pid+ "\n");
			System.out.print("Plane Make: " +input_pmake+ "\n");
			System.out.print("Plane Model: " +input_pmodel+ "\n");
			System.out.print("Plane Age: " +input_page+ "\n");
			System.out.print("Plane Seats: " +input_pseats+ "\n");
				
			String answer = in.readLine();
			
			if(answer.equals("Y") || answer.equals("y"))
			{
				System.out.print("\n");
				System.out.print("Okay adding plane...\n");
				esql.executeUpdate(query);
			}else{
				System.out.print("\n");
				System.out.print("Okay returning to main menu...\n");
				return;
			}		
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2 Ask the user for details of a pilot and add it to the DB
		try{	
			//System.out.print("Please enter the Pilot ID: ");
			//int input_pilot_id = Integer.parseInt(in.readLine()); //Pilot ID
			System.out.print("Please enter the full name of the pilot: ");
			String input_pilot_name = in.readLine(); //Pilot Name
			System.out.print("Please enter the nationality of the pilot: ");
			String input_pilot_nation = in.readLine(); // Pilot Nationality
		
			//Generate Pilot ID
			String generate_pilot_id = "SELECT COUNT(*) FROM Pilot";
			String last_pilot_id = esql.executeQueryAndReturnResult(generate_pilot_id).get(0).get(0);
			int input_pilot_id = Integer.parseInt(last_pilot_id) + 1;
	
			String query = "INSERT INTO Pilot VALUES(" 
				+input_pilot_id+ ",'" 
				+input_pilot_name+ "','" 
				+input_pilot_nation+ "');";
			
			System.out.print("\n");
			System.out.print("Does this information look correct? (Y/N)\n");
			System.out.print("Pilot Name: " +input_pilot_name+ "\n");
			System.out.print("Pilot Nationality: " +input_pilot_nation+ "\n");
			String answer = in.readLine();	
			
			if(answer.equals("Y") || answer.equals("y"))
			{
				System.out.print("\n");
				System.out.print("Okay adding pilot...\n");
				esql.executeUpdate(query);
			}else{
				System.out.print("\n");
				System.out.print("Okay returning to main menu...\n");
				return;
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
		try{	
			//System.out.print("Please enter Flight Number: ");
			//int input_flight_num = Integer.parseInt(in.readLine()); //Flight number
			System.out.print("Please enter Flight Cost: ");
			int input_flight_cost = Integer.parseInt(in.readLine()); //Flight Cost
			System.out.print("Please enter number of tickets sold: ");
			int input_flight_sold = Integer.parseInt(in.readLine()); //Tickets sold
			System.out.print("Please enter number of stops: "); 
			int input_flight_stops = Integer.parseInt(in.readLine()); //Number of stops
			

			//Ask for and format Departure Time and Date
			System.out.print("Please enter departure date (yyyymmdd): "); 
			String input_flight_departure_date = in.readLine(); //Departure Date
			System.out.print("Please enter the time of departure (hh:mm): ");
			String input_flight_departure_time = in.readLine(); //Departure Time
			input_flight_departure_date = input_flight_departure_date+ " " +input_flight_departure_time;
			
			//Ask for and format Arrival Time and Date
			System.out.print("Please enter arrival date (yyyymmdd): "); 
			String input_flight_arrival_date = in.readLine(); //Arrival Date
			System.out.print("Please enter arrival time (hh:mm): ");
			String input_flight_arrival_time = in.readLine(); //Arrival Time
			input_flight_arrival_date = input_flight_arrival_date+ " " +input_flight_arrival_time;
		
			System.out.print("Please enter arrival airport code: ");
			String input_flight_arrival_airport = in.readLine(); //Arrival Airport
			System.out.print("Please entere departure airport code: ");
			String input_flight_departure_airport = in.readLine(); //Departure Airport
			
			//Generating flight number			
			String generate_flight_num = "SELECT COUNT(*) FROM Flight";
			String last_flight_num = esql.executeQueryAndReturnResult(generate_flight_num).get(0).get(0);
			int input_flight_num = Integer.parseInt(last_flight_num) + 1;

			String addFlight = "INSERT INTO Flight VALUES(" 
					+input_flight_num+ "," 
					+input_flight_cost+ "," 
					+input_flight_sold+ "," 
					+input_flight_stops+ ",'" 
					+input_flight_departure_date+ "','" 
					+input_flight_arrival_date+ "','" 
					+input_flight_arrival_airport+ "','" 
					+input_flight_departure_airport+ "');"; 
			
						
			System.out.print("Please enter the Pilot ID for the flight:");
			int input_pilot_id = Integer.parseInt(in.readLine());
			System.out.print("Please enter the Plane ID for the flight:");
			int input_plane_id = Integer.parseInt(in.readLine());

			//Generate flight info ID	
			String generate_info_id = "SELECT COUNT(*) FROM FlightInfo";
			String last_info_id = esql.executeQueryAndReturnResult(generate_info_id).get(0).get(0);
			int input_info_id = Integer.parseInt(last_info_id) + 1;
	
			String addFlightInfo = "INSERT INTO FlightInfo VALUES("
						+input_info_id+ ","
						+input_flight_num+ ","
						+input_pilot_id+ ","
						+input_plane_id+ ");";			
			
						
			//Generate flight info ID	
			String generate_schedule_id = "SELECT COUNT(*) FROM Schedule";
			String last_schedule_id = esql.executeQueryAndReturnResult(generate_schedule_id).get(0).get(0);
			int input_schedule_id = Integer.parseInt(last_schedule_id) + 1;
		
			String addSchedule = "INSERT INTO Schedule VALUES("
					+input_schedule_id+ ","
					+input_flight_num+ ",'"
					+input_flight_departure_date+ "','"
					+input_flight_arrival_date+ "');";
			
			System.out.print("\n");			
			System.out.print("Does this information look correcti(Y/N)?\n");
			System.out.print("Cost of flight: " +input_flight_cost+ "\n");
			System.out.print("Tickets sold: " +input_flight_sold+ "\n");
			System.out.print("Number of stops: " +input_flight_stops+ "\n");
			System.out.print("Departure Date and Time: " +input_flight_departure_date+ "\n");
			System.out.print("Arrival Date and Time: " +input_flight_arrival_date+ "\n");	
			System.out.print("Arrival Airport: " +input_flight_arrival_airport+ "\n");
			System.out.print("Departure Airport: " +input_flight_departure_airport+ "\n");
			System.out.print("Pilot ID: " +input_pilot_id+ "\n");
			System.out.print("Plane ID: " +input_plane_id+ "\n\n");

			String correct = in.readLine();
			
			if(correct.equals("Y")|| correct.equals("y"))
			{	
				System.out.print("Great! Adding flight...\n");
				esql.executeUpdate(addFlight);
				esql.executeUpdate(addFlightInfo);
				esql.executeUpdate(addSchedule);
			}else{
				System.out.print("Returning to main menu \n");
				return;
			}		

		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4 Ask user for details of a technician and add it to the DB
		try{
			//System.out.print("Please enter the Technician ID: ");
			//int input_tech_id = Integer.parseInt(in.readLine()); //Technician ID
			System.out.print("Please enter the Technician Name: ");
			String input_tech_name = in.readLine(); //Technician Name
			
			//Generate Technician ID
			String generate_tech_id = "SELECT COUNT(*) FROM Plane";
			String last_tech_id = esql.executeQueryAndReturnResult(generate_tech_id).get(0).get(0);
			int input_tech_id = Integer.parseInt(last_tech_id) + 1;
		
			String query = "INSERT INTO Technician VALUES("
					+input_tech_id+ ",'"
					+input_tech_name+ "');";
			
			System.out.print("\n");
			System.out.print("Does this information look correct? (Y/N) \n");
			System.out.print("Tech Name: " +input_tech_name+ "\n");
			String answer = in.readLine();
			
			if(answer.equals("Y") || answer.equals("y"))
			{
				System.out.print("\n");
				System.out.print("Okay adding technician...\n)");		
				esql.executeUpdate(query);
			}else{
				System.out.print("Returning to main menu \n");
				return;
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		} 
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
		try{
			System.out.print("Please enter the Customer ID: "); 
			int input_cust_id = Integer.parseInt(in.readLine()); //Customer ID
			System.out.print("Please enter the Flight Number you are trying to book: ");
			int input_flight_num = Integer.parseInt(in.readLine()); //Flight Number
			System.out.print("Checking if there are seats available...\n");
			
			//This generates the reservation number
			String reservation_generator = "SELECT COUNT(*) FROM Reservation"; 
			String current_reservation_num = esql.executeQueryAndReturnResult(reservation_generator).get(0).get(0); //Read in how many reservations there are currently
			int reservation_num = Integer.parseInt(current_reservation_num) + 1; //Assign current reservations + 1 as new reservation #

			//Query for checking available seats on inputed flight number.
			String seats_query = "SELECT P.seats - F.num_sold AS Remaining_Seats FROM Plane P, Flight F WHERE P.id IN (SELECT FI.plane_id FROM FlightInfo FI WHERE FI.flight_id =" +input_flight_num+ ") AND F.fnum IN (SELECT S.flightNum FROM Schedule S WHERE S.flightNum = " +input_flight_num+ ")";
				
			String current_seats = esql.executeQueryAndReturnResult(seats_query).get(0).get(0);//Read in current # of available seats
			int available_seats = Integer.parseInt(current_seats); //Parsing String into Int for comparison	
			
			//Assigning status based on whether there are seats or not on the flight
			if(available_seats > 0)
			{
				System.out.print("We have " +available_seats+ " seats! \n");
				
				//Insert query for Reserved Reservations
				String reserve = "INSERT INTO Reservation VALUES("
						+reservation_num+ ","
						+input_cust_id+ ","
						+input_flight_num+ ", 'R')";
				String flight_numSold = "UPDATE Flight SET num_sold = num_sold + 1 WHERE fnum =" +input_flight_num+ ";";
				
				System.out.print("\n");
				System.out.print("Reserve flight? (Y/N) \n");
				String answer = in.readLine();
				
				if(answer.equals("Y") || answer.equals("y"))
				{	
					esql.executeUpdate(reserve);
					esql.executeUpdate(flight_numSold);
					System.out.print("Your seat has been reserved for flight: " +input_flight_num+ " and your reservation number is: " +reservation_num+ "\n");
				}else{
					System.out.print("Okay cancelling reservation...\n");
					return;
				}
			}else{
				System.out.print("\n");
				System.out.print("Sorry no seats! Would you like to waitlist? (Y/N) \n");
				String answer2 = in.readLine();
				
				//Insert Query for Waitlisted Reservations
				String waitlist = "INSERT INTO Reservation VALUES("
						+reservation_num+ ","
						+input_cust_id+ ","
						+input_flight_num+ ", 'W')";
				String flight_numSold = "UPDATE Flight SET num_sold + 1 WHERE fnum =" +input_flight_num+ ";";
				if(answer2.equals("Y") || answer2.equals("y"))
				{
					esql.executeUpdate(flight_numSold);
					esql.executeUpdate(waitlist);
					System.out.print("You've been put on the waitlist for flight: " +input_flight_num+ " and your reservation number is: " +reservation_num+ "\n");
				}else{
					System.out.print("Okay cancelling reservation...\n");
					return;
				}
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
	    try{
		System.out.print("Please enter flight number: ");
		String input_fn = in.readLine(); // flight number
		System.out.print("Please enter departure date: ");
		String input_dp = in.readLine(); // departure date	
		// Subtracting SELECTED TOTAL SEATS - SELECTED SOLD SEATS
		// Assumes num_sold is updated and num_sold = (num_seats_sold)
		// INPUT: FlightNum: 0 >> Date: 2014-04-18
		String query = "(SELECT P.seats - F.num_sold AS Remaining_Seats FROM Plane P, Flight F WHERE P.id IN (SELECT FI.plane_id FROM FlightInfo FI WHERE FI.flight_id = " +input_fn+") AND F.fnum IN (SELECT S.flightNum FROM Schedule S WHERE S.flightNum = '" +input_fn+ "' AND S.departure_time = '"+input_dp+"'))";   
		esql.executeQueryAndPrintResult(query);
	    } catch(Exception e) {
		System.err.println (e.getMessage());
	    }
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
	   try{
		// Using P.make to make tables understandable. Plane Make is matched with # of repairs.
		List< List<String> > total_repair_list;
		String query = "SELECT P.make, COUNT(R.rid) AS Repairs FROM Repairs R, Plane P WHERE P.id IN (SELECT R.plane_id FROM Repairs) GROUP BY P.make ORDER BY COUNT(R.rid) DESC;";
		esql.executeQueryAndPrintResult(query);
		total_repair_list = esql.executeQueryAndReturnResult(query);
	   } catch(Exception e) {
		System.err.println(e.getMessage());
	   }
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
	   try{
		//EXTRACT year from date. Found on w3resource.com/PostgreSQL/extract-function.php
		List< List<String> > repair_year_list;
		String query = "SELECT COUNT(R.rid) AS repair, EXTRACT(year FROM R.repair_date) AS YEAR FROM Repairs R GROUP BY EXTRACT(year FROM R.repair_date) ORDER BY COUNT(R.rid) ASC;";
		repair_year_list = esql.executeQueryAndReturnResult(query);
		esql.executeQueryAndPrintResult(query);
	   } catch(Exception e) {
		System.err.println(e.getMessage());
	   }
	}
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
	   List< List<String> > status_list;
	   try{
		String query = "SELECT R.status, COUNT(R.status) FROM Reservation R GROUP BY R.status;";
		esql.executeQueryAndPrintResult(query);
		status_list = esql.executeQueryAndReturnResult(query);
	   } catch(Exception e) {
		System.err.println(e.getMessage());
	   }
	}
}
