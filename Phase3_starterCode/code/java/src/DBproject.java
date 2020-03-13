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
				System.out.println("10.Find total number of passengers in all statuses");
				System.out.println("11.Exit");
				
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
					case 10:FindPassengersInAllStatus(esql); break;
					case 11:keepon = false; break;
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
			System.out.print("Please enter Plane ID: ");
			int input_pid = Integer.parseInt(in.readLine()); // Plane ID
			System.out.print("Please enter Plane Make: ");
			String input_pmake = in.readLine(); //Plane make
			System.out.print("Please enter Plane Model: ");
			String input_pmodel = in.readLine(); //Plane model
			System.out.print("Please enter Plane Age: ");
			String input_page = in.readLine(); //Plane Age
			System.out.print("Please enter Plane Seats: "); 
			String input_pseats = in.readLine(); //Plane Seats
			
			String query = "INSERT INTO Plane VALUES(" 
					+input_pid+ ",'" 
					+input_pmake+ "','"  
					+input_pmodel+ "'," 
					+input_page+ "," 
					+input_pseats+ ");"; 			
			
			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2 Ask the user for details of a pilot and add it to the DB
		try{	
			System.out.print("Please enter the Pilot ID: ");
			int input_pilot_id = Integer.parseInt(in.readLine()); //Pilot ID
			System.out.print("Please enter the full name of the pilot: ");
			String input_pilot_name = in.readLine(); //Pilot Name
			System.out.print("Please enter the nationality of the pilot: ");
			String input_pilot_nation = in.readLine(); // Pilot Nationality
			
			String query = "INSERT INTO Pilot VALUES(" 
				+input_pilot_id+ ",'" 
				+input_pilot_name+ "','" 
				+input_pilot_nation+ "');";
			
			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
		try{	
			System.out.print("Please enter Flight Number: ");
			int input_flight_num = Integer.parseInt(in.readLine()); //Flight number
			System.out.print("Please enter Flight Cost: ");
			int input_flight_cost = Integer.parseInt(in.readLine()); //Flight Cost
			System.out.print("Please enter number of tickets sold: ");
			int input_flight_sold = Integer.parseInt(in.readLine()); //Tickets sold
			System.out.print("Please enter number of stops: "); 
			int input_flight_stops = Integer.parseInt(in.readLine()); //Number of stops
			System.out.print("Please enter departure date: "); 
			String input_flight_departure_date = in.readLine(); //Departure Date
			System.out.print("Please enter arrival date: "); 
			String input_flight_arrival_date = in.readLine(); //Arrival Date
			System.out.print("Please enter arrival airport code: ");
			String input_flight_arrival_airport = in.readLine(); //Arrival Airport
			System.out.print("Please entere departure airport code: ");
			String input_flight_departure_airport = in.readLine(); //Departure Airport
			
			String query = "INSERT INTO Flight VALUES(" 
					+input_flight_num+ "," 
					+input_flight_cost+ "," 
					+input_flight_sold+ "," 
					+input_flight_stops+ ",'" 
					+input_flight_departure_date+ "','" 
					+input_flight_arrival_date+ "','" 
					+input_flight_arrival_airport+ "','" 
					+input_flight_departure_airport+ "');"; 
			
			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4 Ask user for details of a technician and add it to the DB
		try{
			System.out.print("Please enter the Technician ID: ");
			int input_tech_id = Integer.parseInt(in.readLine()); //Technician ID
			System.out.print("Please enter the Technician Name: ");
			String input_tech_name = in.readLine(); //Technician Name

			String query = "INSERT INTO Technician VALUES("
					+input_tech_id+ ",'"
					+input_tech_name+ "');";
			
			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		} 
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
		try{
			System.out.print("Please enter the Customer ID: "); 
			int input_cust_id = Integer.parseInt(in.readLine());
			System.out.print("Please enter the Flight Number you are trying to book: ");
			int input_flight_num = Integer.parseInt(in.readLine());
			System.out.print("Checking if there are seats available...\n");
			
			String reservation_generator = "SELECT COUNT(*) FROM Reservation";
			String current_reservation_num = esql.executeQueryAndReturnResult(reservation_generator).get(0).get(0);
			int reservation_num = Integer.parseInt(current_reservation_num) + 1;

			String seats_query = "SELECT P.seats - F.num_sold AS Remaining_Seats FROM Plane P, Flight F WHERE P.id IN (SELECT FI.plane_id FROM FlightInfo FI WHERE FI.flight_id =" +input_flight_num+ ") AND F.fnum IN (SELECT S.flightNum FROM Schedule S WHERE S.flightNum = " +input_flight_num+ ")";
				
			String current_seats = esql.executeQueryAndReturnResult(seats_query).get(0).get(0);
			System.out.println(current_seats);
			int available_seats = Integer.parseInt(current_seats);		
			if(available_seats > 0)
			{
				System.out.print("We have seats! \n");
				String query = "INSERT INTO Reservation VALUES("
						+reservation_num+ ","
						+input_cust_id+ ","
						+input_flight_num+ ", 'R')";
				esql.executeUpdate(query);
				System.out.print("Your seat has been reserved for flight: " +input_flight_num+ " and your reservation number is: " +reservation_num+ "\n");
			}else{
				System.out.print("Sorry no seats! \n");
				String query = "INSERT INTO Reservation VALUES("
						+reservation_num+ ","
						+input_cust_id+ ","
						+input_flight_num+ ", 'W')";
				esql.executeUpdate(query);
				System.out.print("You've been put on the waitlist for flight: " +input_flight_num+ " and your reservation number is: " +reservation_num+ "\n");
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
		String query = "(SELECT P.seats - F.num_sold AS Remaining_Seats FROM Plane P, Flight F WHERE P.id IN (SELECT FI.plane_id FROM FlightInfo FI WHERE FI.flight_id = '" +input_fn+"') AND F.fnum IN (SELECT S.flightNum FROM Schedule S WHERE S.flightNum = '" +input_fn+ "' AND S.departure_time = '"+input_dp+"'))";   
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
		System.out.print("Please enter you flight id: ");
		String input_fn = in.readLine();
		System.out.print("Please enter your flight status: ");
		String input_fs = in.readLine();


		//INPUT: R >> 1479     OUTPUT: 3
		String query = "SELECT R.status, COUNT(R.status) FROM Reservation R WHERE R.fid = '"+input_fn+"' AND R.status = '"+input_fs+"' GROUP BY R.status;";
		esql.executeQueryAndPrintResult(query);
		status_list = esql.executeQueryAndReturnResult(query);
	   } catch(Exception e) {
		System.err.println(e.getMessage());
	   }
	}
	public static void FindPassengersInAllStatus(DBproject esql) {//10
		// Find how many passengers there are with a status W,C,R and list count of each.
	   List< List<String> > total_status_list;
	   try{
		String query = "SELECT R.status, COUNT(R.status) FROM Reservation R GROUP BY R.status;";
		esql.executeQueryAndPrintResult(query);
		total_status_list = esql.executeQueryAndReturnResult(query);
	   } catch(Exception e) {
		System.err.println(e.getMessage());
	   }
	}
}
