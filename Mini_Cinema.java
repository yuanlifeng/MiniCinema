import java.sql.*;
import java.util.Scanner;

public class Mini_Cinema{

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/";

	static final String USER = "root";
	static final String PASS = "0000";
	private static Connection con = null;
	private static Statement statement = null;

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(JDBC_DRIVER);
			createDatabase();
			createTable();
			loadDataIntoTable();
			//menu();
			// batchUpdate();
			// callingStoredProcedure();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
			try {
				if (con != null)
					con.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("Goodbye!");
	}

	private static void createDatabase() throws SQLException {

		System.out.println("Connecting to database...");
		con = DriverManager.getConnection(DB_URL, USER, PASS);
		statement = con.createStatement();

		System.out.println("Creating database...");

		String queryDropDatabase = "DROP DATABASE IF EXISTS Mini_Cinema";
		statement.execute(queryDropDatabase);

		String queryCreateDatabase = "CREATE DATABASE Mini_Cinema";
		statement.execute(queryCreateDatabase);

		System.out.println("Database created successfully...");
		con.close();
	}
	private static void createTable() throws SQLException {

		System.out.println("Connecting to database...");
		con = DriverManager.getConnection(DB_URL + "Mini_Cinema", USER, PASS);
		Statement statement = con.createStatement();

		String queryDropTableMovie = "DROP TABLE IF EXISTS Movie";
		statement.execute(queryDropTableMovie);
		String queryCreateTableMovie = "CREATE TABLE Movie( movie_id INT, title VARCHAR(50),release_date VARCHAR(20),runtime INT,budget INT,PRIMARY KEY (movie_id))";
		statement.execute(queryCreateTableMovie);
		System.out.println("Movie table created successfully...");

		String queryDropTableMovieCast = "DROP TABLE IF EXISTS MovieCast";
		statement.execute(queryDropTableMovieCast);
		String queryCreateTableMovieCast = "CREATE TABLE MovieCast(movie_id INT, movie_character VARCHAR(30), credit_id VARCHAR(24), person_id INT, name VARCHAR(30), PRIMARY KEY (credit_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableMovieCast);
		System.out.println("MovieCast table created successfully...");

		String queryDropTableMovieCrew = "DROP TABLE IF EXISTS MovieCrew";
		statement.execute(queryDropTableMovieCrew);
		String queryCreateTableMovieCrew = "CREATE TABLE MovieCrew( movie_id INT, credit_id VARCHAR(24),person_id INT,job VARCHAR(30),name VARCHAR(30),PRIMARY KEY (credit_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableMovieCrew);
		System.out.println("MovieCrew table created successfully...");

		String queryDropTableUser = "DROP TABLE IF EXISTS User";
		statement.execute(queryDropTableUser);
		String queryCreateTableUser = "CREATE TABLE User( user_id INT AUTO_INCREMENT, user_name VARCHAR(30),age INT,gender CHAR(20),registered_on DATE DEFAULT '0000-00-00',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id))";
		statement.execute(queryCreateTableUser);
		System.out.println("User table created successfully...");

		String queryDropTableWatch_List = "DROP TABLE IF EXISTS Watch_List";
		statement.execute(queryDropTableWatch_List);
		String queryCreateTableWatch_List = "CREATE TABLE Watch_List( user_id INT,movie_id INT,title VARCHAR(50),orders INT,added_on DATE DEFAULT '0000-00-00',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id, movie_id, added_on),FOREIGN KEY (user_id) REFERENCES User (user_id),FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_List);
		System.out.println("Watch_List table created successfully...");

		String queryDropTableWatch_History = "DROP TABLE IF EXISTS Watch_History";
		statement.execute(queryDropTableWatch_History);
		String queryCreateTableWatch_History = "CREATE TABLE Watch_History( user_id INT,movie_id INT,title VARCHAR(50),rating INT,favorite BOOLEAN DEFAULT FALSE,watched_on DATE DEFAULT '0000-00-00',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id, movie_id, watched_on),FOREIGN KEY (user_id) REFERENCES User (user_id),FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_History);
		System.out.println("Watch_History table created successfully...");
	}

	private static void loadDataIntoTable() throws SQLException {
		ResultSet rs;
		System.out.println("Loading data from 'tmdb_5000_movies.csv' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadMovieDataSQL = "LOAD DATA LOCAL INFILE 'movies.csv' INTO TABLE Movie FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,title,release_date,runtime,budget)";
		statement.execute(loadMovieDataSQL);
		System.out.println("Data succesfullly loaded into TABLE Movie");
		rs = statement.executeQuery("SELECT * FROM Movie");
		printResultSetfromMovie(rs);

		System.out.println("Loading data from 'cast.csv' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadCastDataSQL = "LOAD DATA LOCAL INFILE 'cast.csv' INTO TABLE MovieCast FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,movie_character,credit_id,person_id,name)";
		statement.execute(loadCastDataSQL);
		System.out.println("Data succesfullly loaded into TABLE MovieCast");
		rs = statement.executeQuery("SELECT * FROM MovieCast");
		printResultSetfromMovieCast(rs);

		System.out.println("Loading data from 'crew.csv' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadCrewDataSQL = "LOAD DATA LOCAL INFILE 'crew.csv' INTO TABLE MovieCrew FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,credit_id,person_id,job,name)";
		statement.execute(loadCrewDataSQL);
		System.out.println("Data succesfullly loaded into TABLE MovieCrew");
		rs = statement.executeQuery("SELECT * FROM MovieCrew");
		printResultSetfromMovieCrew(rs);
	}

	private static void menu() throws SQLException {
		ResultSet rs = null;

		PreparedStatement preparedstatement = null;
		Statement statement = null;

		System.out.println("Please enter the movie id to find all information of the movie: ");
		preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE movie_id = ?");
		Scanner sc = new Scanner(System.in);
		int x = sc.nextInt();
		preparedstatement.setInt(1,x);
		rs = preparedstatement.executeQuery();
		printResultSetfromMovie(rs);

		System.out.println("Look up movie by runtime range(x,y):");
		preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE runtime > ? AND runtime < ?");
		int min = sc.nextInt();
		int max = sc.nextInt();
		preparedstatement.setInt(1,min);
		preparedstatement.setInt(2,max);
		rs = preparedstatement.executeQuery();
		printResultSetfromMovie(rs);

		System.out.println("User Sign Up Form");
		preparedstatement = con.prepareStatement("INSERT INTO User(user_name, age, gender, registered_on) VALUES(?,?,?,?)");
		System.out.println("Username: ");
		String uname = sc.next();
		System.out.println("Age: (0-99)");
		int age = sc.nextInt();
		System.out.println("Gender: (F/M)");
		String gender = sc.next();

		preparedstatement.setString(1,uname);
		preparedstatement.setInt(2,age);
		preparedstatement.setString(3,gender);
		preparedstatement.setDate(4, new Date(System.currentTimeMillis()) );
		preparedstatement.executeUpdate();

		statement = con.createStatement();
		rs = statement.executeQuery("SELECT * FROM User");
		printResultSetfromUser(rs);

	}

	private static void printResultSetfromMovie(ResultSet rs) throws SQLException {

		System.out.println("movie_id" + "|" + "title" + "|" + "release_date" + "|" + "runtime" + "|" + "budget");
		while (rs.next()) {
			int movie_id = rs.getInt("movie_id");
			String title = rs.getString("title");
			String release_date = rs.getString("release_date");
			int runtime = rs.getInt("runtime");
			int budget = rs.getInt("budget");
			System.out.println(movie_id + "," + title + "," + release_date + "," + runtime + "," + budget);
		}
	}
	private static void printResultSetfromMovieCast(ResultSet rs) throws SQLException {

		System.out.println("movie_id" + "|" + "movie_character " + "|" + "credit_id " + "|" + "person_id " + "|" + "name");
		while (rs.next()) {
			int movie_id = rs.getInt("movie_id");
			String movie_character = rs.getString("movie_character");
			String credit_id = rs.getString("credit_id");
			int person_id  = rs.getInt("person_id ");
			String name = rs.getString("name");
			System.out.println(movie_id + "," + movie_character + "," + credit_id + "," + person_id  + "," + name);
		}
	}
	private static void printResultSetfromMovieCrew(ResultSet rs) throws SQLException {

		System.out.println("movie_id" + "|" + "credit_id " + "|" + "person_id " + "|" + "job " + "|" + "name");
		while (rs.next()) {
			int movie_id = rs.getInt("movie_id");
			String credit_id = rs.getString("credit_id");
			int person_id = rs.getInt("person_id");
			String job = rs.getString("job");
			String name = rs.getString("name");
			System.out.println(movie_id + "," + credit_id + "," + person_id + "," + job  + "," + name);
		}
	}

    private static void printResultSetfromUser(ResultSet rs) throws SQLException {
		System.out.println("\t" + "user_id" + "|" + "user_name" + "|" + "age" + "|" + "gender" + "|" + "registered_on");
			while (rs.next()) {
				int id = rs.getInt("user_id");
				String name = rs.getString("user_name");
				int age = rs.getInt("age");
				String gender = rs.getString("gender");
				Date date= rs.getDate("registered_on");
				System.out.println("\t" + id + "," + name + "," + age + "," + gender + "," + date);
			}
	}

	private static void callingStoredProcedure() throws SQLException {

		String createProcedure = "CREATE PROCEDURE doSomething() " + "BEGIN SELECT * FROM Students ; "
				+ "SELECT * FROM Students where age < 20 ; END";
		statement.executeUpdate(createProcedure);

		boolean hasResults = statement.execute("{CALL doSomething()}");
		do {
			if (hasResults) {
				ResultSet rs = statement.getResultSet();
				while (rs.next()) {
					System.out.print("id:" + rs.getInt("id"));
					System.out.print("name:" + rs.getString("name"));
					System.out.print("age:" + rs.getInt("age"));
				}
			}
			hasResults = statement.getMoreResults();
		} while (hasResults || statement.getUpdateCount() != -1);

	}

}