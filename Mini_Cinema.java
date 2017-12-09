import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Mini_Cinema{

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/";

	static final String USER = "root";
	static final String PASS = "0000";
	private static Connection con = null;
	private static Statement statement = null;
	public static CinemaPrinter printer = new CinemaPrinter();

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(JDBC_DRIVER);

			try {
				System.out.println("Connecting to database...");
				con = DriverManager.getConnection(DB_URL + "Mini_Cinema", USER, PASS);

			} catch (SQLException ce) {
				System.out.println("Connection Failed! Check output console");
				ce.printStackTrace();

				createDatabase();
				createTable();
				loadDataIntoTable();
			}

			menu();
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

		String queryDropTableMovieGenre = "DROP TABLE IF EXISTS MovieGenre";
		statement.execute(queryDropTableMovieGenre);
		String queryCreateTableMovieGenre = "CREATE TABLE MovieGenre( movie_id INT, genre VARCHAR(24), PRIMARY KEY (movie_id, genre), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE)";
		statement.execute(queryCreateTableMovieGenre);
		System.out.println("MovieGenre table created successfully...");

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
		String queryCreateTableUser = "CREATE TABLE User( user_id INT AUTO_INCREMENT, user_name VARCHAR(30),age INT,gender CHAR(20),registered_on DATE DEFAULT '1970-01-01',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id))";
		statement.execute(queryCreateTableUser);
		System.out.println("User table created successfully...");

		String queryDropTableWatch_List = "DROP TABLE IF EXISTS Watch_List";
		statement.execute(queryDropTableWatch_List);
		String queryCreateTableWatch_List = "CREATE TABLE Watch_List( user_id INT,movie_id INT, orders INT,added_on DATE DEFAULT '1970-01-01',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id, movie_id, added_on),FOREIGN KEY (user_id) REFERENCES User (user_id),FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_List);
		System.out.println("Watch_List table created successfully...");

		String queryDropTableWatch_History = "DROP TABLE IF EXISTS Watch_History";
		statement.execute(queryDropTableWatch_History);
		String queryCreateTableWatch_History = "CREATE TABLE Watch_History( user_id INT,movie_id INT,rating INT,favorite BOOLEAN DEFAULT FALSE,watched_on DATE DEFAULT '1970-01-01',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id, movie_id),FOREIGN KEY (user_id) REFERENCES User (user_id),FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_History);
		System.out.println("Watch_History table created successfully...");

		String queryDropTableArchive = "DROP TABLE IF EXISTS Archive";
		statement.execute(queryDropTableArchive);
		String queryCreateTableArchive = "CREATE TABLE Archive( user_id INT,movie_id INT,rating INT,favorite BOOLEAN DEFAULT FALSE,watched_on DATE DEFAULT '1970-01-01',updated_on TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (user_id, movie_id),FOREIGN KEY (user_id) REFERENCES User (user_id))";
		statement.execute(queryCreateTableArchive);
		System.out.println("Archive table created successfully...");
	}

	private static void loadDataIntoTable() throws SQLException {
		ResultSet rs;
		System.out.println("Loading data from 'movie.txt' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadMovieDataSQL = "LOAD DATA LOCAL INFILE 'movie.txt' INTO TABLE Movie FIELDS TERMINATED BY '%' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,title,release_date,runtime,budget)";
		statement.execute(loadMovieDataSQL);
		System.out.println("Data succesfullly loaded into TABLE Movie");
		rs = statement.executeQuery("SELECT * FROM Movie LIMIT 10");
		printer.printResultSetfromMovie(rs);

		System.out.println("Loading data from 'genre.txt' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadGenreDataSQL = "LOAD DATA LOCAL INFILE 'genre.txt' INTO TABLE MovieGenre FIELDS TERMINATED BY '%' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,genre)";
		statement.execute(loadGenreDataSQL);
		System.out.println("Data succesfullly loaded into TABLE MovieGenre");
		rs = statement.executeQuery("SELECT * FROM MovieGenre LIMIT 10");
		printer.printResultSetfromMovieGenre(rs);

		System.out.println("Loading data from 'cast.txt' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadCastDataSQL = "LOAD DATA LOCAL INFILE 'cast.txt' INTO TABLE MovieCast FIELDS TERMINATED BY '%' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,movie_character,credit_id,person_id,name)";
		statement.execute(loadCastDataSQL);
		System.out.println("Data succesfullly loaded into TABLE MovieCast");
		rs = statement.executeQuery("SELECT * FROM MovieCast LIMIT 10");
		printer.printResultSetfromMovieCast(rs);

		System.out.println("Loading data from 'crew.txt' file. Please wait for a moment.");
		statement = con.createStatement();
		String loadCrewDataSQL = "LOAD DATA LOCAL INFILE 'crew.txt' INTO TABLE MovieCrew FIELDS TERMINATED BY '%' LINES TERMINATED BY '\n' IGNORE 1 LINES (movie_id,credit_id,person_id,job,name)";
		statement.execute(loadCrewDataSQL);
		System.out.println("Data succesfullly loaded into TABLE MovieCrew");
		rs = statement.executeQuery("SELECT * FROM MovieCrew LIMIT 10");
		printer.printResultSetfromMovieCrew(rs);
	}

	private static void menu() throws SQLException {

		ResultSet rs = null;
		PreparedStatement preparedstatement = null;
		Statement statement = null;
		Scanner sc = new Scanner(System.in);

		do {

			System.out.println("==================================================================");
			System.out.println(" 1. Sign up for Mini Cinema");
			System.out.println(" 2. Look up a movie by Movie ID");
			System.out.println(" 3. Look up a movie by Movie title");
			System.out.println(" 4. Find movies within a runtime range");
			System.out.println(" 5. Find movies within a release date range");
			System.out.println(" 6. Look up movies by number of favorites");
			System.out.println(" 7. Look up movies by average rating");
			System.out.println(" 8. Look up movies by actor");
			System.out.println(" 9. Add a movie to your Watch List");
			System.out.println("10. Add a movie to your Watch History");
			System.out.println("11. Remove a movie from your Watch List");
			System.out.println("12. Rate a movie you watched");
			System.out.println("13. Mark a movie you watched as one of their 'Favorites'");

			System.out.println("14. Add a new movie to Movie table");
			System.out.println("15. Add a new credit to MovieCast table");
			System.out.println("16. Add a new credit to MovieCrew table");
			System.out.println("17. Look up the most popular movies in all Users Watch Lists");
			System.out.println("18. Look up the most watched movies in all Users Watch History");
			System.out.println("19. Look up the number of movies in all Users Watch Lists, including Users that have nothing in their Watch List");
			System.out.println("20. Look up the activity in Users Watch Lists and Watch History on a certain date");

			System.out.print("Please select one of the options above by its number: ");

			int option = sc.nextInt();
			sc.nextLine();
			switch (option){
				case 1:
					System.out.println("1. User Sign Up Form");
					preparedstatement = con.prepareStatement("INSERT INTO User(user_name, age, gender, registered_on) VALUES(?,?,?,?)");
					System.out.println("Username: ");
					String uname = sc.nextLine();
					System.out.println("Age(0-99):");
					int age = sc.nextInt();
					System.out.println("Gender(F/M):");
					String gender = sc.next();
					preparedstatement.setString(1,uname);
					preparedstatement.setInt(2,age);
					preparedstatement.setString(3,gender);
					preparedstatement.setDate(4, new Date(System.currentTimeMillis()) );
					preparedstatement.executeUpdate();
					statement = con.createStatement();
					rs = statement.executeQuery("SELECT * FROM User");
					printer.printResultSetfromUser(rs);
					break;
				case 2:
					System.out.println("2. Please enter a movie ID to find all information of the movie: ");
					preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE movie_id = ?");
					int id = sc.nextInt();
					preparedstatement.setInt(1,id);
					rs = preparedstatement.executeQuery();
					printer.printResultSetfromMovie(rs);
					break;

				case 3:
					System.out.println("3. Please enter a movie title to find all information of the movie: ");
					preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE title = ?");
					String title = sc.nextLine();
					preparedstatement.setString(1,title);
					rs = preparedstatement.executeQuery();
					printer.printResultSetfromMovie(rs);
				case 4:
					System.out.println("4. Please enter X and Y to find movies with runtime in range(X,Y):");
					preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE runtime > ? AND runtime < ?");
					System.out.println("X:");
					int min = sc.nextInt();
					System.out.println("Y:");
					int max = sc.nextInt();
					preparedstatement.setInt(1,min);
					preparedstatement.setInt(2,max);
					rs = preparedstatement.executeQuery();
					printer.printResultSetfromMovie(rs);
					break;
				case 5:
					System.out.println("5. Find enter a year to find movies released within that year:");
					System.out.println("Year:");
					String year = sc.nextLine();
					statement = con.createStatement();
					rs = statement.executeQuery("SELECT * FROM Movie WHERE release_date LIKE '"  + year + "%'");
					printer.printResultSetfromMovie(rs);
					break;
				case 6:
					System.out.println("6. Please enter a number to find movies with this # of favorite and above: ");
					int fav = sc.nextInt();
					moviesByFavNum(fav);
				case 7:
					System.out.println("7. Please enter a number to find movies with this # of rating and above:  ");
					double average = sc.nextDouble();
					moviesByAvgRating(average);
				case 8:
					System.out.println("8. Please enter the actor name: ");
					String actor = sc.nextLine();
					moviesByActor(actor);
				case 9:
					System.out.println("8. Please enter the actor name: ");

					//addToWatchList(uID,mID,title);
				case 10:
				case 11:
				case 12:
				case 13:
			}
			System.out.println("==================================================================");
		}while (true);

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

	private static void moviesByFavNum(int fav){
		PreparedStatement preparedstatement = null;
		System.out.println("\nList of movies with at least " + fav + " favorites");
		try {
			preparedstatement = con.prepareStatement("select Movie.title, Movie.movie_id, count(favorite) favs " +
                    "from Movie, Watch_History where Movie.movie_id = Watch_History.movie_id and favorite = true group by movie_id having favs > ?;");
			preparedstatement.setInt(1, fav);
			ResultSet rs = preparedstatement.executeQuery();
			while(rs.next()){
				String title = rs.getString("title");
				int mID = rs.getInt("movie_id");
				int mFav = rs.getInt("favs");
				System.out.printf("Title: %s | id: %d | favorites: %d\n", title, mID, mFav);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static void moviesByAvgRating(double average){
		PreparedStatement preparedstatement = null;
		System.out.println("\nList of movies with at least " + average + " average rating");
		try {
			preparedstatement = con.prepareStatement("select Movie.title, Movie.movie_id, avg(rating) avgRating " +
					"from Movie, Watch_History where Movie.movie_id = Watch_History.movie_id group by movie_id having avgRating > ?;");
			preparedstatement.setDouble(1, average);
			ResultSet rs = preparedstatement.executeQuery();
			while(rs.next()){
				String title = rs.getString("title");
				int mID = rs.getInt("movie_id");
				int avg = rs.getInt("avgRating");
				System.out.printf("Title: %s | id: %d | average rating: %d\n", title, mID, avg);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void moviesByActor(String actor){
		PreparedStatement preparedstatement = null;
		System.out.println("\nList of movies with actor " + actor);
		try {
			preparedstatement = con.prepareStatement("select Movie.title from Movie, MovieCast where Movie.movie_id = MovieCast.movie_id and name = ?;");
			preparedstatement.setString(1, actor);
			ResultSet rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void addToWatchList(int uID, int mID, String title){
		PreparedStatement preparedstatement = null;
		System.out.println("\nAdding  " + title + " to Watch List");
		try {
			preparedstatement = con.prepareStatement("insert into Watch_List (user_id, movie_id, title, added_on) values (?, ?, ?, ?);");
			java.sql.Timestamp date = new java.sql.Timestamp(new java.util.Date().getTime());
			preparedstatement.setInt(1, uID);
			preparedstatement.setInt(2, mID);
			preparedstatement.setString(3, title);
			preparedstatement.setTimestamp(4, date);
			ResultSet rs = preparedstatement.executeQuery();
			printer.printResultSetfromWatch_List(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//14
	private static void addNewMoview(int mID, String title, String date, int runtime, int budget){
		PreparedStatement preparedstatement = null;
		System.out.println("\nAdding  " + title + " to Movie table");
		try {
			preparedstatement = con.prepareStatement("insert into Movie (movie_id, title, release_date, runtime, budget) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2, title);
			preparedstatement.setDate(3, Date.valueOf(date));
			preparedstatement.setInt(4, runtime);
			preparedstatement.setInt(5, budget);
			ResultSet rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//15
	private static void addNewCast(int mID, String character, String creditId, int personID, String name){
		PreparedStatement preparedstatement = null;
		System.out.println("\nAdding  credit " + creditId + " to MovieCast table");
		try {
			preparedstatement = con.prepareStatement("insert into MovieCast (movie_id, movie_character, credit_id, person_id, name) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2, character);
			preparedstatement.setString(3,creditId);
			preparedstatement.setInt(4, personID);
			preparedstatement.setString(5, name);
			ResultSet rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovieCast(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//16
	private static void addNewCrew(int mID, String creditId, int personID, String job, String name){
		PreparedStatement preparedstatement = null;
		System.out.println("\nAdding  credit " + creditId + " to MovieCast table");
		try {
			preparedstatement = con.prepareStatement("insert into MovieCrew (movie_id, credit_id, person_id, job, name) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2,creditId);
			preparedstatement.setInt(3, personID);
			preparedstatement.setString(4, job);
			preparedstatement.setString(5, name);
			ResultSet rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovieCrew(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//17
	private static void mostPopularInWatchList(){
		PreparedStatement preparedstatement = null;
		System.out.println("\nPopularity in users watch lists");
		try {
			preparedstatement = con.prepareStatement("select movie_id, count(*) as popularity from Watch_List group by movie_id order by popularity desc;");
			ResultSet rs = preparedstatement.executeQuery();
			while(rs.next()){
				String mID = rs.getString("movie_id");
				int popularity = rs.getInt("popularity");
				System.out.printf("Movie %d has %d popularity\n", mID, popularity);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//18
	private static void mostWatchInWatchList(){
		PreparedStatement preparedstatement = null;
		System.out.println("\nMost watch in users watch lists");
		try {
			preparedstatement = con.prepareStatement("sselect movie_id, count(*) as watches from Watch_History group by movie_id order by watches desc;");
			ResultSet rs = preparedstatement.executeQuery();
			while(rs.next()){
				String mID = rs.getString("movie_id");
				int watch = rs.getInt("popularity");
				System.out.printf("Movie %d has %d number of watch\n", mID, watch);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}