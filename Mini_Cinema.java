import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Mini_Cinema {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/";

	static final String USER = "root";
	static final String PASS = "";
	private static Connection con = null;
	private static Statement statement = null;
	private static PreparedStatement preparedstatement = null;
	private static CallableStatement callablestatement = null;
	private static ResultSet rs = null;
	public static CinemaPrinter printer = new CinemaPrinter();

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(JDBC_DRIVER);

			try {
				System.out.println("Connecting to database...");
				con = DriverManager.getConnection(DB_URL + "Mini_Cinema", USER, PASS);

			} catch (SQLException ce) {
				System.out.println("Connection Failed! Check output console");
				//ce.printStackTrace();

				createDatabase();
				createTable();
				loadDataIntoTable();
			}
			createTriggers();
			createStoredProcs();
			menu();

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
		String queryCreateTableMovie = "CREATE TABLE Movie( movie_id INT, title VARCHAR(100), release_date DATE, runtime INT, budget INT, PRIMARY KEY (movie_id))";statement.execute(queryCreateTableMovie);
		System.out.println("Movie table created successfully...");

		String queryDropTableMovieGenre = "DROP TABLE IF EXISTS MovieGenre";
		statement.execute(queryDropTableMovieGenre);
		String queryCreateTableMovieGenre = "CREATE TABLE MovieGenre( movie_id INT, genre VARCHAR(24), PRIMARY KEY (movie_id, genre), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE)";
		statement.execute(queryCreateTableMovieGenre);
		System.out.println("MovieGenre table created successfully...");

		String queryDropTableMovieCast = "DROP TABLE IF EXISTS MovieCast";
		statement.execute(queryDropTableMovieCast);
		String queryCreateTableMovieCast = "CREATE TABLE MovieCast( movie_id INT, movie_character VARCHAR(300), credit_id VARCHAR(24), person_id INT, name VARCHAR(60), PRIMARY KEY (credit_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableMovieCast);
		System.out.println("MovieCast table created successfully...");

		String queryDropTableMovieCrew = "DROP TABLE IF EXISTS MovieCrew";
		statement.execute(queryDropTableMovieCrew);
		String queryCreateTableMovieCrew = "CREATE TABLE MovieCrew( movie_id INT, credit_id VARCHAR(24), person_id INT, job VARCHAR(60), name VARCHAR(60), PRIMARY KEY (credit_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableMovieCrew);
		System.out.println("MovieCrew table created successfully...");

		String queryDropTableUser = "DROP TABLE IF EXISTS User";
		statement.execute(queryDropTableUser);
		String queryCreateTableUser = "CREATE TABLE User( user_id INT AUTO_INCREMENT, user_name VARCHAR(36), age INT, gender VARCHAR(12), registered_on DATE NOT NULL DEFAULT '1970-01-01', updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (user_id))";
		statement.execute(queryCreateTableUser);
		System.out.println("User table created successfully...");

		String queryDropTableWatch_List = "DROP TABLE IF EXISTS Watch_List";
		statement.execute(queryDropTableWatch_List);
		String queryCreateTableWatch_List = "CREATE TABLE Watch_List( user_id INT, movie_id INT, watch_order INT, added_on DATE NOT NULL DEFAULT '1970-01-01', updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (user_id, movie_id, added_on), FOREIGN KEY (user_id) REFERENCES User (user_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_List);
		System.out.println("Watch_List table created successfully...");

		String queryDropTableWatch_History = "DROP TABLE IF EXISTS Watch_History";
		statement.execute(queryDropTableWatch_History);
		String queryCreateTableWatch_History = "CREATE TABLE Watch_History( user_id INT, movie_id INT, rating INT, favorite BOOLEAN DEFAULT FALSE, watched_on DATE NOT NULL DEFAULT '1970-01-01', updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (user_id, movie_id), FOREIGN KEY (user_id) REFERENCES User (user_id), FOREIGN KEY (movie_id) REFERENCES Movie (movie_id))";
		statement.execute(queryCreateTableWatch_History);
		System.out.println("Watch_History table created successfully...");

		String queryDropTableArchive = "DROP TABLE IF EXISTS Archive";
		statement.execute(queryDropTableArchive);
		String queryCreateTableArchive = "CREATE TABLE Archive( user_id INT, movie_id INT, rating INT, favorite BOOLEAN DEFAULT FALSE, watched_on DATE DEFAULT '1970-01-01', updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (user_id, movie_id), FOREIGN KEY (user_id) REFERENCES User (user_id))";
		statement.execute(queryCreateTableArchive);
		System.out.println("Archive table created successfully...");
	}

	private static void loadDataIntoTable() throws SQLException {

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
	
	private static void createTriggers() throws SQLException {
		System.out.println("Connecting to database...");
		con = DriverManager.getConnection(DB_URL + "Mini_Cinema", USER, PASS);
		Statement statement = con.createStatement();
		
		String queryDropTriggerInsertWatchList = "DROP TRIGGER IF EXISTS InsertWatchList";
		statement.execute(queryDropTriggerInsertWatchList);
		String queryCreateTriggerInsertWatchList = 
				"CREATE TRIGGER InsertWatchList " + 
				"BEFORE INSERT ON Watch_List " + 
				"FOR EACH ROW " + 
				"BEGIN " + 
					"declare countList INT; " + 
					"select count(*) from Watch_List where user_id = NEW.user_id into countList; " + 
					"set NEW.watch_order = countList + 1; " + 
				"END;";

		statement.execute(queryCreateTriggerInsertWatchList);
		System.out.println("InsertWatchList trigger created successfully...");
		
		String queryDropTriggerInsertRatingConsistency = "DROP TRIGGER IF EXISTS InsertRatingConsistency";
		statement.execute(queryDropTriggerInsertRatingConsistency);
		String queryCreateTriggerInsertRatingConsistency = 
				"CREATE TRIGGER InsertRatingConsistency " + 
				"BEFORE INSERT ON Watch_History " + 
				"FOR EACH ROW " + 
				"BEGIN " + 
					"if (NEW.rating < 0) Then set NEW.rating = 0; " + 
					"elseif (NEW.rating > 10) Then set NEW.rating = 10; " + 
					"end if; " + 
				"END;";

		statement.execute(queryCreateTriggerInsertRatingConsistency);
		System.out.println("InsertRatingConsistency trigger created successfully...");
		
		String queryDropTriggerUpdateRatingConsistency = "DROP TRIGGER IF EXISTS UpdateRatingConsistency";
		statement.execute(queryDropTriggerUpdateRatingConsistency);
		String queryCreateTriggerUpdateRatingConsistency = 
				"CREATE TRIGGER UpdateRatingConsistency " + 
				"BEFORE UPDATE ON Watch_History " + 
				"FOR EACH ROW " + 
				"BEGIN " + 
					"if (NEW.rating < 0) Then set NEW.rating = 0; " + 
					"elseif (NEW.rating > 10) Then set NEW.rating = 10; " + 
					"end if; " + 
				"END;";

		statement.execute(queryCreateTriggerUpdateRatingConsistency);
		System.out.println("UpdateRatingConsistency trigger created successfully...");
		
	}
	
	private static void createStoredProcs() throws SQLException {
		System.out.println("Connecting to database...");
		con = DriverManager.getConnection(DB_URL + "Mini_Cinema", USER, PASS);
		Statement statement = con.createStatement();
		
		String queryDropProcedureDeleteFromWatchList = "DROP PROCEDURE IF EXISTS DeleteFromWatchList";
		statement.execute(queryDropProcedureDeleteFromWatchList);
		String queryCreateProcedureDeleteFromWatchList = 
				"CREATE PROCEDURE DeleteFromWatchList(IN uID INT, IN movID INT) " + 
				"BEGIN " + 
					"declare w_o INT; " + 
					"select watch_order from Watch_List where user_id = uID and movie_id = movID into w_o; " + 
					"delete from Watch_List where user_id = uID and movie_id = movID; " + 
					"update Watch_List set watch_order = watch_order - 1 where user_id = uID and watch_order > w_o; " +
				"END";

		statement.execute(queryCreateProcedureDeleteFromWatchList);
		System.out.println("DeleteFromWatchList procedure created successfully...");
		
		String queryDropProcedureArchiveWatchHistory = "DROP PROCEDURE IF EXISTS ArchiveWatchHistory";
		statement.execute(queryDropProcedureArchiveWatchHistory);
		String queryCreateProcedureArchiveWatchHistory = 
				"CREATE PROCEDURE ArchiveWatchHistory(IN lastUpdate TIMESTAMP) " + 
				"BEGIN " + 
					"insert into Archive (select * from Watch_History where updated_on < lastUpdate); " + 
					"delete from Watch_History where updated_on < lastUpdate; " +  
				"END";

		statement.execute(queryCreateProcedureArchiveWatchHistory);
		System.out.println("ArchiveWatchHistory procedure created successfully...");
		
	}

	private static void menu() throws SQLException {

		Scanner sc = new Scanner(System.in);
		int mID, uID, rating, runtime, budget, personID;
		String name, title, date, creditId, character, job, genre, timestamp;

		do {
			System.out.println("==================================================================");
			System.out.println(" 0. Quit");
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
			System.out.println("===========================Admin Only=============================");
			System.out.println("14. Add a new movie to Movie table");
			System.out.println("15. Add a new credit to MovieCast table");
			System.out.println("16. Add a new credit to MovieCrew table");
			System.out.println("17. Look up the most popular movies in all Users Watch Lists");
			System.out.println("18. Look up the most watched movies in all Users Watch History");
			System.out.println("19. Look up the number of movies in all Users Watch Lists, including Users that have nothing in their Watch List");
			System.out.println("20. Look up the activity in Users Watch Lists and Watch History on a certain date");
			System.out.println("21. Look up users that have the same favorite movies");
			System.out.println("22. Look up the X most recent movies in a given genre");
			System.out.println("23. Archive entries in Watch History older than a given timestamp");
			System.out.print("Please select one of the options above by its number: ");

			int option = sc.nextInt();
			sc.nextLine();
			switch (option) {
			case 0:
				return;
			case 1:
				System.out.println("1. User Sign Up Form");
				System.out.println("Username: ");
				String uname = sc.nextLine();
				System.out.println("Age(0-99):");
				int age = sc.nextInt();
				System.out.println("Gender(F/M):");
				String gender = sc.next();
				userSignUp(uname, age, gender);
				break;
			case 2:
				System.out.println("2. Please enter a movie ID to find all information of the movie: ");
				mID = sc.nextInt();
				movieByID(mID);
				break;
			case 3:
				System.out.println("3. Please enter a movie title to find all information of the movie: ");
				title = sc.nextLine();
				movieByTitle(title);
				break;
			case 4:
				System.out.println("4. Please enter X and Y to find movies with runtime in range(X,Y):");
				System.out.println("X:");
				int min = sc.nextInt();
				System.out.println("Y:");
				int max = sc.nextInt();
				moviesByRuntime(min, max);
				break;
			case 5:
				System.out.println("5. Find enter a year to find movies released within that year:");
				System.out.println("Year:");
				String year = sc.nextLine();
				moviesByYear(year);
				break;
			case 6:
				System.out.println("6. Please enter a number to find movies with this # of favorite and above: ");
				int fav = sc.nextInt();
				moviesByFavNum(fav);
				break;
			case 7:
				System.out.println("7. Please enter a number to find movies with this # of rating and above:  ");
				double average = sc.nextDouble();
				moviesByAvgRating(average);
				break;
			case 8:
				System.out.println("8. Look up movies by actor");
				System.out.println("Please enter the actor name: ");
				String actor = sc.nextLine();
				moviesByActor(actor);
				break;
			case 9:
				System.out.println("9. Add a movie to your Watch List");
				System.out.println("Add a movie to your Watch List");
				System.out.println("Please enter your user ID: ");
				uID = sc.nextInt();
				System.out.println("Please enter the movie ID: ");
				mID = sc.nextInt();
				addToWatchList(uID,mID);
				break;
			case 10:
				System.out.println("10. Add a movie to your Watch History");
				System.out.println("Please enter your user ID: ");
				uID = sc.nextInt();
				System.out.println("Please enter the movie ID: ");
				mID = sc.nextInt();
				addToWatchHistory(uID, mID);
				break;
			case 11:
				System.out.println("11. Remove a movie from your Watch List");
				System.out.println("Please enter your user ID: ");
				uID = sc.nextInt();
				System.out.println("Please enter the movie ID: ");
				mID = sc.nextInt();
				removeMovieFromWatchList( uID,  mID);
				break;
			case 12:
				System.out.println("12. Rate a movie you watched");
				System.out.println("Please enter your user ID: ");
				uID = sc.nextInt();
				System.out.println("Please enter the movie ID: ");
				mID = sc.nextInt();
				System.out.println("Please enter your rating: ");
				rating = sc.nextInt();
				rateMovie(rating, uID, mID);
				break;
			case 13:
				System.out.println("13. Mark a movie you watched as one of their 'Favorites'");
				System.out.println("Please enter your user ID: ");
				uID = sc.nextInt();
				System.out.println("Please enter the movie ID: ");
				mID = sc.nextInt();
				markAsFavorite(uID, mID);
				break;
			case 14:
				System.out.println("14. Add a new movie to Movie table");
				System.out.println("Please enter movie ID: ");
				mID = sc.nextInt();
				sc.nextLine();
				System.out.println("Please enter title: ");
				title = sc.nextLine();
				System.out.println("Please enter date: ");
				date = sc.nextLine();
				System.out.println("Please enter runtime: ");
				runtime = sc.nextInt();
				System.out.println("Please enter budget: ");
				budget = sc.nextInt();
				addNewMoview(mID, title, date, runtime, budget);
				break;
			case 15:
				System.out.println("15. Add a new credit to MovieCast table");
				System.out.println("Please enter a Movie ID: ");
				mID = sc.nextInt();
				sc.nextLine();
				System.out.println("Please enter his/her character: ");
				character = sc.nextLine();
				System.out.println("Please enter credit ID: ");
				creditId = sc.nextLine();
				System.out.println("Please enter person ID: ");
				personID = sc.nextInt();
				sc.nextLine();
				System.out.println("Please enter his/her name: ");
				name = sc.nextLine();
				addNewCast(mID, character, creditId,personID,name);
				break;
			case 16:
				System.out.println("16. Add a new credit to MovieCrew table");
				System.out.println("Please enter movie ID: ");
				mID = sc.nextInt();
				sc.nextLine();
				System.out.println("Please enter credit ID: ");
				creditId = sc.nextLine();
				System.out.println("Please enter person ID: ");
				personID = sc.nextInt();
				sc.nextLine();
				System.out.println("Please enter his/her job: ");
				job = sc.nextLine();
				System.out.println("Please enter his/her name: ");
				name = sc.nextLine();
				addNewCrew(mID, creditId, personID, job, name);
				break;
			case 17:
				System.out.println("17. Look up the most popular movies in all Users Watch Lists");
			    mostPopularInWatchList();
			    break;
			case 18:
				System.out.println("18. Look up the most watched movies in all Users Watch History");
			    mostWatchInWatchList();
			    break;
			case 19:
				System.out.println("19. Look up all users and the movies in their Watch Lists, including Users that have nothing in their Watch List");
				lookUserMovie();
				break;
			case 20:
				System.out.println("20. Look up the activity in Users Watch Lists and Watch History on a certain date");
				System.out.println("Please enter a date: ");
				date = sc.nextLine();
				userActivityOn(date);
				break;
			case 21:
				System.out.println("21. Look up users that have the same favorite movies");
				usersWithSameFav();
				break;
			case 22:
				System.out.println("22. Look up the X most recent movies in a given genre");
				System.out.println("Please enter a genre: ");
				genre = sc.nextLine();
				System.out.println("Please enter the number of movies you want to output: ");
				int x = sc.nextInt();
				recentWatchMovieIn(genre, x);
				break;
			case 23:
				System.out.println("23. Archive entries in Watch History older than a given timestamp");
				System.out.println("Please enter a timestamp in the format YYYY-MM-DD HH:MM:SS: ");
				timestamp = sc.nextLine();
				callArchiveProc(timestamp);
				break;
			default:
				break;
			}
			System.out.println("==================================================================");
		} while (true);

	}

	// 1
	private static void userSignUp(String uname, int age, String gender) {

		try {
			preparedstatement = con.prepareStatement("INSERT INTO User(user_name, age, gender, registered_on) VALUES(?,?,?,?)");
			preparedstatement.setString(1, uname);
			preparedstatement.setInt(2, age);
			preparedstatement.setString(3, gender);
			preparedstatement.setDate(4, new Date(System.currentTimeMillis()));
			preparedstatement.executeUpdate();
			statement = con.createStatement();
			ResultSet rs = statement.executeQuery("SELECT * FROM User");
			printer.printResultSetfromUser(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 2
	private static void movieByID(int id) {

		try {
			preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE movie_id = ?");
			preparedstatement.setInt(1, id);
			rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 3
	private static void movieByTitle(String title) {

		try {
			preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE title = ?");

			preparedstatement.setString(1, title);
			rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 4
	private static void moviesByRuntime(int min, int max) {

		try {
			preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE runtime BETWEEN ? AND ?");
			preparedstatement.setInt(1, min);
			preparedstatement.setInt(2, max);
			rs = preparedstatement.executeQuery();
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 5
	private static void moviesByYear(String year) {

		try {
			statement = con.createStatement();
			rs = statement.executeQuery("SELECT * FROM Movie WHERE release_date LIKE '" + year + "%'");
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 6
	private static void moviesByFavNum(int fav) {

		System.out.println("\nList of movies with at least " + fav + " favorites");
		try {
			preparedstatement = con.prepareStatement("select Movie.title, Movie.movie_id, count(favorite) favs "
					+ "from Movie, Watch_History where Movie.movie_id = Watch_History.movie_id and favorite = true group by movie_id having favs >= ?;");
			preparedstatement.setInt(1, fav);
			rs = preparedstatement.executeQuery();
			while (rs.next()) {
				String title = rs.getString("title");
				int mID = rs.getInt("movie_id");
				int mFav = rs.getInt("favs");
				System.out.printf("Title: %s | id: %d | favorites: %d\n", title, mID, mFav);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	// 7
	private static void moviesByAvgRating(double average) {

		System.out.println("\nList of movies with at least " + average + " average rating");
		try {
			preparedstatement = con.prepareStatement("select Movie.title, Movie.movie_id, avg(rating) avgRating "
					+ "from Movie, Watch_History where Movie.movie_id = Watch_History.movie_id group by movie_id having avgRating >= ?;");
			preparedstatement.setDouble(1, average);
			rs = preparedstatement.executeQuery();
			while (rs.next()) {
				String title = rs.getString("title");
				int mID = rs.getInt("movie_id");
				int avg = rs.getInt("avgRating");
				System.out.printf("Title: %s | id: %d | average rating: %d\n", title, mID, avg);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 8
	private static void moviesByActor(String actor) {

		System.out.println("\nList of movies with actor " + actor);
		try {
			preparedstatement = con.prepareStatement(
					"select Movie.title from Movie, MovieCast where Movie.movie_id = MovieCast.movie_id and name = ?;");
			preparedstatement.setString(1, actor);
			rs = preparedstatement.executeQuery();
			while (rs.next()) {
				String title = rs.getString("title");
				System.out.printf("Title: %s \n", title);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 9
	private static void addToWatchList(int uID, int mID) {

		try {
			preparedstatement = con.prepareStatement(
					"insert into Watch_List (user_id, movie_id, added_on) values (?, ?, ?);");
			java.sql.Timestamp date = new java.sql.Timestamp(new java.util.Date().getTime());
			preparedstatement.setInt(1, uID);
			preparedstatement.setInt(2, mID);
			preparedstatement.setTimestamp(3, date);
			preparedstatement.executeUpdate();
			
			preparedstatement = con.prepareStatement(
					"SELECT user_id, movie_id, watch_order FROM Watch_List where user_id = ?;");
			preparedstatement.setInt(1, uID);
			rs = preparedstatement.executeQuery();
			
			printer.printResultSetfromWatch_List(rs);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	// 10
	private static void addToWatchHistory(int uID, int mID) {

			try {
				preparedstatement = con.prepareStatement(
						"insert into Watch_History (user_id, movie_id, watched_on) values (?, ?, ?);");
				java.sql.Timestamp date = new java.sql.Timestamp(new java.util.Date().getTime());
				preparedstatement.setInt(1, uID);
				preparedstatement.setInt(2, mID);
				preparedstatement.setTimestamp(3, date);
				preparedstatement.executeUpdate();
				
				preparedstatement = con.prepareStatement(
						"SELECT user_id, movie_id, rating, favorite FROM Watch_History where user_id = ?;");
				preparedstatement.setInt(1, uID);
				rs = preparedstatement.executeQuery();
				
				printer.printResultSetfromWatch_History(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	// 11
	private static void removeMovieFromWatchList(int uID, int mID) {
			try {
				callablestatement = con.prepareCall("call DeleteFromWatchList(?, ?);");
				callablestatement.setInt(1, uID);
				callablestatement.setInt(2, mID);
				rs = callablestatement.executeQuery();
				
				preparedstatement = con.prepareStatement(
						"SELECT user_id, movie_id, watch_order FROM Watch_List where user_id = ?;");
				preparedstatement.setInt(1, uID);
				rs = preparedstatement.executeQuery();
				
				printer.printResultSetfromWatch_List(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	// 12
	private static void rateMovie(int rating, int uID, int mID) {
			try {
				preparedstatement = con.prepareStatement(
						"update Watch_History set rating = ? where user_id = ? and movie_id = ?;");
				preparedstatement.setInt(1, rating);
				preparedstatement.setInt(2, uID);
				preparedstatement.setInt(3, mID);
				preparedstatement.executeUpdate();
				
				preparedstatement = con.prepareStatement(
						"SELECT user_id, movie_id, rating, favorite FROM Watch_History where user_id = ?;");
				preparedstatement.setInt(1, uID);
				rs = preparedstatement.executeQuery();
				
				printer.printResultSetfromWatch_History(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}

	// 13
	private static void markAsFavorite(int uID, int mID) {

		try {
			preparedstatement = con.prepareStatement(
					"UPDATE Watch_History SET favorite = true WHERE user_id = ? AND movie_id = ?;");
			preparedstatement.setInt(1, uID);
			preparedstatement.setInt(2, mID);
			preparedstatement.executeUpdate();
			
			preparedstatement = con.prepareStatement(
					"SELECT user_id, movie_id, rating, favorite FROM Watch_History where user_id = ?;");
			preparedstatement.setInt(1, uID);
			rs = preparedstatement.executeQuery();
			
			printer.printResultSetfromWatch_History(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	// 14
	private static void addNewMoview(int mID, String title, String date, int runtime, int budget) {

		System.out.println("\nAdding  " + title + " to Movie table");
		try {
			preparedstatement = con.prepareStatement(
					"insert into Movie (movie_id, title, release_date, runtime, budget) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2, title);
			preparedstatement.setDate(3, Date.valueOf(date));
			preparedstatement.setInt(4, runtime);
			preparedstatement.setInt(5, budget);
			preparedstatement.executeUpdate();
			
			preparedstatement = con.prepareStatement("SELECT * FROM Movie WHERE movie_id = ?");
			preparedstatement.setInt(1, mID);
			rs = preparedstatement.executeQuery();
			
			printer.printResultSetfromMovie(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 15
	private static void addNewCast(int mID, String character, String creditId, int personID, String name) {

		System.out.println("\nAdding  credit " + creditId + " to MovieCast table");
		try {
			preparedstatement = con.prepareStatement(
					"insert into MovieCast (movie_id, movie_character, credit_id, person_id, name) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2, character);
			preparedstatement.setString(3, creditId);
			preparedstatement.setInt(4, personID);
			preparedstatement.setString(5, name);
			preparedstatement.executeUpdate();
			
			preparedstatement = con.prepareStatement("SELECT * FROM MovieCast WHERE credit_id = ?");
			preparedstatement.setString(1, creditId);
			rs = preparedstatement.executeQuery();
			
			printer.printResultSetfromMovieCast(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 16
	private static void addNewCrew(int mID, String creditId, int personID, String job, String name) {

		System.out.println("\nAdding  credit " + creditId + " to MovieCast table");
		try {
			preparedstatement = con.prepareStatement(
					"insert into MovieCrew (movie_id, credit_id, person_id, job, name) values (?, ?, ?, ?, ?);");
			preparedstatement.setInt(1, mID);
			preparedstatement.setString(2, creditId);
			preparedstatement.setInt(3, personID);
			preparedstatement.setString(4, job);
			preparedstatement.setString(5, name);
			preparedstatement.executeUpdate();
			
			preparedstatement = con.prepareStatement("SELECT * FROM MovieCrew WHERE credit_id = ?");
			preparedstatement.setString(1, creditId);
			rs = preparedstatement.executeQuery();
			
			printer.printResultSetfromMovieCrew(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 17
	private static void mostPopularInWatchList() {

		System.out.println("\nPopularity in users watch lists");
		try {
			preparedstatement = con.prepareStatement(
					"select movie_id, count(*) as popularity from Watch_List group by movie_id order by popularity desc;");
			rs = preparedstatement.executeQuery();
			while (rs.next()) {
				String mID = rs.getString("movie_id");
				int popularity = rs.getInt("popularity");
				System.out.printf("movie_ID: %s is in %d user's watch lists\n", mID, popularity);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 18
	private static void mostWatchInWatchList() {

		try {
			preparedstatement = con.prepareStatement(
					"select movie_id, count(*) as watches from Watch_History group by movie_id order by watches desc;");
			rs = preparedstatement.executeQuery();
			while (rs.next()) {
				String mID = rs.getString("movie_id");
				int watch = rs.getInt("watches");
				System.out.printf("movie_id: %s has been watched %d times\n", mID, watch);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 19
	private static void lookUserMovie() {

		try {
			preparedstatement = con.prepareStatement(
					"select U.user_id, U.user_name, L.movie_id from User U left outer join Watch_List L on U.user_id = L.user_id;");
			ResultSet rs = preparedstatement.executeQuery();
			System.out.println("user_id|user_name|movie_id");
			while (rs.next()) {
				int uID = rs.getInt("U.user_id");
				String uname = rs.getString("U.user_name");
				int mID = rs.getInt("L.movie_id");
				System.out.printf("%d,%s,%d\n", uID, uname, mID);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 20
	private static void userActivityOn(String date) {

		try {
			preparedstatement = con.prepareStatement(
					"(select user_id, movie_id, updated_on from Watch_List where DATE(updated_on) = ?) union (select user_id, movie_id, updated_on from Watch_History where DATE(updated_on) = ?);");
			preparedstatement.setString(1, date);
			preparedstatement.setString(2, date);
			rs = preparedstatement.executeQuery();
			
			System.out.println("user_id|movie_id|updated_on");
			while (rs.next()) {
				int uID = rs.getInt("user_id");
				int mID = rs.getInt("movie_id");
				Timestamp ts = rs.getTimestamp("updated_on");
				String formattedDate = new SimpleDateFormat("yyyy-MM-dd").format(ts);
				System.out.printf("%d,%d,%s\n", uID, mID, formattedDate);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 21
	private static void usersWithSameFav() {

		try {
			preparedstatement = con.prepareStatement(
					"select user_id, movie_id from Watch_History h1 where h1.favorite = TRUE and h1.movie_id in (select h2.movie_id from Watch_History h2 where h1.user_id <> h2.user_id and h2.favorite = TRUE) group by movie_id, user_id;");
			ResultSet rs = preparedstatement.executeQuery();
			System.out.println("user_id|movie_id");
			while (rs.next()) {
				int uID = rs.getInt("user_id");
				int mID = rs.getInt("movie_id");
				System.out.printf("%d,%d\n", uID, mID);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 22
	private static void recentWatchMovieIn(String genre, int x) {

		try {
			preparedstatement = con.prepareStatement("Select movie_id, title from Movie "
					+ "where movie_id in (select movie_id from MovieGenre where genre = ?) order by release_date desc LIMIT ?;");
			preparedstatement.setString(1, genre);
			preparedstatement.setInt(2, x);
			ResultSet rs = preparedstatement.executeQuery();
			
			System.out.println("movie_id" + "|" + "title");
			while (rs.next()) {
				int movie_id = rs.getInt("movie_id");
				String title = rs.getString("title");
				System.out.println(movie_id + "," + title);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	// 23
    private static void callArchiveProc(String ts) {
        
        System.out.println("\nMoving entries in Watch_History earlier than " + ts + " to Archive");
        
        try {
            callablestatement = con.prepareCall(
                    "{CALL ArchiveWatchHistory(?)}");
            callablestatement.setTimestamp(1, Timestamp.valueOf(ts));
            callablestatement.executeQuery();
            
            preparedstatement = con.prepareStatement(
            		"SELECT user_id, movie_id, rating, favorite FROM Archive;");
            rs = preparedstatement.executeQuery();
            printer.printResultSetfromArchive(rs);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }

}
