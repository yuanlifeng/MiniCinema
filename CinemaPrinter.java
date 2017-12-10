import java.sql.*;

public class CinemaPrinter {

    public void printResultSetfromMovie(ResultSet rs) throws SQLException {

        System.out.println("movie_id" + "|" + "title" + "|" + "release_date" + "|" + "runtime" + "|" + "budget");
        while (rs.next()) {
            int movie_id = rs.getInt("movie_id");
            String title = rs.getString("title");
            Date release_date = (Date) rs.getDate("release_date");
            int runtime = rs.getInt("runtime");
            int budget = rs.getInt("budget");
            System.out.println(movie_id + "," + title + "," + release_date + "," + runtime + "," + budget);
        }
    }

    public void printResultSetfromMovieGenre(ResultSet rs) throws SQLException {

        System.out.println("movie_id" + "|" + "genre");
        while (rs.next()) {
            int movie_id = rs.getInt("movie_id");
            String genre = rs.getString("genre");
            System.out.println(movie_id + "," + genre);
        }
    }

    public void printResultSetfromMovieCast(ResultSet rs) throws SQLException {

        System.out.println("movie_id" + "|" + "movie_character " + "|" + "credit_id " + "|" + "person_id " + "|" + "name");
        while (rs.next()) {
            int movie_id = rs.getInt("movie_id");
            String movie_character = rs.getString("movie_character");
            String credit_id = rs.getString("credit_id");
            int person_id = rs.getInt("person_id");
            String name = rs.getString("name");
            System.out.println(movie_id + "," + movie_character + "," + credit_id + "," + person_id + "," + name);
        }
    }

    public void printResultSetfromMovieCrew(ResultSet rs) throws SQLException {

        System.out.println("movie_id" + "|" + "credit_id " + "|" + "person_id " + "|" + "job " + "|" + "name");
        while (rs.next()) {
            int movie_id = rs.getInt("movie_id");
            String credit_id = rs.getString("credit_id");
            int person_id = rs.getInt("person_id");
            String job = rs.getString("job");
            String name = rs.getString("name");
            System.out.println(movie_id + "," + credit_id + "," + person_id + "," + job + "," + name);
        }
    }

	    public void printResultSetfromUser(ResultSet rs) throws SQLException {
			System.out.println("user_id" + "|" + "user_name" + "|" + "age" + "|" + "gender" + "|" + "registered_on");
				while (rs.next()) {
					int id = rs.getInt("user_id");
					String name = rs.getString("user_name");
					int age = rs.getInt("age");
					String gender = rs.getString("gender");
					Date date = rs.getDate("registered_on");
					System.out.println(id + "," + name + "," + age + "," + gender + "," + date);
				}
		}
	    public void printResultSetfromWatch_List(ResultSet rs) throws SQLException {
			System.out.println("user_id" + "|" + "movie_id" + "|" + "watch_order");
				while (rs.next()) {
					int user_id = rs.getInt("user_id");
					int movie_id = rs.getInt("movie_id");
					int watch_order = rs.getInt("watch_order");
					System.out.println(user_id + "," + movie_id + "," + watch_order);
				}
		}
		public void printResultSetfromWatch_History(ResultSet rs) throws SQLException {
				System.out.println("user_id" + "|" + "movie_id" + "|" + "rating" + "|" + "favorite");
					while (rs.next()) {
						int user_id = rs.getInt("user_id");
						int movie_id = rs.getInt("movie_id");
						int rating  = rs.getInt("rating");
						boolean favorite = rs.getBoolean("favorite");
						System.out.println(user_id + "," + movie_id + "," + rating + "," + favorite);
					}
		}
	}