package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by glerman on 11/12/15.
 */
public class DBConnectionManager {

  public static Connection getConnection() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      return DriverManager.getConnection("jdbc:mysql://localhost/mtg?user=root&password=Qwer812$");
  }
}
