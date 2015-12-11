import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by glerman on 3/12/15.
 */
public class CardCache {

  private static final Map<String, Integer> cardNameToId = new HashMap<String, Integer>(16 * 1000);
  private static final CloseableHttpClient client = HttpClients.createDefault();
  private static boolean initialized = false;

  public static Integer getIdFor(final String cardName) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    if (!initialized) {
      init();
    }
    return cardNameToId.get(cardName);
  }

  static Map<String, Integer> get() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    init();
    return cardNameToId;
  }

  public static void init() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    ResultSet resultSet = null;
    try (final Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?user=root&password=Qwer812$")) {
      final Statement statement = conn.createStatement();
      resultSet = statement.executeQuery("SELECT cd_id, cd_name FROM cd_card");

      while (resultSet.next()) {
        final int cardId = resultSet.getInt(1);
        final String cardName = resultSet.getString(3);
        cardNameToId.put(cardName, cardId);
      }
      initialized = true;
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  public static String fetch(final String uri) throws IOException {
    HttpGet httpget = new HttpGet(uri);
    try (final CloseableHttpResponse response = client.execute(httpget)) {
      final HttpEntity entity = response.getEntity();
      return inputStreamToString(entity.getContent());
    }
  }

  private static String inputStreamToString(final InputStream is) throws IOException {
      StringWriter sw = new StringWriter();
      IOUtils.copy(is, sw);
      return sw.toString();
  }
}
