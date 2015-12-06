import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class CardCacheTest {

  private final static int blockSize = 1000;
  private final static String cardsApiUri = "http://api.mtgapi.com/v2/cards?page=";
  private final static String multiverseRecoveryBaseUri = "http://api.mtgapi.com/v1/card/name/";

  @Test
  public void testInsert() throws Exception {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    final Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?" +
            "user=root&password=Qwer812$");


    final Statement stmt = conn.createStatement();
    final boolean result = stmt.execute("INSERT INTO cd_card (cd_multiverse_id, cd_name) VALUE (15, 'gal')");
    System.out.println(result);
  }

  @Test
  public void test() throws Exception {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    final Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?" +
            "user=root&password=Qwer812$");
    int lastPage, currPage = 1;
    final Map<String, Integer> cardBlock = Maps.newHashMap();
    final List<String> cardNamesWithoutMultiverseId = Lists.newArrayList();
    do {
      try {
        final String uri = cardsApiUri + currPage;
        final String json = CardCache.fetch(uri);
        final JSONObject jsonObject = new JSONObject(json);
        lastPage = Integer.valueOf(jsonObject.getJSONObject("links").getString("last").split("=")[1]);
        final JSONArray cards = jsonObject.getJSONArray("cards");

        for (final Object cardObj : cards) {
          final JSONObject card = (JSONObject) cardObj;
          final String cardName = card.getString("name");
          final int multiverseId = card.getInt("multiverseid");
          if (multiverseId == 0) {
            cardNamesWithoutMultiverseId.add(cardName);
          } else {
            cardBlock.put(cardName, multiverseId);
          }
        }
        if (cardBlock.size() >= blockSize || currPage == lastPage) {
          writeBlockToDB(cardBlock, conn);
        }
        currPage++;
      } catch (final Exception e) {
        System.out.println("Exception on page " + currPage + ": " + e.toString());
        throw e;
      }
    } while (currPage <= lastPage);
    System.out.println(lastPage);
    System.out.println(cardNamesWithoutMultiverseId);
  }


  private void writeBlockToDB(final Map<String, Integer> cardBlock, final Connection conn) throws SQLException {
    final StringBuilder sb = new StringBuilder("INSERT INTO cd_card (cd_multiverse_id, cd_name) VALUES ");

    for(final Map.Entry<String, Integer> card : cardBlock.entrySet()) {
      final String name = card.getKey().replace("'", ""); //TODO: removing the ' char, keep that in mind when comparing card names
      final Integer multiverseId = card.getValue();
      sb.append("(").append(multiverseId).append(",'").append(name).append("'),");
    }
    cardBlock.clear();
    sb.deleteCharAt(sb.length() - 1);
    final String query = sb.toString();

    try (final Statement stmt = conn.createStatement()) {
      stmt.execute(query);
    } catch (final Exception e) {
      System.out.println("Failed to insert. Error: " + e.getMessage() + " query: " + query);
      throw e;
    }
  }
}