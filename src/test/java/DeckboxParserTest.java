import db.DBConnectionManager;
import org.junit.Test;

public class DeckboxParserTest {

  @Test
  public void test() throws Exception {
    final DBDeckStore dbDeckStore = new DBDeckStore(DBConnectionManager.getConnection(), 1000);
    final DeckboxParser deckboxParser = new DeckboxParser(dbDeckStore);

    deckboxParser.readAndStoreDecks();

  }
}