import db.DBConnectionManager;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DeckboxParserTest {

  @Test
  public void test() throws Exception {
    CardCache.init();
    initIndexTable();
    final DBDeckStore dbDeckStore = new DBDeckStore(DBConnectionManager.getConnection(), 2);
    final DeckboxParser deckboxParser = new DeckboxParser(dbDeckStore);

    deckboxParser.readAndStoreDecks();

  }

  private void initIndexTable() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    try (final Connection connection = DBConnectionManager.getConnection()) {
      try (final Statement statement = connection.createStatement()) {
        final String query = "INSERT INTO cdi_card_decks_index (cdi_card_id, cdi_mainboard_appearances, cdi_sideboard_appearances) " +
                "SELECT cd_id, '', '' FROM cd_card WHERE cd_id NOT IN (SELECT cdi_card_id FROM cdi_card_decks_index)";

        statement.execute(query);
      }

    }

  }
}