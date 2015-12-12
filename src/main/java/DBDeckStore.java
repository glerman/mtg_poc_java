import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by glerman on 11/12/15.
 */
public class DBDeckStore implements AutoCloseable {

  private static final String INSTANCES_IN_DECK_SEPARATOR = "x";

  private final Connection connection;
  private final ArrayList<Deck> decksBlock;
  private final int blockSize;
  private int blocksStored;

  public DBDeckStore(final Connection connection, final int blockSize) {
    this.connection = connection;
    this.blockSize = blockSize;

    decksBlock = new ArrayList<>(blockSize);
    blocksStored = 0;
  }

  public void addDeck(final Deck deck) throws SQLException, IllegalAccessException, ClassNotFoundException, InstantiationException {
    decksBlock.add(deck);
    if (decksBlock.size() == blockSize) {
      storeDecks(decksBlock);
      decksBlock.clear();
    }
  }

  public void storeDecks(final List<Deck> decks) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    if (decks.isEmpty()) {
      return;
    }
    insertDecksAndAddIds(decks);
    updateCardToDecks(decks);
  }



  private static class CardApearancesInDecks {
    final List<Appearance> mainboardApearences;
    final List<Appearance> sideboardApearences;

    private CardApearancesInDecks(final Appearance mainboardApearence, final Appearance sideboardApearence) {
      this.mainboardApearences = mainboardApearence != null ? Lists.newArrayList(mainboardApearence) : Lists.newArrayList();
      this.sideboardApearences = sideboardApearence != null ? Lists.newArrayList(sideboardApearence) : Lists.newArrayList();
    }

    public String getMainboardAppearancesString() {
      return getAppearancesString(mainboardApearences);
    }

    public String getSideboardAppearancesString() {
      return getAppearancesString(sideboardApearences);
    }

    private String getAppearancesString(final List<Appearance> appearances) {
      if (appearances == null || appearances.isEmpty()) {
        return "";
      }
      final StringBuilder sb = new StringBuilder();
      for (final Appearance appearance : appearances) {
        sb.append(appearance.instances).append(INSTANCES_IN_DECK_SEPARATOR).append(appearance.deckDBId).append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      return sb.toString();
    }
  }

  private static class Appearance {
    final int deckDBId;
    final int instances;

    private Appearance(final int deckDBId, final int instances) {
      this.deckDBId = deckDBId;
      this.instances = instances;
    }
  }

  private void updateCardToDecks(final List<Deck> decks) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    final Map<Integer, CardApearancesInDecks> cardDBIdToNewApearances = createCardToNewApearances(decks);

    try (final Statement statement = connection.createStatement()) {
    for (final Map.Entry<Integer, CardApearancesInDecks> entry : cardDBIdToNewApearances.entrySet()) {
        final Integer cardDBId = entry.getKey();
        final CardApearancesInDecks newAppearances = entry.getValue();

        final String updateMainboard = String.format("UPDATE cdi_card_decks_index SET cdi_mainboard_appearances = CONCAT(IF(cdi_mainboard_appearances IS NULL, '', cdi_mainboard_appearances), \"%s\") WHERE cdi_card_id=%d", newAppearances.getMainboardAppearancesString(), cardDBId);
        statement.addBatch(updateMainboard);
        if (newAppearances.sideboardApearences != null && newAppearances.sideboardApearences.size() > 0) {
          final String updateSideboard = String.format("UPDATE cdi_card_decks_index SET cdi_sideboard_appearances = CONCAT(IF(cdi_sideboard_appearances IS NULL, '', cdi_sideboard_appearances), \"%s\") WHERE cdi_card_id=%d", newAppearances.getSideboardAppearancesString(), cardDBId);
          statement.addBatch(updateSideboard);
        }
      }
      final int[] updatedRows = statement.executeBatch();
    }
  }

  private Map<Integer, CardApearancesInDecks> createCardToNewApearances(final List<Deck> decks) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    final Map<Integer, CardApearancesInDecks> cardToNewApearances = Maps.newHashMap();
    for (final Deck deck : decks) {
      for(final DeckCard mainBoardCard : deck.mainBoard) {
        final int cardDBId = CardCache.getIdFor(mainBoardCard.card.name);
        final CardApearancesInDecks cardApearences = cardToNewApearances.get(cardDBId);
        final Appearance mainboardApearence = new Appearance(deck.dbId, mainBoardCard.cardCount);
        if (cardApearences != null) {
          cardApearences.mainboardApearences.add(mainboardApearence);
        } else {
          cardToNewApearances.put(cardDBId, new CardApearancesInDecks(mainboardApearence, null));
        }
      }
      if (deck.sideBoard == null) {
        continue;
      }
      for(final DeckCard sideBoardCard : deck.sideBoard) {
        final int cardDBId = CardCache.getIdFor(sideBoardCard.card.name);
        final CardApearancesInDecks cardApearences = cardToNewApearances.get(cardDBId);
        final Appearance sideboardApearence = new Appearance(deck.dbId, sideBoardCard.cardCount);
        if (cardApearences != null) {
          cardApearences.sideboardApearences.add(sideboardApearence);
        } else {
          cardToNewApearances.put(cardDBId, new CardApearancesInDecks(null, sideboardApearence));
        }
      }
    }
    return cardToNewApearances;
  }

  public void insertDecksAndAddIds(final List<Deck> decks) throws SQLException {
    final StringBuilder queryBuilder = new StringBuilder("INSERT INTO dk_deck (dk_name, dk_origin_url, dk_mainboard, dk_sideboard) VALUES ");

    for (final Deck deck : decks) {
      queryBuilder.append("(").append(deck.sqlFormat()).append("),");
    }
    queryBuilder.deleteCharAt(queryBuilder.length() - 1); // Deleting last comma
    final String query = queryBuilder.toString();
    try (final Statement stmt = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
      blocksStored++;
      if (shouldLogStats()) {
        System.out.println("Started writing block at: " + LocalDateTime.now());
      }

      final int rowsAffected = stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
      if (rowsAffected != decks.size()) {
        throw new RuntimeException("Attempted to insert: " + decks.size() + " decks, rows affected: " + rowsAffected);
      }
      try (final ResultSet generatedKeys = stmt.getGeneratedKeys()) {
        final Iterator<Deck> deckIterator = decks.iterator();
        while (generatedKeys.next()) {
          if (!deckIterator.hasNext()) {
            throw new RuntimeException("Something went wrong in deck insertion or id assignment");
          }
          deckIterator.next().setDbId(generatedKeys.getInt(1));
        }
      }
    } catch (final SQLException e) {
      System.out.println("Failed to insert. Error: " + e.getMessage() + " query: " + query);
      throw e;
    } finally {
      if (shouldLogStats()) {
        System.out.println("Finished writing block at: " + LocalDateTime.now());
      }
    }
  }

  public boolean shouldLogStats() {
    return blocksStored % 10 == 0;
  }

  @Override
  public void close() throws Exception {
    storeDecks(decksBlock);
    connection.close();
  }
}
