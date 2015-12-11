import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by glerman on 11/12/15.
 */
public class DBDeckStore implements AutoCloseable {

  private final String INSTANCES_IN_DECK_SEPARATOR = "x";
  Map<String, Integer> deckCache; //TODO: create this cache, using url is costly

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
    }
  }

  public void storeDecks(final List<Deck> decks) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    if (decks.isEmpty()) {
      return;
    }
    insertDecks(decks);
    updateCardToDecks(decks);
  }



  private static class CardApearancesInDecks {
    final List<Appearance> mainboardApearences;
    final List<Appearance> sideboardApearences;

    private CardApearancesInDecks(final Appearance mainboardApearence, final Appearance sideboardApearence) {
      this.mainboardApearences = mainboardApearence != null ? Lists.newArrayList(mainboardApearence) : Lists.newArrayList();
      this.sideboardApearences = sideboardApearence != null ? Lists.newArrayList(sideboardApearence) : Lists.newArrayList();
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
    final Set<Integer> cardDBIds = cardDBIdToNewApearances.keySet();


    //TODO: use update with concat instead of get, append and store
    try (final ResultSet resultSet = getAppearancesFromDB(cardDBIds)) {
      while(resultSet.next()) {
        final int cardDBId = resultSet.getInt(1);
        final String mainboardAppearances = resultSet.getString(2);
        final String sideboardAppearances = resultSet.getString(3);

        final StringBuilder updatedMainboardAppearances = new StringBuilder(mainboardAppearances);
        final StringBuilder updatedSideboardAppearances = new StringBuilder(sideboardAppearances);

        final CardApearancesInDecks cardApearancesInDecks = cardDBIdToNewApearances.get(cardDBId);
        addAppearances(updatedMainboardAppearances, cardApearancesInDecks.mainboardApearences);
        addAppearances(updatedSideboardAppearances, cardApearancesInDecks.sideboardApearences);
      }
    }

  }

  private void addAppearances(final StringBuilder updatedMainboardAppearances, final List<Appearance> newAppearances) {
    for (final Appearance appearance : newAppearances) {
      updatedMainboardAppearances.append(appearance.instances).append(INSTANCES_IN_DECK_SEPARATOR).append(appearance.deckDBId).append(",");
    }
  }

  private ResultSet getAppearancesFromDB(final Set<Integer> cardDBIds) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    final StringBuilder selectBuilder = new StringBuilder("SELECT * FROM cdi_card_decks_index WHERE cdi_card_id IN ");
    selectBuilder.append("(");
    for (final Integer cardDBId : cardDBIds) {
      selectBuilder.append(cardDBId).append(",");
    }
    selectBuilder.deleteCharAt(selectBuilder.length() - 1);// Remove last comma
    selectBuilder.append(")");

    final String selectQuery = selectBuilder.toString();

    ResultSet resultSet;
    try (final Statement stmt = connection.createStatement()) {
      resultSet = stmt.executeQuery(selectQuery);
    } catch (final SQLException e) {
      System.out.println("Failed to insert. Error: " + e.getMessage() + " query: " + selectQuery);
      throw e;
    }
    return resultSet;
  }

  private Map<Integer, CardApearancesInDecks> createCardToNewApearances(final List<Deck> decks) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    final Map<Integer, CardApearancesInDecks> cardToNewApearances = Maps.newHashMap();
    for (final Deck deck : decks) {
      for(final DeckCard mainBoardCard : deck.mainBoard) {
        final CardApearancesInDecks cardApearences = cardToNewApearances.get(mainBoardCard);
        final Appearance mainboardApearence = getAppearance(deck, mainBoardCard);
        if (cardApearences != null) {
          cardApearences.mainboardApearences.add(mainboardApearence);
        } else {
          final String name = mainBoardCard.card.name;
          final Integer cardDBId = CardCache.getIdFor(name);
          if (cardDBId == null) {
            System.out.println("No DB id for card name: " + name);
            continue;
          }
          cardToNewApearances.put(cardDBId, new CardApearancesInDecks(mainboardApearence, null));
        }
      }
      for(final DeckCard sideBoardCard : deck.sideBoard) {
        final CardApearancesInDecks cardApearences = cardToNewApearances.get(sideBoardCard);
        final Appearance sideboardApearence = getAppearance(deck, sideBoardCard);
        if (cardApearences != null) {
          cardApearences.sideboardApearences.add(sideboardApearence);
        } else {
          final String name = sideBoardCard.card.name;
          final Integer cardDBId = CardCache.getIdFor(name);
          if (cardDBId == null) {
            System.out.println("No DB id for card name: " + name);
            continue;
          }
          cardToNewApearances.put(cardDBId, new CardApearancesInDecks(null, sideboardApearence));
        }
      }
    }
    return cardToNewApearances;
  }

  private Appearance getAppearance(final Deck deck, final DeckCard card) {
    final int deckDBId = deckCache.get(deck.originUrl);
    return new Appearance(deckDBId, card.cardCount);
  }

  public void insertDecks(final List<Deck> decks) throws SQLException {
    final StringBuilder queryBuilder = new StringBuilder("INSERT INTO dk_deck (name, dk_origin_url, dk_mainboard, dk_sideboard) VALUES ");

    for (final Deck deck : decks) {
      queryBuilder.append("(").append(deck.sqlFormat()).append("),");
    }
    decks.clear();
    queryBuilder.deleteCharAt(queryBuilder.length() - 1); // Deleting last comma
    final String query = queryBuilder.toString();

    try (final Statement stmt = connection.createStatement()) {
      blocksStored++;
      if (shouldLogStats()) {
        System.out.println("Started writing block at: " + LocalDateTime.now());
      }
      stmt.execute(query);
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
