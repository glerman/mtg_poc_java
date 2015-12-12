import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by glerman on 30/11/15.
 */
public class Deck {

  public static final String CARD_INSTANCES_SEPARATOR = "x";

  public Integer dbId;
  public final String name;
  public final String originUrl;
  public final Collection<DeckCard> mainBoard;
  public final Collection<DeckCard> sideBoard;

  public Deck(final String name, final String originUrl, final Collection<DeckCard> mainBoard, final Collection<DeckCard> sideBoard) {
    this.name = name;
    this.originUrl = originUrl;
    this.mainBoard = mainBoard;
    this.sideBoard = sideBoard;
  }

  public String sqlFormat() {

    final StringBuilder sb = new StringBuilder();
    sb.append("\"").append(name).append("\"").append(",")
    .append("\"").append(originUrl).append("\"").append(",");

    appendCards(sb, mainBoard).append(",");
    appendCards(sb, sideBoard);

    return sb.toString();
  }

  public boolean hasSideoard() {
    return sideBoard != null && sideBoard.size() > 0;
  }

  public static void main(String[] args) {
    final Deck deck = new Deck("deck name", "original url",
                               Lists.newArrayList(new DeckCard(new Card("c1", 231), 3), new DeckCard(new Card("c2", 234), 1)),
                               Lists.newArrayList(new DeckCard(new Card("s1", 387), 2), new DeckCard(new Card("s2", 765), 2)));
    System.out.println(deck.sqlFormat());
  }

  private StringBuilder appendCards(final StringBuilder sb, final Collection<DeckCard> cards) {
    sb.append("\"");
    if (cards != null && cards.size() > 0) {
      for (final DeckCard deckCard : cards) {
        final int cardDBId = CardCache.getIdFor(deckCard.card.name);
        sb.append(deckCard.cardCount).append(CARD_INSTANCES_SEPARATOR).append(cardDBId).append(",");
      }
      sb.deleteCharAt(sb.length() - 1); //Remove last comma
    }
    return sb.append("\"");
  }


  public static Deck fromString(final String deckStr) {
    final Gson gson = new Gson();
    return gson.fromJson(deckStr, Deck.class);
  }

  public void setDbId(final Integer dbId) {
    this.dbId = dbId;
  }
}
