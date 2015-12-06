/**
 * Created by glerman on 30/11/15.
 */
public class DeckCard {

  public final Card card;
  public final int cardCount;

  public DeckCard(final Card card, final int cardCount) {
    this.card = card;
    this.cardCount = cardCount;
  }

  public DeckCard(final String cardName, final Integer multiverseId, final int cardCount) {
    this.card = new Card(cardName, multiverseId);
    this.cardCount = cardCount;
  }
}
