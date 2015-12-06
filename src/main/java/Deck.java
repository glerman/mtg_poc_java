import com.google.gson.Gson;

import java.util.Collection;

/**
 * Created by glerman on 30/11/15.
 */
public class Deck {

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

  @Override
  public String toString() {
    final Gson gson = new Gson();
    return gson.toJson(this);
//    final MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
//    toStringHelper.add("name", name).add("originUrl", originUrl).add("mainBoard", mainBoard).add("sideBoard", sideBoard)
//                  .add("hasSideBoard", hasSideBoard);
//    return toStringHelper.toString();
  }

  public static Deck fromString(final String deckStr) {
    final Gson gson = new Gson();
    return gson.fromJson(deckStr, Deck.class);
  }
}
