import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;

public class FileBasedDeckStoreTest {

  @Test
  public void testRead() throws Exception {
    final FileBasedDeckStore deckStore = new FileBasedDeckStore("/Users/glerman/Documents/deckbox_db_test");

    final Deck deck = deckStore.readFirstDeck();

    Assert.assertTrue(!deck.name.isEmpty());
    Assert.assertTrue(deck.mainBoard.size() > 0);
  }

  @Test
  public void testByteArray() throws Exception {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final ObjectOutputStream outputStream = new ObjectOutputStream(bos);

    final Deck deck = new Deck("deck", "url", Collections.singleton(new DeckCard("card", 1, 1)), null);

    final byte[] bytes = deck.toString().getBytes();



    System.out.println(Arrays.toString(bytes));
    System.out.println(deck);
    System.out.println(deck.toString().length());
    System.out.println(bytes.length);

  }
}