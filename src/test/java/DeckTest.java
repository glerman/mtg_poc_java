import org.junit.Assert;
import org.junit.Test;

public class DeckTest {


  @Test
  public void test() throws Exception {
    final Deck deck1 = new Deck("deck name", "www.wizards.com", null, null);
    final String s1 = deck1.toString();

    System.out.println(s1);

    final Deck deck2 = Deck.fromString(s1);
    Assert.assertEquals(s1, deck2.toString());
  }
}