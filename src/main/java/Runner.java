import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by glerman on 14/11/15.
 */
public class Runner {

  private static final String BASE_URI = "https://deckbox.org";


  public static void main(String[] args) throws Exception {
    final long start = System.currentTimeMillis();
    try(final FileBasedDeckStore deckStore = new FileBasedDeckStore("/Users/glerman/Documents/deckbox_db")) {
      // Fetch and parse the first deck search result page
      final String deckSearchUrl = BASE_URI + "/decks/mtg";
      final Document firstDeckSearchPage = fetchUrl(deckSearchUrl);
      parseDeckSearchResultPage(firstDeckSearchPage, deckStore);

      // Parse out next search page url and the number of pages
      final Element paginationControls = firstDeckSearchPage.getElementsByClass("pagination_controls").first();
      final Elements pageLinks = paginationControls.select("a[href]");
      final Element lastPageLink = pageLinks.last();
      final String lastPageHref = lastPageLink.attr("href");
      final String[] hrefSplit = lastPageHref.split("=");
      final String pageLessHref = hrefSplit[0];
      final int lastPage = Integer.valueOf(hrefSplit[1]);

      // Parse all the remaining deck pages
      for (int currPage = 2 ; currPage <= lastPage ; currPage++) {
        final String nextPageUri = BASE_URI + pageLessHref + currPage;
        final Document nextDeckSearchPage = fetchUrl(nextPageUri);
        parseDeckSearchResultPage(nextDeckSearchPage, deckStore);
      }
      final long minutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start);
      System.out.println("Running time:" + minutes + " [min]");
    }
  }

  private static void parseDeckSearchResultPage(final Document deckSearchResultPage, final FileBasedDeckStore deckStore) throws IOException {
    System.out.println("Parsing deck page: " + deckSearchResultPage.baseUri());
    final Elements links = deckSearchResultPage.select("a[href]");
    for(final Element deckLink : links) {
      String linkHref = deckLink.attr("href");

      if (linkHref.startsWith("/sets/")) {
        final String deckUrl = BASE_URI + linkHref;
        final Element deckNameElem = deckLink.children().last();
        final String deckName = deckNameElem.childNode(0).toString();
        final Document deckPage = fetchUrl(deckUrl);
        parseDeckPage(deckPage, deckName, deckStore);
      }
    }
  }

  private static void parseDeckPage(final Document deckPage, final String deckName, final FileBasedDeckStore deckStore) throws IOException {

    final Elements possibleCards = deckPage.select("tr:matches(\\d+)");

    final Elements mainBoardCardElems = new Elements();
    final Elements sideBoardCardElems = new Elements();
    for (final Element card : possibleCards) {
      if (card.id().contains("_main")) {
        mainBoardCardElems.add(card);
      } else if (card.id().contains("_sideboard")) {
        sideBoardCardElems.add(card);
      }
    }
    final List<DeckCard> mainBoardCards = parseCardTable(mainBoardCardElems);
    List<DeckCard> sideBoardCards = null;
    if (!sideBoardCardElems.isEmpty()) {
      sideBoardCards = parseCardTable(sideBoardCardElems);
    }

    final Deck deck = new Deck(deckName, deckPage.baseUri(), mainBoardCards, sideBoardCards);
    deckStore.write(deck);
  }

  private static List<DeckCard> parseCardTable(final Elements cards) {
    final List<DeckCard> deckCards = new ArrayList<DeckCard>();
    for (final Element card : cards) {
      final String cardName = card.getElementsByClass("card_name").first().childNode(1).childNode(0).toString();
      final String cardCount = card.getElementsByClass("card_count").first().childNode(0).toString();
      final DeckCard deckCard = new DeckCard(cardName, null, Integer.valueOf(cardCount));
      deckCards.add(deckCard);
    }

    return deckCards;
  }

  private static Document fetchUrl(final String url) throws IOException {
    HttpClient client = new HttpClient();

    // Create a method instance.
    GetMethod method = new GetMethod(url);

    // Provide custom retry handler is necessary
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
            new DefaultHttpMethodRetryHandler(3, false));

    try {
      // Execute the method.
      int statusCode = client.executeMethod(method);

      if (statusCode != HttpStatus.SC_OK) {
        System.err.println("Method failed: " + method.getStatusLine());
      }

      // Read the response body.
      final InputStream responseBody = method.getResponseBodyAsStream();
      final String responseCharSet = method.getResponseCharSet();
      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary data
      return Jsoup.parse(responseBody, responseCharSet, url);

    } finally {
      // Release the connection.
      method.releaseConnection();
    }
  }
}
