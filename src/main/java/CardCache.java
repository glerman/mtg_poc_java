import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by glerman on 3/12/15.
 */
public class CardCache {

//  private static final String cardsApiUrl = "http://api.mtgapi.com/v2/cards";
//  private static final String cardsApiUrl = "http://scry.me.uk/api.php?name=chronozoa";

  private static final Map<String, Integer> cardNameToId = new HashMap<>(30 * 1000);
  private static boolean initialized = false;


  public static Integer getIdFor(final String cardName) {
    if (!initialized) {
      init();
    }

    return cardNameToId.get(cardName);
  }

  private static void init(){


    initialized = true;
  }

  public static String fetch(final String uri) throws IOException {
//    HttpClient client = new HttpClient();
//
//    // Create a method instance.
//    PostMethod method = new PostMethod(cardsApiUrl);
//
//    // Provide custom retry handler is necessary
//    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
//            new DefaultHttpMethodRetryHandler(3, false));
//
//    try {
//      // Execute the method.
//      int statusCode = client.executeMethod(method);
//
//      if (statusCode != HttpStatus.SC_OK) {
//        System.err.println("Method failed: " + method.getStatusLine());
//      }
//
//      // Read the response body.
//      final InputStream inputStream = method.getResponseBodyAsStream();
//      StringWriter sw = new StringWriter();
//      IOUtils.copy(inputStream, sw);
//      return sw.toString();
//    } finally {
//      // Release the connection.
//      method.releaseConnection();
//    }
////  }
//    HttpGet httpget = new HttpGet(
//            "http://api.mtgapi.com/v2/cards");

    final CloseableHttpClient client = HttpClients.createDefault();
    HttpGet httpget = new HttpGet(uri);
    final CloseableHttpResponse response = client.execute(httpget);
    final HttpEntity entity = response.getEntity();
    return inputStringToString(entity.getContent());
  }

  private static String inputStringToString(final InputStream is) throws IOException {
      StringWriter sw = new StringWriter();
      IOUtils.copy(is, sw);
      return sw.toString();
  }
}
