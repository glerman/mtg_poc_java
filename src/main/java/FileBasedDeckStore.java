import org.apache.log4j.FileAppender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Scanner;

/**
 * Created by glerman on 30/11/15.
 */
public class FileBasedDeckStore implements AutoCloseable {

//  private final Writer writer;
  private final Scanner scanner;

  public FileBasedDeckStore(final String filePath) throws IOException {
    final File file = new File(filePath);
//    file.createNewFile();
//    this.writer = new BufferedWriter(new FileWriter(file));
    scanner = new Scanner(file);
  }


  public void write(final Deck deck) throws IOException {
//    writer.append(deck.toString()).append('\n');
  }

  public Deck readFirstDeck() {
    final String l = scanner.nextLine();
    return Deck.fromString(l);
  }

  @Override
  public void close() throws Exception {
//    writer.close();
    scanner.close();
  }
}
