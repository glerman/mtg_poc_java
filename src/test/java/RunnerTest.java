import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;

public class RunnerTest {

  @Test
  public void testWriter() throws Exception {


    final ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    final BufferedOutputStream out = new BufferedOutputStream(out1);

    out.write("blablabla".getBytes());
    out.write("blablabla".getBytes());
    out.write("blablabla".getBytes());
    out.write("blablabla".getBytes());

    out.flush();
    System.out.println(out1);
  }
}