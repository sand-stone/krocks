package ktsdb;

import org.restexpress.RestExpress;
import org.restexpress.Request;
import org.restexpress.Response;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.util.Properties;
import java.util.List;
import org.restexpress.util.Environment;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Server {
  private static Logger log = LogManager.getLogger(Server.class);

  public static class Configuration {

    List<String> kdb;
    int port;
    String name;

    public List<String> getKdb() {
      return kdb;
    }

    public void setKdb(List<String> kdb) {
      this.kdb = kdb;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public static class HelloController {
    public ByteBuf create(Request request, Response response) {
      response.setResponseCreated();
      return request.getBody();
    }

    public String delete(Request request, Response response) {
      return "";
    }

    public String read(Request request, Response response) {
      log.info(request.getRemoteAddress());
      String echo = request.getHeader("xx");
      return "hello from ktsdb!";
    }

    public ByteBuf update(Request request, Response response) {
      return request.getBody();
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("hello world\n");
    new Server().run(args);
  }

  public void run(String[] args) {
    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      Configuration config = mapper.readValue(new File(args[0]), Configuration.class);
      Kdb ts = Kdb.open(config.getKdb(), config.getName());
      RestExpress server = new RestExpress();
      server
        .uri("/", new HelloController()).noSerialization();
      server
        .uri("/api/put", new PutResource(ts))
        .action("read", HttpMethod.POST)
        .method(HttpMethod.POST).noSerialization();
      server
        .uri("/query", new QueryResource(ts))
        .action("read", HttpMethod.POST)
        .method(HttpMethod.POST).noSerialization();
      server
        .uri("/suggest", new SuggestResource(ts))
        .action("read", HttpMethod.POST)
        .method(HttpMethod.POST).noSerialization();
      server.bind(config.getPort());
      server.awaitShutdown();
    } catch(Exception e) {
      e.printStackTrace();
      log.info(e);
    }
  }
}
