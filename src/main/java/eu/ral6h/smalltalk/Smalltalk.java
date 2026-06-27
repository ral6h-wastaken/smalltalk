package eu.ral6h.smalltalk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import javax.net.ServerSocketFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

public class Smalltalk {

  record Message(String sender, String message) {
  }

  private static final Logger log = System.getLogger(Smalltalk.class.getName());
  private static final ExecutorService ex = Executors.newWorkStealingPool();
  private static final ConcurrentHashMap<String, BlockingQueue<Message>> messages = new ConcurrentHashMap<>();

  private static final AtomicLong counter = new AtomicLong();

  public static void main(String[] args) throws IOException {
    try (final var server = ServerSocketFactory.getDefault().createServerSocket(1234)) {
      while (server.isBound()) {
        final var socket = server.accept();
        final var alias = "cli" + counter.incrementAndGet();
        messages.put(alias, new LinkedBlockingQueue<>(256));
        final var msg = "%s joined the chat".formatted(alias);
        broadcast("OS", msg);
        ex.submit(() -> handleWrites(socket, alias));
        ex.submit(() -> handleReads(socket, alias));
      }
    }
  }

  static void handleWrites(Socket socket, String alias) {
    // socket lifetime is owned by handleReads
    try {
      final var os = new PrintStream(socket.getOutputStream());
      for (;;) {
        final var queue = messages.get(alias);
        if (queue == null)
          break;

        final var msg = queue.take();
        os.println("> %s: %s".formatted(msg.sender(), msg.message()));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      log.log(Level.ERROR, "Write error for {0}", alias, e);
    }
  }

  static void handleReads(Socket socket, String alias) {
    try (final var is = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
      String line;
      while ((line = is.readLine()) != null) {
        broadcast(alias, line);
      }
    } catch (IOException e) {
      log.log(Level.ERROR, "Read error for {0}", alias, e);
    } finally {
      messages.remove(alias);
      final var msg = "%s left the chat".formatted(alias);
      broadcast("OS", msg);
      try {
        socket.close();
      } catch (IOException ignored) {
      }
    }
  }

  private static void broadcast(String alias, final String message) {
    final var msg = new Message(alias, message);

    for (final var entry : messages.entrySet()) {
      if (!alias.equals(entry.getKey())) {
        final var accepted = entry.getValue().offer(msg);
        if (!accepted) {
          log.log(Level.WARNING, "Queue full for {0}, dropping message from {1}",
              entry.getKey(), alias);
        }
      }
    }
  }
}
