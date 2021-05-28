package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TServer server;

  private static Manager manager = Manager.getInstance();

  private static long sessionNumber = 0;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
//    String len = "create table personmem(id string(8),name string(8),age int,undergraduate int,birth_year double, primary key (id));";
//    CharStream stream = CharStreams.fromString(len);
//    SQLLexer lexer = new SQLLexer(stream);
//    CommonTokenStream token = new CommonTokenStream(lexer);
//    SQLParser parser = new SQLParser(token);
//    SQLParser.ParseContext context = parser.parse();
//    logger.info(context.toStringTree());
    logger.info("recoverMeta");
    ThssDB server = ThssDB.getInstance();
    server.start();
  }

  public long setupSession() {
    long sessionId = sessionNumber++;
    manager.addSession(sessionId);
    return sessionId;
  }

  public void deleteSession(long sessionNumber) {
    manager.deleteSession(sessionNumber);
  }

  private void start() {
    handler = new IServiceHandler();
    processor = new IService.Processor(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TSimpleServer(new TServer.Args(transport).processor(processor));
      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  public List<SQLResult> execute(String sql, long sessionId) {
    return manager.execute(sql, sessionId);
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();
    private ThssDBHolder() {

    }
  }
}
