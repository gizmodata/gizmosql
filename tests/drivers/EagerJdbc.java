// Licensed under the Apache License, Version 2.0.
import java.sql.*;
import java.util.Properties;

/** The same harness runs in separate JVMs with each unmodified released jar. */
public class EagerJdbc {
  static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
  static long scalar(Connection c, String sql) throws Exception {
    try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
      check(r.next(), "missing scalar"); return r.getLong(1);
    }
  }
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("JDBC_URL [--reads-writes-only] [--allow-aggregate-batch-counts]");
    }
    boolean transactions = true;
    boolean allowAggregateBatchCounts = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--reads-writes-only")) transactions = false;
      else if (args[i].equals("--allow-aggregate-batch-counts")) allowAggregateBatchCounts = true;
      else throw new IllegalArgumentException("Unknown option: " + args[i]);
    }
    Properties p = new Properties();
    p.setProperty("user", System.getenv().getOrDefault("GIZMOSQL_TEST_USERNAME", "eager_test"));
    p.setProperty("password", System.getenv().getOrDefault("GIZMOSQL_TEST_PASSWORD", "eager_test_password"));
    p.setProperty("useEncryption", "false");
    String table = "jdbc_eager_" + Long.toUnsignedString(System.nanoTime());
    try (Connection c = DriverManager.getConnection(args[0], p)) {
      DatabaseMetaData metadata = c.getMetaData();
      System.out.println(metadata.getDriverName() + " " + metadata.getDriverVersion());
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE " + table + " (token BIGINT PRIMARY KEY, n BIGINT)");
        // execute + no ResultSet access, immediately replace the statement.
        s.execute("INSERT INTO " + table + " VALUES (0, 0)");
        check(scalar(c, "SELECT count(*) FROM " + table) == 1, "unfetched INSERT lost");
        check(s.executeUpdate("INSERT INTO " + table + " VALUES (99, 99)") == 1, "update count");
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
          for (int n = 1; n <= 20; n++) {
            ps.setLong(1, n); ps.setLong(2, n * 7);
            ps.execute();
            check(scalar(c, "SELECT count(*) FROM " + table) == n + 2, "prepared reuse at " + n);
          }
        }
        check(scalar(c, "SELECT sum(n) FROM " + table) == 1569, "ledger checksum");
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + table + " SET n = n + 1 WHERE token = ?")) {
          ps.setLong(1, 0);
          for (int n = 0; n < 10; n++) check(ps.executeUpdate() == 1, "update affected rows");
        }
        check(scalar(c, "SELECT n FROM " + table + " WHERE token=0") == 10, "UPDATE repeated or lost");
        check(s.executeUpdate("DELETE FROM " + table + " WHERE token=99") == 1, "delete count");
        try (ResultSet r = s.executeQuery("INSERT INTO " + table + " VALUES (100, 7) RETURNING n")) {
          check(r.next() && r.getLong(1) == 7 && !r.next(), "INSERT RETURNING");
        }
        // Plain DML has no result set (the server advertises an empty dataset
        // schema): execute() reports an update count, and executeQuery() must
        // not hand back a synthetic count row, per the JDBC specification. The
        // write itself still happens exactly once either way.
        check(!s.execute("INSERT INTO " + table + " VALUES (101, 8)") && s.getUpdateCount() == 1,
              "execute() on DML reports an update count");
        boolean refused;
        try {
          refused = s.executeQuery("INSERT INTO " + table + " VALUES (102, 9)") == null;
        } catch (SQLException e) {
          refused = true;
        }
        check(refused, "executeQuery() on DML must not return a result set");
        check(scalar(c, "SELECT count(*) FROM " + table + " WHERE token IN (101, 102)") == 2,
              "DML through execute()/executeQuery() ran exactly once");
        try (PreparedStatement ps = c.prepareStatement("SELECT n FROM " + table + " WHERE token=?")) {
          for (int n = 1; n <= 20; n++) {
            ps.setLong(1, n);
            try (ResultSet r = ps.executeQuery()) { check(r.next() && r.getLong(1) == n * 7, "read rebind"); }
          }
        }
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
          for (int n = 300; n < 305; n++) {
            ps.setLong(1, n); ps.setLong(2, n); ps.addBatch();
          }
          int[] counts = ps.executeBatch();
          if (allowAggregateBatchCounts && counts.length == 1) {
            check(counts[0] == 5, "aggregate batch count");
            System.out.println("KNOWN DRIVER LIMITATION: aggregate batch count returned instead of one result per entry");
          } else {
            check(counts.length == 5, "batch result length");
            for (int count : counts) check(count == 1 || count == Statement.SUCCESS_NO_INFO, "batch update count");
            check(ps.executeBatch().length == 0, "empty batch result");
          }
        }
        check(scalar(c, "SELECT sum(n) FROM " + table + " WHERE token BETWEEN 300 AND 304") == 1510, "batch ledger checksum");
        s.execute("ALTER TABLE " + table + " ADD COLUMN extra INTEGER");
        check(scalar(c, "SELECT count(*) FROM " + table + " WHERE extra IS NOT NULL") == 0, "ALTER default");
        String dropped = table + "_drop";
        s.execute("CREATE TABLE " + dropped + " AS SELECT 42 AS n");
        check(scalar(c, "SELECT n FROM " + dropped) == 42, "CREATE AS SELECT");
        s.execute("DROP TABLE " + dropped);
        try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM information_schema.tables WHERE table_name=?")) {
          ps.setString(1, dropped);
          try (ResultSet r = ps.executeQuery()) { check(r.next() && r.getLong(1) == 0, "DROP TABLE"); }
        }
        System.out.println("PASS: reads, DDL, DML, repeated binds, batch writes, RETURNING");
        if (transactions) {
        c.setAutoCommit(false);
        s.executeUpdate("INSERT INTO " + table + " (token, n) VALUES (200, 1)");
        c.rollback();
        check(scalar(c, "SELECT count(*) FROM " + table + " WHERE token=200") == 0, "ROLLBACK");
        s.executeUpdate("INSERT INTO " + table + " (token, n) VALUES (201, 1)");
        c.commit();
        c.setAutoCommit(true);
        check(scalar(c, "SELECT count(*) FROM " + table + " WHERE token=201") == 1, "COMMIT");
        System.out.println("PASS: rollback and commit");
        } else {
          System.out.println("NOT TESTED in this invocation: JDBC transactions");
        }
        s.execute("DROP TABLE " + table);
      }
    }
  }
}
