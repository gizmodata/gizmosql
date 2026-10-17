// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Credentials never reach a log line or an instrumentation row: SQL that carries
// them, and errors that quote that SQL, are redacted (redact_sql_for_logs), and
// ordinary SQL is left as it is.

#include <gtest/gtest.h>

#include <string>

#include "gizmosql_logging.h"

using gizmosql::redact_sql_for_logs;

namespace {

void ExpectRedacted(const std::string& sql, const std::string& kind) {
  const std::string out = redact_sql_for_logs(sql);
  EXPECT_EQ(out, "[REDACTED: " + kind + "]") << sql;
  EXPECT_EQ(out.find("LEAK"), std::string::npos) << sql;
}

void ExpectMasked(const std::string& sql) {
  const std::string out = redact_sql_for_logs(sql);
  EXPECT_EQ(out.find("LEAK"), std::string::npos) << sql << "\n -> " << out;
  EXPECT_NE(out.find("***"), std::string::npos) << sql << "\n -> " << out;
  EXPECT_EQ(redact_sql_for_logs(out), out) << "not stable: " << out;
}

void ExpectUnchanged(const std::string& sql) { EXPECT_EQ(redact_sql_for_logs(sql), sql); }

}  // namespace

TEST(LogRedaction, SecretDdlIsRedactedHoweverItIsWritten) {
  for (const char* sql : {
           "CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'LEAK')",
           "create or replace persistent secret s (TYPE s3, SECRET 'LEAK')",
           "CREATE TEMP SECRET s (TYPE s3, SECRET 'LEAK')",
           "CREATE /* x */ SECRET s (TYPE s3, KEY_ID 'k', SECRET 'LEAK')",
           "CREATE -- x\nSECRET s (TYPE s3, SECRET 'LEAK')",
           "CREATE OR/**/REPLACE SECRET s (TYPE s3, SECRET 'LEAK')",
           "CREATE /* a /* nested */ comment */ SECRET s (TYPE s3, SECRET 'LEAK')",
           "/* lead */ CREATE SECRET s (TYPE s3, SECRET 'LEAK')",
           "SELECT 1; CREATE SECRET s (TYPE s3, SECRET 'LEAK')",
           // A string that looks like a comment opener does not hide what follows.
           "SELECT '/*'; CREATE SECRET s (TYPE s3, SECRET 'LEAK'); SELECT '*/'",
           "SELECT $$ -- $$; CREATE SECRET s (TYPE s3, SECRET 'LEAK')",
           "SELECT E'it\\'s'; CREATE SECRET s (TYPE s3, SECRET 'LEAK')",
       }) {
    ExpectRedacted(sql, "secret DDL");
  }
}

TEST(LogRedaction, AnErrorThatQuotesTheStatementIsRedactedToo) {
  ExpectRedacted(
      "Parser Error: syntax error at or near \"BOGUS\"\n\nLINE 1: CREATE SECRET s6 (TYPE "
      "s3, KEY_ID 'k', SECRET 'LEAK' BOGUS)\n                                   ^",
      "secret DDL");
  ExpectRedacted(
      "Can't prepare statement: 'ALTER /* c */ USER bob PASSWORD 'LEAK'' - Error: x",
      "user DDL");
}

TEST(LogRedaction, UserDdlAndCredentialSettingsAreRedacted) {
  ExpectRedacted("alter /* c */ user bob password 'LEAK'", "user DDL");
  ExpectRedacted("CREATE USER acme/boss PASSWORD 'LEAK'", "user DDL");
  for (const char* sql :
       {"SET s3_secret_access_key = 'LEAK'", "SET GLOBAL s3_secret_access_key='LEAK'",
        "SET /* c */ SESSION s3_access_key_id TO 'LEAK'",
        "SET \"http_proxy_password\" = 'LEAK'", "PRAGMA s3_session_token='LEAK'",
        "SET azure_storage_connection_string = 'LEAK'"}) {
    ExpectRedacted(sql, "credential setting");
  }
}

TEST(LogRedaction, ConnectionStringAndUriPasswordsAreMasked) {
  for (const char* sql : {
           "ATTACH 'postgres:host=h dbname=x user=u password=LEAK' AS p (TYPE postgres)",
           "ATTACH 'ducklake:postgres:dbname=x user=u password=LEAK' AS d",
           "ATTACH 'host=h PASSWORD = LEAK user=u' AS p (TYPE postgres)",
           "ATTACH 'mysql:host=h user=u passwd=LEAK' AS m (TYPE mysql)",
           "ATTACH 'host=h password=''LEAK with space''' AS p (TYPE postgres)",
           "SELECT * FROM read_csv('postgresql://u:LEAK@127.0.0.1:1/x')",
           "SELECT * FROM "
           "'https://acct.blob.core.windows.net/c/f.csv?sv=1&sig=LEAK&se=2'",
           "SELECT * FROM read_json('https://api.example.com/x.json?token=LEAK')",
           "ATTACH 'ducklake:x' (METADATA_PARAMETERS MAP {'password': 'LEAK'})",
           "IO Error: Unable to connect to Postgres at \"host=h password=LEAK\": refused",
       }) {
    ExpectMasked(sql);
  }
  // The rest of the statement stays readable.
  EXPECT_EQ(redact_sql_for_logs("ATTACH 'postgres:dbname=x user=u password=LEAK' AS p"),
            "ATTACH 'postgres:dbname=x user=u password=***' AS p");
  EXPECT_EQ(redact_sql_for_logs("SELECT * FROM read_csv('s3://u:LEAK@bucket/k.csv')"),
            "SELECT * FROM read_csv('s3://u:***@bucket/k.csv')");
}

TEST(LogRedaction, OrdinarySqlAndMessagesAreLeftAlone) {
  for (const char* sql : {
           "SELECT l_orderkey, sum(l_extendedprice) FROM lineitem GROUP BY 1",
           "SELECT * FROM users WHERE user_id = 1 OFFSET 10",
           "CREATE TABLE secrets_inventory (id INT)",
           "SELECT 'CREATE' AS a, 'SECRET' AS b",
           "SELECT * FROM duckdb_secrets()",
           "SET threads = 4",
           "SET search_path = 'main'",
           "ALTER TABLE users ADD COLUMN created_by VARCHAR",
           "SELECT * FROM read_csv('https://example.com/data.csv')",
           "Bearer Token was validated successfully - token_claims=(id=abc)",
           "invalid token: expired",
           "Client session was successfully created.",
           "",
       }) {
    ExpectUnchanged(sql);
  }
}

TEST(LogRedaction, RedactingTwiceGivesTheSameText) {
  for (const char* sql :
       {"CREATE SECRET s (SECRET 'LEAK')", "SET s3_secret_access_key='LEAK'",
        "ATTACH 'password=LEAK' AS p", "SELECT 'postgres://u:LEAK@h/x'"}) {
    const std::string once = redact_sql_for_logs(sql);
    EXPECT_EQ(redact_sql_for_logs(once), once) << sql;
  }
}
