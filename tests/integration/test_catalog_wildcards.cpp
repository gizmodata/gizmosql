// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
//
// Unit tests for AWS IAM-style wildcard matching in catalog-level access
// control. These exercise the pure pattern matcher directly and therefore do
// NOT require an enterprise license at runtime (the matcher is license-free;
// the license gate lives in GetCatalogAccess()). This file is only compiled in
// enterprise builds, where catalog_permissions_handler is linked in.

#include <gtest/gtest.h>

#include "catalog_permissions_handler.h"

using gizmosql::enterprise::MatchesCatalogPattern;

namespace {

// ----------------------------------------------------------------------------
// Exact (no-wildcard) patterns — backward-compatible behavior
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, ExactMatch) {
  EXPECT_TRUE(MatchesCatalogPattern("production", "production"));
  EXPECT_FALSE(MatchesCatalogPattern("production", "staging"));
  EXPECT_FALSE(MatchesCatalogPattern("prod", "production"));   // not a prefix match
  EXPECT_FALSE(MatchesCatalogPattern("production", "prod"));
}

TEST(CatalogWildcard, ExactMatchIsCaseSensitive) {
  EXPECT_TRUE(MatchesCatalogPattern("Prod", "Prod"));
  EXPECT_FALSE(MatchesCatalogPattern("prod", "Prod"));
  EXPECT_FALSE(MatchesCatalogPattern("PROD", "prod"));
}

TEST(CatalogWildcard, EmptyStrings) {
  EXPECT_TRUE(MatchesCatalogPattern("", ""));
  EXPECT_FALSE(MatchesCatalogPattern("", "a"));
  EXPECT_TRUE(MatchesCatalogPattern("*", ""));   // '*' matches empty
  EXPECT_FALSE(MatchesCatalogPattern("?", ""));  // '?' requires one char
}

// ----------------------------------------------------------------------------
// Bare '*' — matches everything (existing wildcard semantics)
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, StarMatchesEverything) {
  EXPECT_TRUE(MatchesCatalogPattern("*", "anything"));
  EXPECT_TRUE(MatchesCatalogPattern("*", "a"));
  EXPECT_TRUE(MatchesCatalogPattern("*", ""));
  EXPECT_TRUE(MatchesCatalogPattern("*", "with spaces and . dots"));
}

// ----------------------------------------------------------------------------
// Prefix / suffix / infix globs
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, PrefixGlob) {
  EXPECT_TRUE(MatchesCatalogPattern("prod_*", "prod_sales"));
  EXPECT_TRUE(MatchesCatalogPattern("prod_*", "prod_finance_eu"));
  EXPECT_TRUE(MatchesCatalogPattern("prod_*", "prod_"));  // '*' can match empty
  EXPECT_FALSE(MatchesCatalogPattern("prod_*", "prod"));  // missing the underscore
  EXPECT_FALSE(MatchesCatalogPattern("prod_*", "staging_sales"));
}

TEST(CatalogWildcard, SuffixGlob) {
  EXPECT_TRUE(MatchesCatalogPattern("*_west", "sales_west"));
  EXPECT_TRUE(MatchesCatalogPattern("*_west", "_west"));
  EXPECT_FALSE(MatchesCatalogPattern("*_west", "west"));
  EXPECT_FALSE(MatchesCatalogPattern("*_west", "sales_west_2"));
}

TEST(CatalogWildcard, InfixGlob) {
  EXPECT_TRUE(MatchesCatalogPattern("data_*_2025", "data_sales_2025"));
  EXPECT_TRUE(MatchesCatalogPattern("data_*_2025", "data__2025"));   // middle '*' empty
  EXPECT_FALSE(MatchesCatalogPattern("data_*_2025", "data_2025"));   // needs both underscores
  EXPECT_FALSE(MatchesCatalogPattern("data_*_2025", "data_sales_2024"));
}

TEST(CatalogWildcard, MultipleStars) {
  EXPECT_TRUE(MatchesCatalogPattern("*sales*", "prod_sales_eu"));
  EXPECT_TRUE(MatchesCatalogPattern("*sales*", "sales"));
  EXPECT_FALSE(MatchesCatalogPattern("*sales*", "prod_finance"));
  // Adjacent and trailing stars collapse to a single match-anything.
  EXPECT_TRUE(MatchesCatalogPattern("a**b", "axyzb"));
  EXPECT_TRUE(MatchesCatalogPattern("prod_**", "prod_x"));
}

// ----------------------------------------------------------------------------
// '?' single-character wildcard
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, QuestionMark) {
  EXPECT_TRUE(MatchesCatalogPattern("tenant_?", "tenant_a"));
  EXPECT_TRUE(MatchesCatalogPattern("tenant_?", "tenant_1"));
  EXPECT_FALSE(MatchesCatalogPattern("tenant_?", "tenant_"));    // needs exactly one
  EXPECT_FALSE(MatchesCatalogPattern("tenant_?", "tenant_ab"));  // exactly one, not two
  EXPECT_TRUE(MatchesCatalogPattern("db_??", "db_99"));
}

TEST(CatalogWildcard, MixedQuestionAndStar) {
  EXPECT_TRUE(MatchesCatalogPattern("t_?_*", "t_a_anything"));
  EXPECT_TRUE(MatchesCatalogPattern("t_?_*", "t_1_"));
  EXPECT_FALSE(MatchesCatalogPattern("t_?_*", "t__x"));  // '?' needs a char before second '_'
}

// ----------------------------------------------------------------------------
// Backtracking edge cases (the matcher must retry from the last '*')
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, BacktrackingRequired) {
  // Naive left-to-right matching without backtracking fails these.
  EXPECT_TRUE(MatchesCatalogPattern("*abc", "zzabcabc"));
  EXPECT_TRUE(MatchesCatalogPattern("*a*b", "xaxxb"));
  EXPECT_FALSE(MatchesCatalogPattern("*abc", "zzabcx"));
  EXPECT_TRUE(MatchesCatalogPattern("a*c*e", "abcde"));
  EXPECT_FALSE(MatchesCatalogPattern("a*c*e", "abcdx"));
}

// ----------------------------------------------------------------------------
// Literal characters that are NOT treated as wildcards (only * and ? are)
// ----------------------------------------------------------------------------
TEST(CatalogWildcard, NonWildcardSpecialCharsAreLiteral) {
  // '.' and '-' are ordinary characters, matched literally.
  EXPECT_TRUE(MatchesCatalogPattern("my-bucket.v1", "my-bucket.v1"));
  EXPECT_FALSE(MatchesCatalogPattern("my-bucket.v1", "myXbucketXv1"));
  EXPECT_TRUE(MatchesCatalogPattern("bucket.*", "bucket.prod"));
  EXPECT_FALSE(MatchesCatalogPattern("bucket.*", "bucketXprod"));
}

}  // namespace
