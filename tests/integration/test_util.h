#pragma once
#include <iostream>
#include <gtest/gtest.h>
#include <arrow/result.h>

#define ASSERT_ARROW_OK(expr) \
ASSERT_TRUE((expr).ok()) << "Arrow error: " << (expr).ToString()

#define ASSERT_ARROW_OK_AND_ASSIGN_IMPL(varname, lhs, rexpr)                     \
auto varname = (rexpr);                                                        \
ASSERT_TRUE(varname.ok()) << "Arrow failure: " << varname.status().ToString(); \
lhs = std::move(varname).ValueOrDie();

#define ASSERT_ARROW_OK_AND_ASSIGN(lhs, rexpr) \
ASSERT_ARROW_OK_AND_ASSIGN_IMPL(             \
ASSERT_ARROW_OK_AND_ASSIGN_UNIQUE_NAME(_res, __COUNTER__), lhs, rexpr)

#define ASSERT_ARROW_OK_AND_ASSIGN_UNIQUE_NAME(base, counter) \
ASSERT_ARROW_OK_AND_ASSIGN_UNIQUE_NAME_EXPAND(base, counter)

#define ASSERT_ARROW_OK_AND_ASSIGN_UNIQUE_NAME_EXPAND(base, counter) base##counter
