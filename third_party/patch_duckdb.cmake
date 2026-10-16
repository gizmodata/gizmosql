# Adds one hook to DuckDB's binder: a function pointer that is called with the
# plan of every view as the view is bound. GizmoSQL's access control uses it to
# mark which part of a statement's plan came from which view, so a view can be
# checked with its owner's rights (src/enterprise/rbac/rbac_views.cpp). The
# pointer is null unless GizmoSQL sets it, so DuckDB behaves exactly as before.
#
# DuckDB 1.4 binds a view without building its plan, so there is nothing to
# hook; it is left unpatched, and views there are checked with the caller's
# rights. From 1.5 on, a source that no longer matches is a build error rather
# than a silent skip.
#
# Run as: cmake -DDUCKDB_SOURCE_DIR=<dir> -DDUCKDB_VERSION_TAG=<vX.Y.Z> -P patch_duckdb.cmake

set(BINDER_FILE "${DUCKDB_SOURCE_DIR}/src/planner/binder/tableref/bind_basetableref.cpp")
set(MARKER "gizmosql_bound_view_hook")

if(DUCKDB_VERSION_TAG MATCHES "^v1[.][0-4][.]")
  message(STATUS "patch_duckdb: DuckDB ${DUCKDB_VERSION_TAG} has no view plan to hook; not patched")
  return()
endif()

file(READ "${BINDER_FILE}" CONTENT)
string(FIND "${CONTENT}" "${MARKER}" already)
if(NOT already EQUAL -1)
  message(STATUS "patch_duckdb: ${BINDER_FILE} is already patched")
  return()
endif()

# 1. The hook's definition, at the top of the duckdb namespace. (Built with
# string(CONCAT) of quoted pieces: the C++ contains semicolons, which an
# unquoted CMake list expansion would split on and drop.)
string(FIND "${CONTENT}" "namespace duckdb {\n" ns_at)
if(ns_at EQUAL -1)
  message(FATAL_ERROR "patch_duckdb: no 'namespace duckdb {' in ${BINDER_FILE}")
endif()
string(LENGTH "namespace duckdb {\n" ns_len)
math(EXPR ns_end "${ns_at} + ${ns_len}")
string(SUBSTRING "${CONTENT}" 0 ${ns_end} head)
string(SUBSTRING "${CONTENT}" ${ns_end} -1 tail)
string(CONCAT CONTENT "${head}"
  "\n// GizmoSQL: called with the plan of each view as it is bound (null unless set).\n"
  "void (*gizmosql_bound_view_hook)(ClientContext &context, ViewCatalogEntry &view,\n"
  "                                 unique_ptr<LogicalOperator> &plan) = nullptr;\n\n"
  "${tail}")

# 2. The call, after the view's binding is recorded and before its plan is
# returned to the enclosing query. The anchor ends the VIEW_ENTRY case in 1.5
# and 2.0 alike, and must occur exactly once.
set(ANCHOR "view_catalog_entry);\n\t\treturn bound_child;\n")
string(FIND "${CONTENT}" "${ANCHOR}" first_at)
string(FIND "${CONTENT}" "${ANCHOR}" last_at REVERSE)
if(first_at EQUAL -1 OR NOT first_at EQUAL last_at)
  message(FATAL_ERROR "patch_duckdb: the view-binding return in ${BINDER_FILE} was not found exactly once; "
                      "update third_party/patch_duckdb.cmake for DuckDB ${DUCKDB_VERSION_TAG}")
endif()
string(CONCAT REPLACEMENT
  "view_catalog_entry);\n"
  "\t\tif (gizmosql_bound_view_hook) {\n"
  "\t\t\tgizmosql_bound_view_hook(context, view_catalog_entry, bound_child.plan);\n"
  "\t\t}\n"
  "\t\treturn bound_child;\n")
string(REPLACE "${ANCHOR}" "${REPLACEMENT}" CONTENT "${CONTENT}")

file(WRITE "${BINDER_FILE}" "${CONTENT}")
message(STATUS "patch_duckdb: added the view-binding hook to ${BINDER_FILE}")
