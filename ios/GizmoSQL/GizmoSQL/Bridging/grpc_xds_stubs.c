// Stub definitions for gRPC xDS/envoy proto symbols.
// These are referenced by gRPC's xDS transport security code but never
// actually called in our use case (simple gRPC server, no service mesh).
// Providing stubs avoids pulling in the entire envoy proto compilation.

#include <stddef.h>

// Envoy config protos
void envoy_config_cluster_v3_Cluster_msginit(void) {}
void envoy_config_cluster_v3_cluster_proto_upbdefinit(void) {}
void envoy_config_core_v3_TypedExtensionConfig_msginit(void) {}
void envoy_config_core_v3_extension_proto_upbdefinit(void) {}
void envoy_config_rbac_v3_RBAC_msginit(void) {}
void envoy_config_rbac_v3_rbac_proto_upbdefinit(void) {}
void envoy_extensions_filters_common_fault_v3_FaultDelay_msginit(void) {}
void envoy_extensions_filters_common_fault_v3_FaultRateLimit_msginit(void) {}
void envoy_extensions_filters_common_fault_v3_fault_proto_upbdefinit(void) {}

// Envoy type matchers
void envoy_type_matcher_v3_ListStringMatcher_msginit(void) {}
void envoy_type_matcher_v3_MetadataMatcher_msginit(void) {}
void envoy_type_matcher_v3_RegexMatchAndSubstitute_msginit(void) {}
void envoy_type_matcher_v3_RegexMatcher_msginit(void) {}
void envoy_type_matcher_v3_StringMatcher_msginit(void) {}
void envoy_type_matcher_v3_StructMatcher_msginit(void) {}
void envoy_type_matcher_v3_metadata_proto_upbdefinit(void) {}
void envoy_type_matcher_v3_regex_proto_upbdefinit(void) {}
void envoy_type_matcher_v3_string_proto_upbdefinit(void) {}
void envoy_type_v3_http_proto_upbdefinit(void) {}
void envoy_config_tap_v3_TapConfig_msginit(void) {}
void envoy_config_tap_v3_common_proto_upbdefinit(void) {}
void envoy_type_matcher_v3_struct_proto_upbdefinit(void) {}

// Google RPC
void google_rpc_status_proto_upbdefinit(void) {}

// gRPC trace
int grpc_tcp_trace = 0;

// UDPA annotations
void udpa_annotations_migrate_proto_upbdefinit(void) {}
void udpa_annotations_security_proto_upbdefinit(void) {}
void udpa_annotations_sensitive_proto_upbdefinit(void) {}
void udpa_annotations_status_proto_upbdefinit(void) {}
void udpa_annotations_versioning_proto_upbdefinit(void) {}
