package org.rocksdb.japigen;

public record CFunction(
  String comment,
  String kind,
  String name,
  String receiver_type,
  String receiver_name,
  String return_type,
  String value_type,
  String value_name,
  String value_expr,
  String field,
  String expr,
  String len_type,
  String len_name) {
}
