package query;

public sealed interface Op {}
record Eq() implements Op {}
record Lt() implements Op {}
record Gt() implements Op {}
record Like() implements Op {}
