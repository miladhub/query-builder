package query;

public sealed interface Term {}
record AttrTerm(Attr attr) implements Term {}
record Value(Object value) implements Term {}
record Null() implements Term {}
record Max(AttrTerm t) implements Term {}