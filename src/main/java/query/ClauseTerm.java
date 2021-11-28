package query;

public sealed interface ClauseTerm {}
record AttrClauseTerm(Attr attr) implements ClauseTerm {}
record Value(Object value) implements ClauseTerm {}
record Null() implements ClauseTerm {}