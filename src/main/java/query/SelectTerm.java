package query;

public sealed interface SelectTerm {}
record AttrSelectTerm(Attr attr) implements SelectTerm {}
record Aggregation(AttrSelectTerm t, AggrType at) implements SelectTerm {}