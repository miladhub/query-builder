package query;

public sealed interface SelectTerm {}
record AttrSelectTerm(Attr attr) implements SelectTerm {}
record Max(AttrSelectTerm t) implements SelectTerm {}