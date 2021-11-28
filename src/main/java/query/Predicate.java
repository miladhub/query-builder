package query;

public sealed interface Predicate {}
record BinOp(AttrClauseTerm left, Op op, ClauseTerm right) implements Predicate {}
record Or(Predicate left, Predicate right) implements Predicate {}
record And(Predicate left, Predicate right) implements Predicate {}
record Not(Predicate predicate) implements Predicate {}
