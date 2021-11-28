package query;

public sealed interface Predicate {}
record BinOp(AttrTerm left, Op op, Term right) implements Predicate {}
record Or(Predicate left, Predicate right) implements Predicate {}
record And(Predicate left, Predicate right) implements Predicate {}
record Not(Predicate predicate) implements Predicate {}
