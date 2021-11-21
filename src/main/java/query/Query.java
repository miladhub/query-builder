package query;

import java.util.List;

public sealed interface Query {}
record From(EntityType from, List<Predicate> where) implements Query {}
record Join(Query left, Query right, List<Predicate> on, List<Predicate> where) implements Query {}
