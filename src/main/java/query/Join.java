package query;

import io.vavr.collection.List;

public record Join(From from, List<Predicate> on) {}
