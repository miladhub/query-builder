package query;

import io.vavr.collection.List;

public record Query(
        From from,
        List<Predicate> where,
        List<Join> joins,
        List<Term> groupBy,
        List<Predicate> having,
        List<OrderBy> orderBy
)
{}
