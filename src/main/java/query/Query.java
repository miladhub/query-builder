package query;

import io.vavr.collection.List;

public record Query(
        List<Term> select,
        From from,
        List<Predicate> where,
        List<Join> joins,
        List<Term> groupBy,
        List<Predicate> having,
        List<OrderBy> orderBy
)
{}
