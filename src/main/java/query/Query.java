package query;

import io.vavr.collection.List;

public record Query(
        List<SelectTerm> select,
        From from,
        List<Predicate> where,
        List<Join> joins,
        List<AttrSelectTerm> groupBy,
        List<Predicate> having,
        List<OrderBy> orderBy
)
{}
