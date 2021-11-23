package query;

import io.vavr.collection.List;

public record Select(List<Term> select, Query from) {}
