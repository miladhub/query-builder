package query;

import java.util.List;

public record SelectQuery(Query from, List<Term> select) {}
