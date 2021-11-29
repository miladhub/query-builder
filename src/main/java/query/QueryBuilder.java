package query;

import io.vavr.collection.List;

import static query.Queries.pred;

public class QueryBuilder
{
    private final List<SelectTerm> select;
    private From from;
    private List<Predicate> where = List.of();
    private List<Join> joins = List.of();
    private GroupByBuilder groupBy = new GroupByBuilder(List.of());
    private List<OrderBy> orderBy = List.of();

    public QueryBuilder(List<SelectTerm> select)
    {
        this.select = select;
    }

    public QueryBuilder from(EntityType from)
    {
        this.from = new From(from);
        return this;
    }

    public QueryBuilder from(From from)
    {
        this.from = from;
        return this;
    }

    public QueryBuilder where(PredicateBuilder pb) {
        this.where = List.of(pb.build());
        return this;
    }


    public QueryBuilder where(Predicate p) {
        this.where = List.of(p);
        return this;
    }

    public QueryBuilder where(AttrClauseTerm l, Op o, ClauseTerm r)
    {
        this.where = List.of(pred(l, o, r).build());
        return this;
    }

    public QueryBuilder and(AttrClauseTerm l, Op o, ClauseTerm r)
    {
        this.where = where.append(pred(l, o, r).build());
        return this;
    }

    public QueryBuilder and(PredicateBuilder pb)
    {
        this.where = where.append(pb.build());
        return this;
    }

    public QueryBuilder join(JoinBuilder join)
    {
        this.joins = joins.append(join.build());
        return this;
    }

    public QueryBuilder groupBy(Attr... groupBy)
    {
        this.groupBy = new GroupByBuilder(List.of(groupBy));
        return this;
    }

    public QueryBuilder order(OrderBy... orderBy)
    {
        this.orderBy = List.of(orderBy);
        return this;
    }

    public Query build() {
        return new Query(
                select, from, where, joins,
                groupBy.groupBy(), groupBy.having(), orderBy
        );
    }
}
