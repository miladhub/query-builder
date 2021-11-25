package query;

import io.vavr.collection.List;

import static query.Queries.pred;

public class SelectBuilder
{
    private final List<Term> select;
    private From from;
    private List<Predicate> where = List.of();
    private List<Join> joins = List.of();
    private GroupByBuilder groupBy = new GroupByBuilder(List.of());
    private List<OrderBy> orderBy = List.of();

    public SelectBuilder(List<Term> select)
    {
        this.select = select;
    }

    public SelectBuilder from(EntityType from)
    {
        this.from = new From(from);
        return this;
    }

    public SelectBuilder from(From from)
    {
        this.from = from;
        return this;
    }

    public SelectBuilder where(PredicateBuilder pb) {
        this.where = List.of(pb.build());
        return this;
    }


    public SelectBuilder where(Predicate p) {
        this.where = List.of(p);
        return this;
    }

    public SelectBuilder where(Term l, Op o, Term r)
    {
        this.where = List.of(pred(l, o, r).build());
        return this;
    }

    public SelectBuilder and(Term l, Op o, Term r)
    {
        this.where = where.append(pred(l, o, r).build());
        return this;
    }

    public SelectBuilder and(PredicateBuilder pb)
    {
        this.where = where.append(pb.build());
        return this;
    }

    public SelectBuilder join(JoinBuilder join)
    {
        this.joins = joins.append(join.build());
        return this;
    }

    public SelectBuilder groupBy(Term... groupBy)
    {
        this.groupBy = new GroupByBuilder(List.of(groupBy));
        return this;
    }

    public SelectBuilder order(OrderBy... orderBy)
    {
        this.orderBy = List.of(orderBy);
        return this;
    }

    public Select build()
    {
        return new Select(select, new Query(
                from, where, joins, groupBy.groupBy(), groupBy.having(), orderBy
        ));
    }
}
