package query;

import io.vavr.collection.List;

public class GroupByBuilder
{
    private final List<Term> groupBy;
    private List<Predicate> having = List.of();

    public GroupByBuilder(List<Term> groupBy)
    {
        this.groupBy = groupBy;
    }

    public GroupByBuilder having(Predicate... having)
    {
        this.having = List.of(having);
        return this;
    }

    public GroupByBuilder and(AttrTerm l, Op o, Term r)
    {
        this.having = having.append(new BinOp(l, o, r));
        return this;
    }

    public GroupByBuilder and(Predicate pred)
    {
        this.having = having.append(pred);
        return this;
    }

    public List<Term> groupBy()
    {
        return groupBy;
    }

    public List<Predicate> having()
    {
        return having;
    }
}
