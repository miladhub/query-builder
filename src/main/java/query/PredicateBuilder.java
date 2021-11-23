package query;

public class PredicateBuilder
{
    private Predicate pred;

    public PredicateBuilder(Term l, Op op, Term r) {
        this.pred = new BinOp(l, op, r);
    }

    public PredicateBuilder and(PredicateBuilder and) {
        this.pred = new And(pred, and.build());
        return this;
    }

    public PredicateBuilder or(PredicateBuilder or) {
        this.pred = new Or(pred, or.build());
        return this;
    }

    public PredicateBuilder not() {
        this.pred = new Not(pred);
        return this;
    }

    public Predicate build()
    {
        return pred;
    }
}
