package query;

public class PredicateBuilder
{
    private Predicate pred;

    private PredicateBuilder(Predicate pred) {
        this.pred = pred;
    }

    public PredicateBuilder(AttrTerm l, Op op, Term r) {
        this.pred = new BinOp(l, op, r);
    }

    public static PredicateBuilder not(PredicateBuilder pred) {
        return new PredicateBuilder(new Not(pred.build()));
    }

    public static PredicateBuilder either(
            PredicateBuilder l,
            PredicateBuilder r
    ) {
        return new PredicateBuilder(new Or(l.build(), r.build()));
    }

    public static PredicateBuilder allOf(
            PredicateBuilder l,
            PredicateBuilder r
    ) {
        return new PredicateBuilder(new And(l.build(), r.build()));
    }

    public Predicate build()
    {
        return pred;
    }
}
