package query;


import io.vavr.collection.List;

public class Queries
{
    public static QueryBuilder select(Term... terms) {
        return new QueryBuilder(List.of(terms));
    }

    public static From type(EntityType et) {
        return new From(et);
    }

    public static PredicateBuilder pred(AttrTerm l, Op op, Term r) {
        return new PredicateBuilder(l, op, r);
    }

    public static Op eq()
    {
        return Op.EQ;
    }

    public static Op lt()
    {
        return Op.LT;
    }

    public static Op gt()
    {
        return Op.GT;
    }

    public static Op like()
    {
        return Op.LIKE;
    }

    public static Term value(Object value)
    {
        return new Value(value);
    }

    public static Term nullVal()
    {
        return new Null();
    }

    public static OrderBy by(Term t, OrderByMode mode) {
        return new OrderBy(t, mode);
    }
}
