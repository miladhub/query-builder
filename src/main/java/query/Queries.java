package query;


import io.vavr.collection.List;

import static query.Entities.clauseAttr;

public class Queries
{
    public static QueryBuilder select(SelectTerm... terms) {
        return new QueryBuilder(List.of(terms));
    }

    public static PredicateBuilder pred(AttrClauseTerm l, Op op, ClauseTerm r) {
        return new PredicateBuilder(l, op, r);
    }

    public static PredicateBuilder pred(Attr l, Op op, ClauseTerm r) {
        return new PredicateBuilder(clauseAttr(l), op, r);
    }

    public static PredicateBuilder pred(Attr l, Op op, Attr r) {
        return new PredicateBuilder(clauseAttr(l), op, clauseAttr(r));
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

    public static ClauseTerm value(Object value)
    {
        return new Value(value);
    }

    public static ClauseTerm nullVal()
    {
        return new Null();
    }

    public static OrderBy by(AttrSelectTerm t, OrderByMode mode) {
        return new OrderBy(t, mode);
    }
}
