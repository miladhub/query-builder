package query;

import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class Factory
{
    public static Entity newEntity(
            EntityType type,
            String id
    )
    {
        return new Entity(type, id, Collections.emptyList());
    }

    public static Entity newEntity(
            EntityType type,
            String id,
            AttrValue... attrs
    )
    {
        return new Entity(type, id, asList(attrs));
    }

    public static Attr attr(
            AttrType type,
            String name
    )
    {
        return new Attr(type, name);
    }

    public static Term attr(Attr attr)
    {
        return new AttrTerm(attr);
    }

    public static AttrValue strValue(
            Attr attr,
            String value
    )
    {
        return new StrAttrValue(attr, value);
    }

    public static AttrValue intValue(
            Attr attr,
            int value
    )
    {
        return new IntAttrValue(attr, value);
    }

    public static EntityType newEntityType(
            String name,
            Attr... attrs
    )
    {
        return new EntityType(name, asList(attrs));
    }

    public static SelectQuery select(
            Query from,
            Term... attrs
    )
    {
        return new SelectQuery(from, asList(attrs));
    }

    public static From from(EntityType from)
    {
        return new From(from, Collections.emptyList());
    }

    public static From from(
            EntityType from,
            List<Predicate> where
    )
    {
        return new From(from, where);
    }

    public static Query join(
            Query left,
            Query right,
            List<Predicate> on
    )
    {
        return new Join(left, right, on, Collections.emptyList());
    }

    public static Query join(
            Query left,
            Query right,
            List<Predicate> on,
            List<Predicate> where
    )
    {
        return new Join(left, right, on, where);
    }

    public static List<Predicate> where(Predicate... predicates)
    {
        return asList(predicates);
    }

    public static Predicate and(
            Predicate left,
            Predicate right
    )
    {
        return new And(left, right);
    }

    public static Predicate or(
            Predicate left,
            Predicate right
    )
    {
        return new Or(left, right);
    }

    public static Predicate not(Predicate predicate)
    {
        return new Not(predicate);
    }

    public static Predicate pred(
            Term left,
            Op op,
            Term right
    )
    {
        return new BinOp(left, op, right);
    }

    public static Predicate on(
            Term left,
            Op op,
            Term right
    )
    {
        return new BinOp(left, op, right);
    }

    public static List<Predicate> on(Predicate... predicates)
    {
        return asList(predicates);
    }

    public static <T, U> Tuple2<T, U> pair(
            T left,
            U right
    )
    {
        return Tuple.of(left, right);
    }

    public static Op eq()
    {
        return new Eq();
    }

    public static Op lt()
    {
        return new Lt();
    }

    public static Op gt()
    {
        return new Gt();
    }

    public static Op like()
    {
        return new Like();
    }

    public static Term value(Object value)
    {
        return new Value(value);
    }

    public static Term nullVal()
    {
        return new Null();
    }
}
