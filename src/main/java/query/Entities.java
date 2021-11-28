package query;

import io.vavr.collection.List;

public class Entities
{
    public static Entity newEntity(
            EntityType type,
            String id,
            AttrValue... attrs
    )
    {
        return new Entity(type, id, List.of(attrs));
    }

    public static Attr attr(
            AttrType type,
            String name
    )
    {
        return new Attr(type, name);
    }

    public static AttrSelectTerm attr(Attr attr)
    {
        return new AttrSelectTerm(attr);
    }

    public static AttrClauseTerm clauseAttr(Attr attr)
    {
        return new AttrClauseTerm(attr);
    }

    public static SelectTerm max(AttrSelectTerm t) {
        return new Max(t);
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
        return new EntityType(name, List.of(attrs));
    }
}
