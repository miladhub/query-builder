package query;

import io.vavr.collection.List;

public class JoinBuilder
{
    public static JoinBuilder type(EntityType from) {
        return new JoinBuilder(new From(from));
    }

    private final From from;
    private List<Predicate> on = List.of();

    private JoinBuilder(From from)
    {
        this.from = from;
    }

    public JoinBuilder on(PredicateBuilder on) {
        this.on = this.on.append(on.build());
        return this;
    }

    public Join build()
    {
        return new Join(from, on);
    }
}
