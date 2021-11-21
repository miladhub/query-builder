package query;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static query.AttrType.*;
import static query.Factory.*;

public class QueryTest
{
    private final H2Repo repo = new H2Repo();

    private final Attr foo_str = attr(Str, "foo_str");
    private final Attr foo_int = attr(Int, "foo_int");
    private final EntityType foo = Factory.newEntityType(
            "foo", foo_str, foo_int);
    private final Entity foo_1 = newEntity(foo, "foo_1", strValue(foo_str,
            "foo_str_1"), intValue(foo_int, 42));
    private final Entity foo_2 = newEntity(foo, "foo_2", strValue(foo_str,
            "foo_str_2"), intValue(foo_int, 43));

    private final Attr bar_int = attr(Int, "bar_int");
    private final Attr bar_str = attr(Str, "bar_str");
    private final EntityType bar = Factory.newEntityType(
            "bar", bar_int, bar_str);
    private final Entity bar_1 = newEntity(bar, "bar_1", strValue(bar_str,
            "str_2"), intValue(bar_int, 44));
    private final Entity bar_2 = newEntity(bar, "bar_2", strValue(bar_str,
            "str_3"), intValue(bar_int, 42));

    @Before
    public void setUp()
    throws
            Exception
    {
        repo.init(foo, bar);
        repo.addEntities(foo_1, foo_2, bar_1, bar_2);
    }

    @Test
    public void all_from_et1()
    {
        assertThat(
                fetch(from(foo)),
                contains(foo_1, foo_2));
    }

    @Test
    public void select_from_et1_str()
    {
        assertThat(
                fetch(select(from(foo), attr(foo_str))),
                contains(contains("foo_str_1"), contains("foo_str_2")));
    }

    @Test
    public void select_from_et1_str_int()
    {
        assertThat(
                fetch(select(from(foo), attr(foo_str), attr(foo_int))),
                contains(contains("foo_str_1", 42), contains("foo_str_2", 43)));
    }

    @Test
    public void select_from_et1_where()
    {
        SelectQuery select =
                select(
                        from(foo,
                                where(
                                        pred(attr(foo_str), like(), value(
                                                "Foo%")),
                                        pred(attr(foo_int), lt(), value(102))
                                )),
                        attr(foo_str), attr(foo_int));

        assertThat(
                fetch(select),
                contains("foo_str_1", "foo_str_2"));
    }

    @Test
    public void select_from_et1_where_or()
    {
        SelectQuery query =
                select(
                        from(foo,
                                where(
                                        or(
                                                pred(attr(foo_str), like(),
                                                        value("Foo%")),
                                                pred(attr(foo_int), lt(),
                                                        nullVal()))
                                )),
                        attr(foo_str));

        assertThat(
                fetch(query),
                contains("foo_str_1", "foo_str_2"));
    }

    @Test
    public void all_from_et1_join_et2_on_str()
    {
        assertThat(
                fetch(
                        join(
                                from(foo),
                                from(bar),
                                on(
                                        pred(attr(foo_str), eq(),
                                                attr(bar_str))))),
                contains(pair(foo_2, bar_1)));
    }

    @Test
    public void all_from_et1_join_et2_on_int()
    {
        assertThat(
                fetch(
                        join(
                                from(foo),
                                from(bar),
                                where(
                                        pred(attr(foo_int), eq(),
                                                attr(bar_int))))),
                contains(pair(foo_1, bar_2)));
    }

    @Test
    public void all_from_et1_join_et2_on_where()
    {
        assertThat(
                fetch(
                        join(
                                from(foo),
                                from(bar),
                                on(
                                        pred(attr(foo_int), eq(),
                                                attr(bar_int))),
                                where(
                                        pred(attr(foo_str), gt(), value(42))))),
                contains(pair(foo_1, bar_2)));
    }

    @Test
    public void select_from_et1_join_et2()
    {
        assertThat(
                fetch(
                        select(
                                join(
                                        from(foo), from(bar), where(
                                                pred(attr(foo_str), eq(),
                                                        attr(bar_str)))),
                                attr(foo_str), attr(bar_int))),
                contains(pair("str_1", 42)));
    }

    @Test
    public void select_from_et1_join_et2_where()
    {

        assertThat(
                fetch(select(
                        join(from(foo), from(bar), on(pred(attr(foo_str), eq(), attr(bar_str)))),
                        attr(foo_str), attr(bar_int))),
                contains(pair("str_1", 42)));
    }

    private List<Entity> fetch(Query query)
    {
        return repo.fetch(query).get();
    }

    private List<List<Object>> fetch(SelectQuery query)
    {
        return repo.fetch(query).get();
    }
}