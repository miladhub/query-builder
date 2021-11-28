package query;

import io.vavr.collection.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static query.AttrType.*;
import static query.Entities.*;
import static query.OrderByMode.*;
import static query.PredicateBuilder.*;
import static query.Queries.*;

@RunWith(Parameterized.class)
public class QueryTest
{
    private final H2Repo repo;

    public QueryTest(H2Repo repo) {
        this.repo = repo;
    }

    @Parameterized.Parameters
    public static java.util.List<Repository> data() {
        return Arrays.asList(
                new H2Repo()
        );
    }

    private final Attr foo_str = attr(Str, "foo_str");
    private final Attr foo_int = attr(Int, "foo_int");
    private final EntityType foo = Entities.newEntityType(
            "foo", foo_str, foo_int);
    private final Entity foo_1 = newEntity(foo, "foo_1", strValue(foo_str,
            "foo_str_1"), intValue(foo_int, 42));
    private final Entity foo_2 = newEntity(foo, "foo_2", strValue(foo_str,
            "foo_str_2"), intValue(foo_int, 43));
    private final Entity foo_3 = newEntity(foo, "foo_3", strValue(foo_str,
            "foo_str_2"), intValue(foo_int, 44));

    private final Attr bar_int = attr(Int, "bar_int");
    private final Attr bar_str = attr(Str, "bar_str");
    private final EntityType bar = Entities.newEntityType(
            "bar", bar_int, bar_str);
    private final Entity bar_1 = newEntity(bar, "bar_1", strValue(bar_str,
            "bar_str_2"), intValue(bar_int, 44));
    private final Entity bar_2 = newEntity(bar, "bar_2", strValue(bar_str,
            "bar_str_3"), intValue(bar_int, 42));

    @Before
    public void setUp()
    {
        repo.init(foo, bar);
        repo.addEntities(foo_1, foo_2, bar_1, bar_2);
    }

    @Test
    public void select_str_from_foo()
    {
        QueryBuilder query =
                select(attr(foo_str))
                        .from(type(foo));
        assertThat(
                fetch(query),
                contains(contains("foo_str_1"), contains("foo_str_2")));
    }

    @Test
    public void select_int_from_foo()
    {
        assertThat(
                fetch(select(attr(foo_str), attr(foo_int)).from(foo)),
                contains(contains("foo_str_1", 42), contains("foo_str_2", 43)));
    }

    @Test
    public void select_int_from_foo_order_by()
    {
        assertThat(
                fetch(select(attr(foo_str), attr(foo_int))
                              .from(foo)
                              .order(by(attr(foo_int), DESC))),
                contains(contains("foo_str_2", 43), contains("foo_str_1", 42)));
    }

    @Test
    public void select_group_by()
    {
        repo.addEntities(foo_3);

        assertThat(
                fetch(select(attr(foo_str), max(attr(foo_int)))
                              .from(foo)
                              .groupBy(attr(foo_str))
                              .order(by(attr(foo_str), ASC))),
                contains(contains("foo_str_1", 42),
                         contains("foo_str_2", Math.max(43, 44))));
    }

    @Test
    public void select_from_foo_where_lt()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(foo_int))
                        .from(foo)
                        .where(clauseAttr(foo_str), like(), value("foo_str_%"))
                        .and(clauseAttr(foo_int), lt(), value(43));

        assertThat(
                fetch(query),
                contains(contains("foo_str_1", 42)));
    }

    @Test
    public void select_from_foo_where_gt()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(foo_int))
                        .from(foo)
                        .where(clauseAttr(foo_str), like(), value("foo_str_%"))
                        .and(clauseAttr(foo_int), gt(), value(41));

        assertThat(
                fetch(query),
                contains(contains("foo_str_1", 42), contains("foo_str_2", 43)));
    }

    @Test
    public void select_from_foo_where_or()
    {
        QueryBuilder query =
                select(attr(foo_str))
                        .from(foo)
                        .where(either(
                                pred(clauseAttr(foo_str), like(), value("bar%")),
                                not(pred(clauseAttr(foo_int), eq(), nullVal()))));

        assertThat(
                fetch(query),
                contains(contains("foo_str_1"), contains("foo_str_2")));
    }

    @Test
    public void select_from_foo_join_bar_on_int()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(bar_str))
                        .from(foo)
                        .join(JoinBuilder.type(bar)
                                .on(pred(clauseAttr(foo_int), eq(), clauseAttr(bar_int))));

        assertThat(
                fetch(query),
                contains(contains("foo_str_1", "bar_str_3")));
    }

    @Test
    public void select_from_foo_join_bar_where()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(bar_str))
                        .from(foo)
                        .where(clauseAttr(foo_int), lt(), value(43))
                        .join(JoinBuilder.type(bar)
                                .on(pred(clauseAttr(foo_int), eq(), clauseAttr(bar_int))));

        assertThat(
                fetch(query),
                contains(contains("foo_str_1", "bar_str_3")));
    }

    private List<List<Object>> fetch(QueryBuilder query)
    {
        return repo.select(query.build()).get();
    }
}
