package querytest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.vavr.collection.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import query.*;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static query.AttrType.Int;
import static query.AttrType.Str;
import static query.Entities.*;
import static query.JoinBuilder.type;
import static query.OrderByMode.ASC;
import static query.OrderByMode.DESC;
import static query.PredicateBuilder.either;
import static query.PredicateBuilder.not;
import static query.Queries.*;

@RunWith(Parameterized.class)
public class QueryTest
{
    private static final MongoClient mongoClient = MongoClients.create();
    private final Repository repo;

    public QueryTest(String ignored, Repository repo) {
        this.repo = repo;
    }

    @Parameterized.Parameters(name = "{0}")
    public static java.util.List<Object[]> data() {
        return Arrays.asList(
                new Object[]{"H2", new H2Repo()},
                new Object[]{"MongoDB",
                        new MongoRepo(mongoClient.getDatabase("test"))}
        );
    }

    @AfterClass
    public static void afterClass() {
        mongoClient.close();
    }

    private final Attr foo_str = attr(Str, "foo_str");
    private final Attr foo_cat = attr(Str, "foo_cat");
    private final Attr foo_int = attr(Int, "foo_int");
    private final EntityType foo = Entities.newEntityType(
            "foo", foo_str, foo_int, foo_cat);
    private final Entity foo_1 = newEntity(foo, "foo_1",
            strValue(foo_str, "str_1"), 
            intValue(foo_int, 42),
            strValue(foo_cat, "cat_1"));
    private final Entity foo_2 = newEntity(foo, "foo_2",
            strValue(foo_str, "str_2"),
            intValue(foo_int, 43),
            strValue(foo_cat, "cat_2"));
    private final Entity foo_3 = newEntity(foo, "foo_3",
            strValue(foo_str, "str_2"),
            intValue(foo_int, 57),
            strValue(foo_cat, "cat_2"));

    private final Attr bar_int = attr(Int, "bar_int");
    private final Attr bar_cat = attr(Str, "bar_cat");
    private final Attr bar_str = attr(Str, "bar_str");
    private final EntityType bar = Entities.newEntityType(
            "bar", bar_int, bar_str, bar_cat);
    private final Entity bar_1 = newEntity(bar, "bar_1", 
            strValue(bar_str, "str_2"),
            intValue(bar_int, 44),
            strValue(bar_cat, "cat_1"));
    private final Entity bar_2 = newEntity(bar, "bar_2",
            strValue(bar_str, "str_3"),
            intValue(bar_int, 42),
            strValue(bar_cat, "cat_2"));

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
                        .from(foo);
        assertThat(
                fetch(query),
                contains(contains("str_1"), contains("str_2")));
    }

    @Test
    public void select_int_from_foo()
    {
        assertThat(
                fetch(select(attr(foo_str), attr(foo_int)).from(foo)),
                contains(contains("str_1", 42), contains("str_2", 43)));
    }

    @Test
    public void select_int_from_foo_order_by()
    {
        assertThat(
                fetch(select(attr(foo_str), attr(foo_int))
                              .from(foo)
                              .order(by(attr(foo_int), DESC))),
                contains(contains("str_2", 43), contains("str_1", 42)));
    }

    @Test
    public void select_max_group_by()
    {
        repo.addEntities(foo_3);

        assertThat(
                fetch(select(attr(foo_str), max(attr(foo_int)))
                              .from(foo)
                              .groupBy(foo_str)
                              .order(by(attr(foo_str), ASC))),
                contains(contains("str_1", 42),
                         contains("str_2", Math.max(43, 57))));
    }

    @Test
    public void select_min_group_by()
    {
        repo.addEntities(foo_3);

        assertThat(
                fetch(select(attr(foo_str), min(attr(foo_int)))
                              .from(foo)
                              .groupBy(foo_str)
                              .order(by(attr(foo_str), ASC))),
                contains(contains("str_1", 42),
                         contains("str_2", Math.min(43, 57))));
    }

    @Test
    public void select_sum_group_by()
    {
        repo.addEntities(foo_3);

        assertThat(
                fetch(select(attr(foo_str), sum(attr(foo_int)))
                        .from(foo)
                        .groupBy(foo_str)
                        .order(by(attr(foo_str), ASC))),
                contains(contains("str_1", 42),
                        contains("str_2", 43 + 57)));
    }

    @Test
    public void select_avg_group_by()
    {
        repo.addEntities(foo_3);

        List<List<Object>> fetch = fetch(select(attr(foo_str),
                avg(attr(foo_int)))
                .from(foo)
                .groupBy(foo_str)
                .order(by(attr(foo_str), ASC)));
        assertEquals("str_1", fetch.get(0).get(0));
        assertTrue("42", fetch.get(0).get(1).toString().startsWith("42"));
        assertEquals("str_2", fetch.get(1).get(0));
        assertTrue("50", fetch.get(1).get(1).toString().startsWith("50"));
    }

    @Test
    public void select_count_group_by()
    {
        repo.addEntities(foo_3);

        assertThat(
                fetch(select(attr(foo_str), count(attr(foo_int)))
                              .from(foo)
                              .groupBy(foo_str)
                              .order(by(attr(foo_str), ASC))),
                contains(contains("str_1", 1),
                         contains("str_2", 2)));
    }

    @Test
    public void select_from_foo_where_lt()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(foo_int))
                        .from(foo)
                        .where(foo_str, eq(), value("str_1"))
                        .and(foo_int, lt(), value(43));

        assertThat(
                fetch(query),
                contains(contains("str_1", 42)));
    }

    @Test
    public void select_from_foo_where_gt_or()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(foo_int))
                        .from(foo)
                        .where(either(
                                pred(foo_str, eq(), value("str_1")),
                                pred(foo_str, eq(), value("str_2"))))
                        .and(foo_int, gt(), value(41));

        assertThat(
                fetch(query),
                contains(contains("str_1", 42), contains("str_2", 43)));
    }

    @Test
    public void select_from_foo_where_or()
    {
        QueryBuilder query =
                select(attr(foo_str))
                        .from(foo)
                        .where(either(
                                pred(foo_str, eq(), value("foobar")),
                                not(pred(foo_int, eq(), nullVal()))));

        assertThat(
                fetch(query),
                contains(contains("str_1"), contains("str_2")));
    }

    @Test
    public void select_from_foo_join_bar_on_int()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(bar_str))
                        .from(foo)
                        .join(type(bar)
                                .on(pred(foo_int, eq(), bar_int)));

        assertThat(
                fetch(query),
                contains(contains("str_1", "str_3")));
    }

    @Test
    public void select_from_foo_join_bar_on_str()
    {
        repo.addEntities(foo_3);

        QueryBuilder query =
                select(attr(foo_int), attr(bar_int))
                        .from(foo)
                        .join(type(bar)
                                .on(pred(foo_str, eq(), bar_str)));

        assertThat(
                fetch(query),
                contains(contains(43, 44), contains(57, 44)));
    }

    @Test
    public void select_from_foo_join_bar_on_str_group_by_sum()
    {
        repo.addEntities(foo_3);

        QueryBuilder query =
                select(attr(foo_str), sum(attr(foo_int)))
                        .from(foo)
                        .join(type(bar)
                                .on(pred(foo_cat, eq(), bar_cat)))
                        .groupBy(foo_str)
                        .order(by(attr(foo_str), ASC));

        assertThat(
                fetch(query),
                contains(contains("str_1", 42),
                        contains("str_2", 43 + 57)));
    }

    @Test
    public void select_from_foo_join_bar_where()
    {
        QueryBuilder query =
                select(attr(foo_str), attr(bar_str))
                        .from(foo)
                        .where(foo_int, lt(), value(43))
                        .join(type(bar)
                                .on(pred(foo_int, eq(), bar_int)));

        assertThat(
                fetch(query),
                contains(contains("str_1", "str_3")));
    }

    private List<List<Object>> fetch(QueryBuilder query)
    {
        return repo.select(query.build()).get();
    }
}
