package typed;

import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TypedQueryTest {
    private final Repo repo = new Repo();
    private final TypedApiImpl api = new TypedApiImpl(repo);

    @Test
    public void doubles() {
        repo.add(1);
        repo.add(2);
        repo.add(3);

        TypedQuery<Integer> select = api.select(api.from(Integer.class), i -> i * 2);

        assertEquals(
                Arrays.asList(2, 4, 6),
                api.fetch(select).collect(Collectors.toList()));
    }

    @Test
    public void join() {
        repo.add(1);
        repo.add(2);
        repo.add(3);
        repo.add("1");
        repo.add("2");
        repo.add("3");

        TypedQuery<Integer> select = api.select(
                api.from(Integer.class), i -> i * 2);

        TypedQuery<String> join = api.join(
                api.from(Integer.class),
                i -> api.select(api.from(String.class), String::toUpperCase));

        System.out.println(api.fetch(join).collect(Collectors.toList()));
    }
}