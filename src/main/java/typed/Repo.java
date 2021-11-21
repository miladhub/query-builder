package typed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Repo {
    private final Map<Class<?>, List<Object>> objs = new HashMap<>();

    public void add(Object o) {
        List<Object> before = objs.getOrDefault(o.getClass(), new ArrayList<>());
        before.add(o);
        objs.put(o.getClass(), before);
    }

    @SuppressWarnings("unchecked")
    <T> Stream<T> instancesOf(From<T> from) {
        return ((List<T>) objs.get(from.t())).stream();
    }
}
