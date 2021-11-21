package query;

import io.vavr.control.Try;

import java.util.List;

public interface Repository {
    Try<List<Entity>> fetch(Query q);
    Try<List<List<Object>>> fetch(SelectQuery s);
}
