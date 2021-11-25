package query;

import io.vavr.collection.List;
import io.vavr.control.Try;

public interface Repository {
    Try<List<List<Object>>> select(Query q);
}
