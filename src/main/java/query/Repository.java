package query;

import io.vavr.collection.List;
import io.vavr.control.Try;

public interface Repository {
    void init(EntityType... types);
    void addEntities(Entity... es);
    Try<List<List<Object>>> select(Query q);
}
