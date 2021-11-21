package typed;

import java.util.function.Function;
import java.util.stream.Stream;

public sealed interface TypedQuery<T> {}
record From<T>(Class<T> t) implements TypedQuery<T> {}
record Select<U, T>(TypedQuery<U> qu, Function<U, T> mapper) implements TypedQuery<T> {}
record Join<U, T>(TypedQuery<U> qu, Function<U, TypedQuery<T>> mapper) implements TypedQuery<T> {}

interface TypedApi {
    <T> TypedQuery<T> from(Class<T> t); // bind(T t)
    <U, T> TypedQuery<T> select(TypedQuery<U> qu, Function<U, T> mapper); // map
    <U, T> TypedQuery<T> join(TypedQuery<U> qu, Function<U, TypedQuery<T>> mapper); // flatMap
    <U, T> Stream<T> fetch(TypedQuery<T> q); // run
}

class TypedApiImpl implements TypedApi {
    private final Repo repo;

    TypedApiImpl(Repo repo) {
        this.repo = repo;
    }

    @Override
    public <T> TypedQuery<T> from(Class<T> t) {
        return new From<>(t);
    }
    @Override
    public <U, T> TypedQuery<T> select(TypedQuery<U> qu, Function<U, T> mapper) {
        return new Select<>(qu, mapper);
    }
    @Override
    public <U, T> TypedQuery<T> join(TypedQuery<U> qu, Function<U, TypedQuery<T>> mapper) {
        return new Join<>(qu, mapper);
    }
    @Override
    public <U, T> Stream<T> fetch(TypedQuery<T> q) {
        /*return switch (q) {
            case From<T> t -> repo.instancesOf(t);
            case Select<U, T> select -> fetch(select.qu()).map(select.mapper());
            case Join<U, T> select -> fetch(select.qu()).flatMap(u -> fetch(select.mapper().apply(u)));
        };*/

        if (q instanceof From) {
            return repo.instancesOf((From<T>) q);
        }
        if (q instanceof Select) {
            Select<U, T> casted = (Select<U, T>) q;
            return fetch(casted.qu()).map(casted.mapper());
        }
        if (q instanceof Join) {
            Join<U, T> casted = (Join<U, T>) q;
            return fetch(casted.qu()).flatMap(u -> fetch(casted.mapper().apply(u)));
        }

        throw new IllegalArgumentException();
    }
}

