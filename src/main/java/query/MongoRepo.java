package query;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

@SuppressWarnings("ClassCanBeRecord")
public class MongoRepo
        implements Repository
{
    private final MongoDatabase db;

    public MongoRepo(MongoDatabase db) {
        this.db = db;
    }

    @Override
    public void init(EntityType... types) {
        List.of(types).forEach(t -> {
            db.getCollection(t.name()).drop();
            db.createCollection(t.name());
        });
    }

    @Override
    public void addEntities(Entity... es) {
        List.of(es).forEach(e -> {
            MongoCollection<Document> coll =
                    db.getCollection(e.type().name());
            coll.insertOne(toDoc(e));
        });
    }

    private Document toDoc(Entity e) {
        Document doc = new Document("_id", e.id());
        e.attrs()
                .forEach(a -> {
                    Option<Object> value = attrValue(e, a.attr());
                    if (value.isDefined())
                        doc.append(a.attr().name(), value.get());
                });
        return doc;
    }

    private Option<Object> attrValue(
            Entity e,
            Attr attr
    ) {
        return e.attrs()
                .find(av -> av.attr().equals(attr))
                .map(AttrValue::value);
    }

    @Override
    public Try<List<List<Object>>> select(Query q) {
        return Try.of(() -> query(q));
    }

    private List<List<Object>> query(Query q) {
        MongoCollection<Document> coll =
                db.getCollection(q.from().et().name());

        Bson include = include(q.select()
                .filter(t -> t instanceof AttrSelectTerm)
                .map(at -> ((AttrSelectTerm) at).attr().name())
                .toJavaList());
        Bson projection = fields(include, excludeId());

        java.util.List<Bson> pipeline = new ArrayList<>(Arrays.asList(
                Aggregates.match(toFiltersDoc(q.where())),
                Aggregates.project(projection)
        ));

        if (!q.orderBy().isEmpty())
            pipeline.add(Aggregates.sort(toSortDoc(q.orderBy())));

        AggregateIterable<Document> find = coll.aggregate(pipeline);

        ArrayList<Document> l = new ArrayList<>();
        find.into(l);
        return List.ofAll(l).map(d -> List.ofAll(d.values()));
    }

    private Bson toSortDoc(List<OrderBy> orderBy) {
        return !orderBy.isEmpty()
                ? Sorts.orderBy(orderBy.map(ob -> switch (ob.mode()) {
                    case ASC -> Sorts.ascending(ob.t().attr().name());
                    case DESC -> Sorts.descending(ob.t().attr().name());
                }).toJavaList())
                : new Document();
    }

    private Bson toFiltersDoc(List<Predicate> where) {
        return !where.isEmpty()
                ? and(where.map(this::toFilterDoc))
                : new Document();
    }

    private Bson toFilterDoc(Predicate p) {
        return switch (p) {
            case BinOp bo -> switch (bo.op()) {
                case EQ -> eq(bo.left().attr().name(), toTermDoc(bo.right()));
                case LT -> lt(bo.left().attr().name(), toTermDoc(bo.right()));
                case GT -> gt(bo.left().attr().name(), toTermDoc(bo.right()));
                case LIKE -> throw new UnsupportedOperationException("like");
            };
            case And and -> and(toFilterDoc(and.left()), toFilterDoc(and.right()));
            case Or or -> or(toFilterDoc(or.left()), toFilterDoc(or.right()));
            case Not not -> not(toFilterDoc(not.predicate()));
        };
    }

    private Object toTermDoc(ClauseTerm term) {
        return switch (term) {
            case AttrClauseTerm at -> at.attr().name();
            case Null ignored -> null;
            case Value v -> v.value();
        };
    }
}
