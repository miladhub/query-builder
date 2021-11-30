package query;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BsonField;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

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
        List.of(types)
                .map(t -> db.getCollection(t.name()))
                .forEach(MongoCollection::drop);
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
                .map(this::toFieldName)
                .toJavaList());
        Bson projection = fields(include, excludeId());

        java.util.List<Bson> pipeline = new ArrayList<>();

        if (!q.where().isEmpty())
            pipeline.add(match(toFiltersDoc(q.where())));

        if (!q.joins().isEmpty())
            pipeline.addAll(toJoinDocs(q).toJavaList());

        if (!q.groupBy().isEmpty())
            pipeline.addAll(toGroupByDocs(q).toJavaList());

        pipeline.add(project(projection));

        if (!q.orderBy().isEmpty())
            pipeline.add(sort(toSortDoc(q.orderBy())));

        AggregateIterable<Document> find = coll.aggregate(pipeline);

        MongoIterable<List<Object>> map =
                find.map(d -> q.select().map(s -> d.get(toFieldName(s))));
        ArrayList<List<Object>> l = new ArrayList<>();
        map.into(l);
        return List.ofAll(l);
    }

    private String toFieldName(SelectTerm at) {
        return switch (at) {
            case AttrSelectTerm ast -> ast.attr().name();
            case Aggregation aggr -> aggr.at().name().toLowerCase() +
                    "_" +
                    aggr.t().attr().name();
        };
    }

    private List<Bson> toJoinDocs(Query q) {
        return q.joins().flatMap(join -> toJoinDoc(q.from(), join));
    }

    private List<Bson> toJoinDoc(
            From from,
            Join join
    ) {
        String joinedColl = join.from().et().name();
        String joinName = from.et().name() + "_" + joinedColl;

        if (join.on().size() != 1
                || !(join.on().get(0) instanceof BinOp on)
                || !(((BinOp) join.on().get(0)).right() instanceof AttrClauseTerm foreign))
            throw new UnsupportedOperationException();

        String localField = on.left().attr().name();
        String foreignField = foreign.attr().name();

        return List.of(
                lookup(joinedColl, localField, foreignField, joinName),
                match(ne(joinName, java.util.List.of())),
                new Document(
                        "$addFields",
                        new Document(joinName,
                                new Document("$arrayElemAt",
                                        Arrays.asList("$" + joinName, 0)))),
                replaceRoot(
                        new Document("$mergeObjects",
                                Arrays.asList("$" + joinName, "$$ROOT"))),
                new Document("$project", new Document(joinName, 0))
        );
    }

    private List<Bson> toGroupByDocs(Query q) {
        List<Tuple2<Aggregation, Attr>> groups = q.groupBy()
                .flatMap(gb -> q.select()
                        .filter(s -> s instanceof Aggregation)
                        .map(s -> (Aggregation) s)
                        .map(m -> Tuple.of(m, gb)));
        return groups
                .flatMap(this::toGroupByDoc);
    }

    private List<Bson> toGroupByDoc(Tuple2<Aggregation, Attr> g) {
        String aggregatedFieldName = g._1.t().attr().name();
        String groupedByFieldName = g._2.name();
        String aggrModeName = g._1.at().name().toLowerCase();

        BsonField aggrField = switch (g._1.at()) {
            case SUM -> sum(aggrModeName + "_" + aggregatedFieldName,
                    "$" + aggregatedFieldName);
            case MAX -> max(aggrModeName + "_" + aggregatedFieldName,
                    "$" + aggregatedFieldName);
            case MIN -> min(aggrModeName + "_" + aggregatedFieldName,
                    "$" + aggregatedFieldName);
            case AVG -> avg(aggrModeName + "_" + aggregatedFieldName,
                    "$" + aggregatedFieldName);
            case COUNT -> sum(aggrModeName + "_" + aggregatedFieldName, 1);
        };

        return List.of(
                group("$" + groupedByFieldName,
                        aggrField),
                project(fields(
                        include(aggrModeName + "_" + aggregatedFieldName),
                        new BsonDocument(groupedByFieldName, new BsonString("$_id")),
                        excludeId()))
        );
    }

    private Bson toSortDoc(List<OrderBy> orderBy) {
        return orderBy(orderBy.map(ob -> switch (ob.mode()) {
                    case ASC -> ascending(ob.t().attr().name());
                    case DESC -> descending(ob.t().attr().name());
                }).toJavaList());
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
