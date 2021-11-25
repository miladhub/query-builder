package query;

import io.vavr.collection.List;
import io.vavr.control.Try;

import java.sql.*;
import java.util.ArrayList;

import static java.util.stream.Collectors.joining;

public class H2Repo
        implements Repository
{
    @Override
    public Try<List<List<Entity>>> select(Query q) {
        return Try.of(() -> queryEntities(q));
    }

    @Override
    public Try<List<List<Object>>> select(Select s) {
        return select(s.from()).map(es -> toObjects(s, es));
    }

    private List<List<Object>> toObjects(
            Select sq,
            List<List<Entity>> es
    ) {
        return es.map(e -> project(e, sq.select()));
    }

    private List<Object> project(
            List<Entity> es,
            List<Term> select
    ) {
        return select.map(t ->
                switch (t) {
                    case AttrTerm at -> es.flatMap(Entity::attrs)
                            .filter(a -> a.attr().equals(at.attr()))
                            .map(this::attrValue)
                            .get();
                    case Value v -> v.value();
                    case Null ignored -> null;
                });
    }

    private Object attrValue(AttrValue av) {
        return switch (av) {
            case StrAttrValue sv -> sv.value();
            case IntAttrValue iv -> iv.value();
        };
    }

    private List<List<Entity>> queryEntities(Query q)
    throws SQLException {
        String sql = toSqlQuery(q);

        try (
                Connection c = createConnection();
                PreparedStatement select = c.prepareStatement(sql)
        ) {
            ResultSet rs = select.executeQuery();
            java.util.List<java.util.List<Entity>> rows = new ArrayList<>();

            while (rs.next()) {
                java.util.List<Entity> row = new ArrayList<>();
                row.add(readEntity(q.from().et(), rs));
                for (Join j : q.joins()) {
                    row.add(readEntity(j.from().et(), rs));
                }
                rows.add(row);
            }
            return List.ofAll(rows).map(List::ofAll);
        }
    }

    private String toSqlQuery(Query q) {
        String select = toSqlSelect(q);
        String from = toSqlFrom(q.from());
        String joins = toSqlJoins(q.joins());
        String where = toSqlWhere(q.where());
        String orderBy = toSqlOrderBy(q.orderBy());

        return select + "\n" +
               from + "\n" +
               joins + "\n" +
               where + "\n" +
               orderBy;
    }

    private String toSqlSelect(Query query) {
        return "select " +
                query.from().et().name() + ".id, " +
                query.from().et().attrs()
                        .map(attr -> query.from().et().name() + "." + attr.name())
                        .collect(joining(", ")) +
                (query.joins().isEmpty() ? "" : ", ") +
                query.joins()
                        .map(j -> j.from().et().name() + ".id, ")
                        .collect(joining(", ")) +
                query.joins()
                        .flatMap(j -> j.from().et().attrs().map(
                                attr -> j.from().et().name() + "." + attr.name()))
                        .collect(joining(", "));
    }

    private String toSqlFrom(From from) {
        return "from " + from.et().name() +
                " as " +
                from.et().name().toLowerCase();
    }

    private String toSqlJoins(List<Join> joins) {
        return joins
                .map(j -> "join " + j.from().et().name() + " " +
                        "on " + j.on()
                        .map(this::toSql)
                        .collect(joining(" and ")))
                .collect(joining("\n"));
    }

    private String toSqlWhere(List<Predicate> w) {
        return w.isEmpty()
                ? ""
                : ("where " + w
                .map(this::toSql)
                .collect(joining(" and ")));
    }

    private String toSqlOrderBy(List<OrderBy> orderBy) {
        return orderBy.isEmpty()
                ? ""
                : "order by " + orderBy.map(this::toSql)
                .collect(joining(", "));
    }

    private String toSql(OrderBy orderBy) {
        return toSql(orderBy.t()) + " " + orderBy.mode().name().toLowerCase();
    }

    private Entity readEntity(EntityType et, ResultSet rs)
    throws SQLException {
        String id = rs.getString(et.name() + ".id");

        java.util.List<AttrValue> avs = new ArrayList<>();

        for (Attr attr : et.attrs()) {
            switch (attr.type()) {
                case Str -> avs.add(new StrAttrValue(
                        attr,
                        rs.getString(attr.name())));
                case Int -> avs.add(new IntAttrValue(
                        attr,
                        rs.getInt(attr.name())));
            }
        }

        return new Entity(et, id, List.ofAll(avs));
    }

    public void init(EntityType... types)
    throws SQLException {
        for (EntityType type : types) {
            try (
                    Connection c = createConnection();
                    Statement s = c.createStatement()
            ) {
                s.execute("drop table if exists " + type.name());
                String columnsDdl =
                        List.of("id varchar primary key")
                                .appendAll(type.attrs().map(this::colDdl))
                                .collect(joining(", "));

                String ddl = "create table " + type.name() +
                        " (" + columnsDdl + ")";
                s.execute(ddl);
            }
        }
    }

    private String colDdl(Attr attr) {
        return attr.name() + switch (attr.type()) {
            case Str -> " varchar";
            case Int -> " int";
        };
    }

    private String toSql(Predicate p) {
        return switch (p) {
            case BinOp binOp -> {
                if (binOp.right().equals(new Null()))
                    yield toSql(binOp.left()) +
                            " is " +
                            toSql(binOp.right());
                else
                    yield toSql(binOp.left()) +
                            " " + toSql(binOp.op()) + " " +
                            toSql(binOp.right());
            }
            case And and -> "( " + toSql(and.left()) + " ) and ( " + toSql(and.right()) + " )";
            case Or or -> "( " + toSql(or.left()) + " ) or ( " + toSql(or.right()) + " )";
            case Not not -> "not ( " + toSql(not.predicate()) + " )";
        };
    }

    private String toSql(Term t) {
        return switch (t) {
            case AttrTerm at -> at.attr().name();
            case Value v -> "'" + v.value().toString() + "'";
            case Null ignored -> "null";
        };
    }

    private String toSql(Op op) {
        return switch (op) {
            case Eq ignored -> "=";
            case Lt ignored -> "<";
            case Gt ignored -> ">";
            case Like ignored -> "like";
        };
    }

    public void addEntities(Entity... es)
    throws SQLException {
        try (Connection c = createConnection()) {
            for (Entity e : es) {
                String dml = List.of("id")
                        .appendAll(e.attrs().map(av -> av.attr().name()))
                        .collect(joining(", "));

                int idx = 0;
                try (
                        PreparedStatement insert = c.prepareStatement(
                                "insert into " + e.type().name() +
                                        " (" + dml + ") values (" +
                                        "?" + ", ?".repeat(e.attrs().size()) + ")")
                ) {
                    insert.setString(++idx, e.id());
                    for (AttrValue a : e.attrs()) {
                        switch (a) {
                            case StrAttrValue s -> insert.setString(
                                    ++idx,
                                    s.value());
                            case IntAttrValue i -> insert.setInt(
                                    ++idx,
                                    i.value());
                        }
                    }
                    insert.execute();
                }
            }
        }
    }

    private static Connection createConnection()
    throws SQLException {
        return DriverManager.getConnection(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
    }
}
