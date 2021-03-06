package query;

import io.vavr.collection.List;
import io.vavr.control.Try;

import java.sql.*;
import java.util.ArrayList;

import static java.util.stream.Collectors.joining;

public class H2Repo
        implements Repository {
    @Override
    public void init(EntityType... types) {
        Try.run(() -> init(List.of(types)));
    }

    @Override
    public void addEntities(Entity... es) {
        Try.run(() -> addEntities(List.of(es)));
    }

    @Override
    public Try<List<List<Object>>> select(Query q) {
        return Try.of(() -> query(q));
    }

    private void init(List<EntityType> types)
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
        return toSql(attr) + switch (attr.type()) {
            case Str -> " varchar";
            case Int -> " int";
        };
    }

    private void addEntities(List<Entity> es)
    throws SQLException {
        try (Connection c = createConnection()) {
            for (Entity e : es) {
                String dml = List.of("id")
                        .appendAll(e.attrs()
                                .map(AttrValue::attr)
                                .map(this::toSql))
                        .collect(joining(", "));

                int idx = 0;
                try (
                        PreparedStatement insert = c.prepareStatement(
                                "insert into " + e.type().name() +
                                        " (" + dml + ") values (" +
                                        "?" + ", ?" .repeat(e.attrs().size()) + ")")
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

    private List<List<Object>> query(Query q) throws SQLException {
        String sql = toSqlQuery(q);

        try (
                Connection c = createConnection();
                PreparedStatement select = c.prepareStatement(sql)
        ) {
            ResultSet rs = select.executeQuery();
            java.util.List<java.util.List<Object>> rows = new ArrayList<>();

            while (rs.next()) {
                java.util.List<Object> row = new ArrayList<>();

                int i = 1;
                for (SelectTerm term : q.select()) {
                    Object value = switch (term) {
                        case AttrSelectTerm at -> readAttr(i, at.attr(), rs);
                        case Aggregation aggr -> readAttr(i, aggr.t().attr(), rs);
                    };
                    i++;

                    row.add(value);
                }

                rows.add(row);
            }
            return List.ofAll(rows).map(List::ofAll);
        }
    }

    private String toSqlQuery(Query q) {
        String select = toSqlSelect(q.select());
        String from = toSqlFrom(q.from());
        String joins = toSqlJoins(q.joins());
        String where = toSqlWhere(q.where());
        String groupBy = toSqlGroupBy(q.groupBy());
        String orderBy = toSqlOrderBy(q.orderBy());

        return select + "\n" +
                from + "\n" +
                joins + "\n" +
                where + "\n" +
                groupBy + "\n" +
                orderBy;
    }

    private String toSqlSelect(List<SelectTerm> terms) {
        return "select " + terms
                .map(this::toSql)
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

    private String toSqlGroupBy(List<Attr> groupBy) {
        return groupBy.isEmpty()
                ? ""
                : "group by " + groupBy.map(this::toSql)
                .collect(joining(", "));
    }

    private String toSql(OrderBy orderBy) {
        return toSql(orderBy.t()) + " " + orderBy.mode().name().toLowerCase();
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

    private String toSql(ClauseTerm term) {
        return switch (term) {
            case AttrClauseTerm at -> at.attr().name();
            case Value v -> "'" + v.value().toString() + "'";
            case Null ignored -> "null";
        };
    }

    private String toSql(SelectTerm term) {
        return switch (term) {
            case AttrSelectTerm at -> toSql(at.attr());
            case Aggregation aggregation -> switch (aggregation.at()) {
                case MAX -> "max( " + toSql(aggregation.t()) + ")";
                case MIN -> "min( " + toSql(aggregation.t()) + ")";
                case SUM -> "sum( " + toSql(aggregation.t()) + ")";
                case AVG -> "avg( " + toSql(aggregation.t()) + ")";
                case COUNT -> "count( " + toSql(aggregation.t()) + ")";
            };
        };
    }

    private String toSql(Attr attr) {
        return attr.name();
    }

    private String toSql(Op op) {
        return switch (op) {
            case EQ -> "=";
            case LT -> "<";
            case GT -> ">";
            case LIKE -> "like";
        };
    }

    private Object readAttr(
            int i, Attr attr, ResultSet rs
    ) throws SQLException {
        return switch (attr.type()) {
            case Str -> rs.getString(i);
            case Int -> rs.getInt(i);
        };
    }

    private static Connection createConnection()
    throws SQLException {
        return DriverManager.getConnection(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
    }
}
