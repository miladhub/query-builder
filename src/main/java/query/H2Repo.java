package query;

import io.vavr.control.Try;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class H2Repo implements Repository
{
    @Override
    public Try<List<Entity>> fetch(Query q)
    {
        return Try.of(() -> switch (q) {
            case From f -> queryEntities(f);
            case Join ignored -> throw new UnsupportedOperationException();
        });
    }

    public Try<List<List<Object>>> fetch(SelectQuery sq)
    {
        return fetch(sq.from())
                .map(es -> toObjects(sq, es));
    }

    private List<List<Object>> toObjects(
            SelectQuery sq,
            List<Entity> es
    )
    {
        return es.stream()
                .map(e -> project(e, sq.select()))
                .collect(Collectors.toList());
    }

    private List<Object> project(
            Entity e,
            List<Term> select
    )
    {
        return select.stream().map(t ->
                switch (t) {
                    case AttrTerm at -> e.attrs().stream()
                            .filter(a -> a.attr().equals(at.attr()))
                            .findFirst()
                            .map(this::attrValue)
                            .orElseThrow(IllegalStateException::new);
                    case Value v -> v.value();
                    case Null ignored -> null;
                }).collect(Collectors.toList());
    }

    private Object attrValue(AttrValue av)
    {
        return switch (av) {
            case StrAttrValue sv -> sv.value();
            case IntAttrValue iv -> iv.value();
        };
    }

    private List<Entity> queryEntities(From f)
    throws SQLException
    {
        try (Connection c = createConnection();
             PreparedStatement select = c.prepareStatement(
                     "select * from " + f.from().name())
        ) {
            ResultSet rs = select.executeQuery();
            List<Entity> entities = new ArrayList<>();
            while (rs.next()) {
                List<AttrValue> avs = new ArrayList<>();
                String id = rs.getString("id");
                entities.add(new Entity(f.from(), id, avs));
                for (Attr attr : f.from().attrs()) {
                    switch (attr.type()) {
                        case Str -> avs.add(new StrAttrValue(attr,
                                rs.getString(attr.name())));
                        case Int -> avs.add(new IntAttrValue(attr,
                                rs.getInt(attr.name())));
                    }
                }
            }
            return entities;
        }
    }

    public void init(EntityType... types)
    throws SQLException
    {
        for (EntityType type : types) {
            try (Connection c = createConnection();
                 Statement s = c.createStatement()) {
                s.execute("drop table if exists " + type.name());
                String columnsDdl = Stream.concat(
                                Stream.of("id varchar primary key"),
                                type.attrs().stream()
                                        .map(this::colDdl))
                        .collect(Collectors.joining(", "));
                String ddl = "create table " + type.name() +
                        " (" + columnsDdl + ")";
                s.execute(ddl);
            }
        }
    }

    private String colDdl(Attr attr)
    {
        return attr.name() + switch (attr.type()) {
            case Str -> " varchar";
            case Int -> " int";
        };
    }

    public void addEntities(Entity... es)
    throws SQLException
    {
        try (Connection c = createConnection()) {
            for (Entity e : es) {
                String dml = Stream.concat(Stream.of("id"), e.attrs().stream()
                                .map(av -> av.attr().name()))
                        .collect(Collectors.joining(", "));
                int idx = 0;
                try (PreparedStatement insert = c.prepareStatement(
                        "insert into " + e.type().name() +
                                " (" + dml + ") values (" +
                                "?" + ", ?".repeat(e.attrs().size()) + ")")
                ) {
                    insert.setString(++idx, e.id());
                    for (AttrValue a : e.attrs()) {
                        switch (a) {
                            case StrAttrValue s -> insert.setString(++idx,
                                    s.value());
                            case IntAttrValue i -> insert.setInt(++idx,
                                    i.value());
                        }
                    }
                    insert.execute();
                }
            }
        }
    }

    private static Connection createConnection()
    throws SQLException
    {
        return DriverManager.getConnection(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
    }
}
