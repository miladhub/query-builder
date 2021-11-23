package query;

import io.vavr.collection.List;

public record EntityType(String name, List<Attr> attrs) {}
