package query;

import io.vavr.collection.List;

public record Entity(
        EntityType type,
        String id,
        List<AttrValue> attrs
) {}
