package query;

import java.util.List;

public record Entity(
        EntityType type,
        String id,
        List<AttrValue> attrs
) {}
