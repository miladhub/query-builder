package query;

public sealed interface AttrValue {
    Attr attr();
    Object value();
}
record StrAttrValue(Attr attr, String value) implements AttrValue { }
record IntAttrValue(Attr attr, Integer value) implements AttrValue {}
