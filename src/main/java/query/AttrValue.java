package query;

public sealed interface AttrValue {
    Attr attr();
}
record StrAttrValue(Attr attr, String value) implements AttrValue { }
record IntAttrValue(Attr attr, int value) implements AttrValue {}
