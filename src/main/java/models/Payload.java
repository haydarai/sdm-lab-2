package models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Payload implements Serializable {
    private Integer value;
    private List<Object> vertices;

    public Payload(Integer value) {
        this.value = value;
        this.vertices = new ArrayList<>();
    }

    public Payload(Integer value, List<Object> vertices) {
        this.value = value;
        this.vertices = vertices;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public List<Object> getVertices() {
        return vertices;
    }

    public void setVertices(List<Object> vertices) {
        this.vertices = vertices;
    }

    public List<Object> addVertex(Object vertex) {
        this.vertices.add(vertex);
        return this.vertices;
    }
}
