package models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Payload implements Serializable {
    private Integer value;
    private List<Long> vertices;

    public Payload(Integer value) {
        this.value = value;
        this.vertices = new ArrayList<>();
    }

    public Payload(Integer value, List<Long> vertices) {
        this.value = value;
        this.vertices = vertices;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public List<Long> getVertices() {
        return vertices;
    }

    public void setVertices(List<Long> vertices) {
        this.vertices = vertices;
    }

    public List<Long> addVertex(Long vertex) {
        this.vertices.add(vertex);
        return this.vertices;
    }
}
