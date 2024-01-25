/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.core;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.FakeObjects;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

public class EdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();
        schema.propertyKey("message").asText().create();
        schema.propertyKey("place").asText().create();
        schema.propertyKey("tool").asText().create();
        schema.propertyKey("reason").asText().create();
        schema.propertyKey("hurt").asBoolean().create();
        schema.propertyKey("arrested").asBoolean().create();
        schema.propertyKey("date").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();

        LOG.debug("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
              .properties("id", "amount", "timestamp", "message")
              .nullableKeys("message")
              .multiTimes().sortKeys("id")
              .link("person", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("authored").singleTime()
              .properties("contribution", "comment", "score")
              .nullableKeys("score", "contribution", "comment")
              .link("author", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("write").properties("time")
              .multiTimes().sortKeys("time")
              .link("author", "book")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("look").properties("time", "score")
              .nullableKeys("score")
              .multiTimes().sortKeys("time")
              .link("person", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("know").singleTime()
              .link("author", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("followedBy").singleTime()
              .link("author", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("friend").singleTime()
              .link("person", "person")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("follow").singleTime()
              .link("person", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("created").singleTime()
              .link("author", "language")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("strike").link("person", "person")
              .properties("id", "timestamp", "place", "tool", "reason",
                          "hurt", "arrested")
              .multiTimes().sortKeys("id")
              .nullableKeys("tool", "reason", "hurt")
              .enableLabelIndex(false)
              .ifNotExist().create();
        schema.edgeLabel("read").link("person", "book")
              .properties("place", "date")
              .ttl(3000L)
              .enableLabelIndex(true)
              .ifNotExist()
              .create();
        schema.edgeLabel("borrow").link("person", "book")
              .properties("place", "date")
              .ttl(3000L)
              .ttlStartTime("date")
              .enableLabelIndex(true)
              .ifNotExist()
              .create();
    }

    protected void initStrikeIndex() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  strike index  ================");

        schema.indexLabel("strikeByTimestamp").onE("strike").range()
              .by("timestamp").create();
        schema.indexLabel("strikeByTool").onE("strike").secondary()
              .by("tool").create();
        schema.indexLabel("strikeByPlaceToolReason").onE("strike").secondary()
              .by("place", "tool", "reason").create();
    }

    @Test
    public void testQueryEdgesAdjacentVerticesWithLimitAndFilterProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 62,
                                       "lived", "California");
        Vertex marko = graph.addVertex(T.label, "author", "id", 3,
                                       "name", "Marko", "age", 61,
                                       "lived", "California");
        guido.addEdge("know", james);
        guido.addEdge("know", marko);
        marko.addEdge("know", james);

        for (int i = 0; i < 20; i++) {
            Vertex java = graph.addVertex(T.label, "book", "name", "java-" + i);
            james.addEdge("authored", java, "score", i % 2);
            james.addEdge("write", java, "time", "2020-6-" + i);

            marko.addEdge("authored", java, "score", i % 2);
        }

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 62);
        Vertex java0 = graph.addVertex(T.label, "book", "name", "java-0");
        louise.addEdge("look", java0, "time", "2020-6-18", "score", 1);
        louise.addEdge("look", java0, "time", "2020-6-0", "score", 1);

        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 62);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 61);

        louise.addEdge("friend", jeff);
        louise.addEdge("friend", sean);
        jeff.addEdge("friend", sean);
        jeff.addEdge("follow", marko);
        sean.addEdge("follow", marko);

        graph.tx().commit();

        // out
        List<Vertex> vertices = graph.traversal().V()
                                     .out().has("age", 62)
                                     .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals(2, new HashSet<>(vertices).size());
        Assert.assertTrue(vertices.contains(james));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V()
                        .out().has("age", 62)
                        .limit(4).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .limit(6).toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .toList();
        Assert.assertEquals(11, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .limit(12).toList();
        Assert.assertEquals(11, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .limit(2).toList();
        Assert.assertEquals(2, vertices.size());

        boolean firstIsLook = graph.traversal().V()
                                   .outE("write", "look")
                                   .limit(1).label().is("look").hasNext();
        if (firstIsLook) {
            // query edges of louise if before james
            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(1).toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals("java-1", vertices.get(0).value("name"));

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            Assert.assertEquals(9, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(9, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(13)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(10, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(13)
                            .has("name", Text.contains("java-1"))
                            .limit(13).toList();
            Assert.assertEquals(10, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(14)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(3)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(3, vertices.size());
        } else {
            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            Assert.assertEquals(11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(11, vertices.size());

            boolean firstLouise = graph.traversal().V(louise, james)
                                       .outE("write", "look").outV().next()
                                       .equals(louise);
            vertices = graph.traversal().V(louise, james)
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            // two look edges not matched
            Assert.assertEquals(firstLouise ? 9 : 11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(3)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(1, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(20)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(1, vertices.size()); // skip java1~java19

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(21)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(2, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(22)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(3, vertices.size());
        }

        // in
        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(2, new HashSet<>(vertices).size());
        Assert.assertTrue(vertices.contains(james));
        Assert.assertTrue(vertices.contains(louise));

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(5).toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .dedup().limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .dedup().limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(guido));
        Assert.assertTrue(vertices.contains(louise));

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        // both
        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(louise));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(guido));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .limit(4).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(sean));
        Assert.assertTrue(vertices.contains(marko));

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabels() {
        HugeGraph graph = graph();
        init18Edges();

        long size;
        size = graph.traversal().V().outE("created", "authored", "look")
                    .hasLabel("authored", "created")
                    .count().next();
        Assert.assertEquals(5L, size);

        size = graph.traversal().V().outE("created", "authored", "look")
                    .hasLabel("authored", "friend")
                    .count().next();
        Assert.assertEquals(3L, size);

        size = graph.traversal().V().outE("created")
                    .hasLabel("authored", "created")
                    .count().next();
        Assert.assertEquals(2L, size);

        size = graph.traversal().V().inE("created", "authored", "look")
                    .has("score", 3)
                    .count().next();
        Assert.assertEquals(3L, size);
    }

    private void init18Edges() {
        this.init18Edges(true);
    }

    private void init18Edges(boolean commit) {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                                        "name", "Guido van Rossum", "age", 61,
                                        "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        Vertex selina = graph.addVertex(T.label, "person", "name", "Selina",
                                        "city", "Beijing", "age", 24);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        guido.addEdge("know", james);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 3);

        louise.addEdge("look", java1, "time", "2017-5-1");
        louise.addEdge("look", java1, "time", "2017-5-27");
        louise.addEdge("look", java2, "time", "2017-5-27");
        louise.addEdge("look", java3, "time", "2017-5-1", "score", 3);
        jeff.addEdge("look", java3, "time", "2017-5-27", "score", 3);
        sean.addEdge("look", java3, "time", "2017-5-27", "score", 4);
        selina.addEdge("look", java3, "time", "2017-5-27", "score", 0);

        louise.addEdge("friend", jeff);
        louise.addEdge("friend", sean);
        louise.addEdge("friend", selina);
        jeff.addEdge("friend", sean);
        jeff.addEdge("follow", james);

        if (commit) {
            graph.tx().commit();
        }
    }

    private void init100LookEdges() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);

        Vertex java = graph.addVertex(T.label, "book", "name", "java-book");

        for (int i = 0; i < 50; i++) {
            louise.addEdge("look", java, "time", "time-" + i);
        }

        for (int i = 0; i < 50; i++) {
            jeff.addEdge("look", java, "time", "time-" + i);
        }

        graph.tx().commit();
    }

    private Edge initEdgeTransfer() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");

        graph.tx().commit();
        return edge;
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertices = graph().traversal().V()
                                       .hasLabel(label).has(pkName, pkValue)
                                       .toList();
        Assert.assertTrue(vertices.size() <= 1);
        return vertices.size() == 1 ? vertices.get(0) : null;
    }

    private static void assertContains(
        List<Edge> edges,
        String label,
        Vertex outVertex,
        Vertex inVertex,
        Object... kvs) {
        Assert.assertTrue(Utils.contains(edges,
                                         new FakeObjects.FakeEdge(label, outVertex, inVertex, kvs)));
    }

    private int traverseInPage(Function<String, GraphTraversal<?, ?>> fetcher) {
        String page = PageInfo.PAGE_NONE;
        int count = 0;
        while (page != null) {
            GraphTraversal<?, ?> iterator = fetcher.apply(page);
            Long size = IteratorUtils.count(iterator);
            Assert.assertLte(1L, size);
            page = TraversalUtil.page(iterator);
            count += size.intValue();
        }
        return count;
    }

    private int superNodeSize() {
        return params().configuration().get(CoreOptions.EDGE_TX_CAPACITY);
    }
}
