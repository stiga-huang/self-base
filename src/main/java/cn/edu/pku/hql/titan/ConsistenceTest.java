package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import java.util.Set;

/**
 * Created by huangql on 3/28/16.
 */
public class ConsistenceTest {

    public static void main(String[] args) {
        final TitanGraph g = TitanFactory.open("inmemory");

        TitanManagement management = g.getManagementSystem();
        PropertyKey prop = management.makePropertyKey("name").dataType(String.class).make();
        TitanGraphIndex namei = management.buildIndex("nameIndex", Vertex.class).addKey(prop)
                .unique().buildCompositeIndex();
        //management.setConsistency(namei, ConsistencyModifier.LOCK);
        management.commit();

        try {
            TitanVertex v1 = g.addVertex();
            TitanVertex v2 = g.addVertex();
            v1.setProperty("name", "Bob");
            v2.setProperty("name", "Bob");
        } catch (TitanException e) {
            e.printStackTrace();
        } finally {
            g.commit();
        }

        int vertexCount = 0;
        for (Vertex v : g.getVertices()) {
            System.out.println("vid = " + v.getId());

            Set<String> keys = v.getPropertyKeys();
            for (String k : keys) {
                System.out.println(k + "->" + v.getProperty(k));
            }
            vertexCount++;
        }
        System.out.println("vertexCount = " + vertexCount);
        g.shutdown();
    }
}
