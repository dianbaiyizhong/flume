package org.apache.flume.sink.hdfs;

import com.google.common.collect.Lists;
import org.apache.orc.TypeDescription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcSchemaManager {

    private static volatile OrcSchemaManager instance;

    private Map<String, List<String>> schemaMap = new HashMap<>();


    public TypeDescription getSchema(String key) {

        TypeDescription schema = TypeDescription.createStruct();

        List<String> list = schemaMap.get(key);
        for (int i = 0; i < list.size(); i++) {
            schema.addField(list.get(i), TypeDescription.createString());
        }
        return schema;
    }

    private OrcSchemaManager() {
        schemaMap.put("test_table", Lists.newArrayList("name", "sex"));
    }

    public static OrcSchemaManager getInstance() {
        if (instance == null) {
            synchronized (OrcSchemaManager.class) {
                if (instance == null) {
                    instance = new OrcSchemaManager();
                }
            }
        }
        return instance;
    }


}
