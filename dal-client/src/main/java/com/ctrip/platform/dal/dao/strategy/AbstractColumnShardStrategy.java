package com.ctrip.platform.dal.dao.strategy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ctrip.platform.dal.common.enums.ParameterDirection;
import com.ctrip.platform.dal.dao.DalHintEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameter;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.configure.DalConfigure;

public abstract class AbstractColumnShardStrategy extends AbstractRWSeparationStrategy implements DalShardingStrategy {
    /**
     * Key used to declared columns for locating DB shard.
     */
    public static final String COLUMNS = "columns";
    private static final String COLUMNS_CSHARP = "column";

    /**
     * Key used to declared tables that qualified for table shard. That's not every table is sharded
     */
    public static final String SHARDED_TABLES = "shardedTables";
    
    /**
     * Key used to declared columns for locating table shard.
     */
    public static final String TABLE_COLUMNS = "tableColumns";
    private static final String TABLE_COLUMNS_CSHARP = "tableColumn";
    
    public static final String TABLE_MOD = "tableMod";

    public static final String SEPARATOR = "separator";

    private String[] columns;

    private Set<String> shardedTables = new HashSet<String>();
    private String[] tableColumns;
    private String separator;
    
    /**
     * columns are separated by ','
     * @Override
     */
    public void initialize(Map<String, String> settings) {
        if(settings.containsKey(COLUMNS)) {
            columns = settings.get(COLUMNS).split(",");
        }else {
            if(settings.containsKey(COLUMNS_CSHARP)) {
                columns = settings.get(COLUMNS_CSHARP).split(",");
            }
        }
        
        if(settings.containsKey(SHARDED_TABLES)) {
            String[] tables = settings.get(SHARDED_TABLES).split(",");
            for(String table: tables)
                shardedTables.add(table);
        }
        
        if(settings.containsKey(TABLE_COLUMNS)) {
            tableColumns = settings.get(TABLE_COLUMNS).split(",");
        }else {
            if(settings.containsKey(TABLE_COLUMNS_CSHARP)) {
                tableColumns = settings.get(TABLE_COLUMNS_CSHARP).split(",");
            }
        }
        
        if(settings.containsKey(SEPARATOR)) {
            separator = settings.get(SEPARATOR);
        }
    }

    @Override
    public boolean isShardingByDb() {
        return columns != null;
    }
    
    /**
     * Locate DB shard value
     * @param value column or parameter value
     * @return DB shard id
     */
    abstract public String calculateDbShard(Object value);
    
    /**
     * Locate DB shard value
     * @param value column or parameter value
     * @return table shard id
     */
    abstract public String calculateTableShard(Object value);

    public String locateDbShard(DalConfigure configure, String logicDbName,
            DalHints hints) {
        if(!isShardingByDb())
            throw new RuntimeException(String.format("Logic Db %s is not configured to be shard by database", logicDbName));
        
        String shard = hints.getShardId();
        if(shard != null)
            return shard;
        
        // Shard value take the highest priority
        if(hints.is(DalHintEnum.shardValue)) {
            return calculateDbShard(hints.get(DalHintEnum.shardValue));
        }
        
        shard = evaluateDbShard(columns, (Map<String, ?>)hints.get(DalHintEnum.shardColValues));
        if(shard != null)
            return shard;
        
        shard = evaluateDbShard(columns, (StatementParameters)hints.get(DalHintEnum.parameters));
        if(shard != null)
            return shard;
        
        shard = evaluateDbShard(columns, (Map<String, ?>)hints.get(DalHintEnum.fields));
        if(shard != null)
            return shard;
        
        return null;
    }

    @Override
    public boolean isShardingByTable() {
        return tableColumns != null;
    }

    @Override
    public String locateTableShard(DalConfigure configure, String logicDbName, String tabelName,
            DalHints hints) {
        if(!isShardingByTable())
            throw new RuntimeException(String.format("Logic Db %s is not configured to be shard by table", logicDbName));
        
        String shard = hints.getTableShardId();
        if(shard != null)
            return shard;
        
        // Shard value take the highest priority
        if(hints.is(DalHintEnum.tableShardValue)) {
            return calculateDbShard(hints.get(DalHintEnum.tableShardValue));
        }
        
        shard = evaluateTableShard(tableColumns, (Map<String, ?>)hints.get(DalHintEnum.shardColValues));
        if(shard != null)
            return shard;
        
        shard = evaluateTableShard(tableColumns, (StatementParameters)hints.get(DalHintEnum.parameters));
        if(shard != null)
            return shard;
        
        shard = evaluateTableShard(tableColumns, (Map<String, ?>)hints.get(DalHintEnum.fields));
        if(shard != null)
            return shard;
        
        return null;
    }
    
    private String evaluateDbShard(String[] columns, StatementParameters parameters) {
        if(parameters == null)
            return null;

        for(String column: columns) {
            StatementParameter param = parameters.get(column, ParameterDirection.Input);
            if(param != null && param.getValue() != null) {
                Object id = param.getValue();
                if(id != null) {
                    return calculateDbShard(id);
                }
            }
        }

        return null;
    }
    
    private String evaluateTableShard(String[] columns, StatementParameters parameters) {
        if(parameters == null)
            return null;

        for(String column: columns) {
            StatementParameter param = parameters.get(column, ParameterDirection.Input);
            if(param != null && param.getValue() != null) {
                Object id = param.getValue();
                if(id != null) {
                    return calculateTableShard(id);
                }
            }
        }

        return null;
    }
    
    private String evaluateDbShard(String[] columns, Map<String, ?> shardColValues) {
        if(shardColValues != null) {
            for(String column: columns) {
                Object id = shardColValues.get(column);
                if(id == null) {
                    //To check in case insensitive way
                    for(String col: shardColValues.keySet())
                        if(col.equalsIgnoreCase(column))
                            id = shardColValues.get(col);
                }
                if(id != null) {
                    return calculateDbShard(id);
                }
            }
        }
        return null;
    }
    
    private String evaluateTableShard(String[] columns, Map<String, ?> shardColValues) {
        if(shardColValues != null) {
            for(String column: columns) {
                Object id = shardColValues.get(column);
                if(id == null) {
                    //To check in case insensitive way
                    for(String col: shardColValues.keySet())
                        if(col.equalsIgnoreCase(column))
                            id = shardColValues.get(col);
                }
                if(id != null) {
                    return calculateTableShard(id);
                }
            }
        }
        return null;
    }
    
    private Object findValue(String[] columns, Map<String, ?> colValues) {
        if(colValues == null)
            return null;            
        
        Object id = null;
        for(String column: columns) {
            id = colValues.get(column);
            if(id == null) {
                //To check in case insensitive way
                for(String col: colValues.keySet())
                    if(col.equalsIgnoreCase(column))
                        id = colValues.get(col);
            }
            if(id != null) {
                return calculateTableShard(id);
            }
        }
        return null;            
    }
    
    @Override
    public boolean isShardingEnable(String tableName) {
        return shardedTables.contains(tableName);
    }

    @Override
    public String getTableShardSeparator() {
        return separator;
    }
}