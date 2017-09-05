package test.com.ctrip.platform.dal.dao.sqlbuilder;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.TextClause;

public class AbstractFreeSqlBuilderTest {
    private static final String template = "template";
    private static final String elseTemplate = "elseTemplate";
    private static final String EMPTY = "";
    private static final String logicDbName = "dao_test_sqlsvr_tableShard";
    private static final String separator = ", ";
    private static final String tableName = "dal_client_test";
    private static final String templateWIthHolder = "AAA %s BBB";
    
    @Test
    public void testSetLogicDbName() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        try {
            test.setLogicDbName(null);
            fail();
        } catch (Exception e) {
        }
        
        try {
            test.setLogicDbName("Not exist");
            fail();
        } catch (IllegalArgumentException e) {
        } catch(Throwable ex) {
            fail();
        }
        
        test.setLogicDbName(logicDbName);
    }
    
    @Test
    public void testSetHints() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        try {
            test.setHints(null);
            fail();
        } catch (Exception e) {
        }
        
        test.setHints(new DalHints());
    }
    
    @Test
    public void testWith() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        try {
            test.with(null);
            fail();
        } catch (Exception e) {
        }
        
        StatementParameters p = new StatementParameters();
        test.with(p);
        // Same is allowed
        test.with(p);

        //Empty is allowed
        p = new StatementParameters();
        test.with(p);
        p.set("", 1, "");
        
        p = new StatementParameters();
        try {
            test.with(p);
            fail();
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testAppend() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(template);
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendCondition() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, template);
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, template);
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendConditionWithElse() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, template, elseTemplate);
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, template, elseTemplate);
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendClause() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(new TextClause(template));
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendClauseCondition() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, new TextClause(template));
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, new TextClause(template));
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendClauseConditionWithElse() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, new TextClause(template), new TextClause(elseTemplate));
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, new TextClause(template), new TextClause(elseTemplate));
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendColumn() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendColumn(template);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "]", test.build());
    }
    
    @Test
    public void testAppendColumns() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendColumns(new String[]{template, template}, separator);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "], [" + template + "]", test.build());
    }
    
    @Test
    public void testAppendColumnsWithTpl() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendWithColumns(templateWIthHolder, new String[]{template, template}, separator);
        test.setLogicDbName(logicDbName);
        assertEquals(String.format(templateWIthHolder, "[" + template + "], [" + template + "]"), test.build());
    }
    
    @Test
    public void testAppendTable() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendTable(noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("[" + noShardTable + "]", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendTable(tableName);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals("[" + tableName + "_1]", test.build());
    }
    
    @Test
    public void testAppendWithTable() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendWithTable(templateWIthHolder, noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(String.format(templateWIthHolder, "[" + noShardTable + "]"), test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendWithTable(templateWIthHolder, tableName);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals(String.format(templateWIthHolder, "[" + tableName + "_1]"), test.build());
    }
    
    @Test
    public void testAppendWithTableClause() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendWithTable(templateWIthHolder, AbstractFreeSqlBuilder.table(noShardTable));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(String.format(templateWIthHolder, "[" + noShardTable + "]"), test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendWithTable(templateWIthHolder, AbstractFreeSqlBuilder.table(tableName));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals(String.format(templateWIthHolder, "[" + tableName + "_1]"), test.build());
    }
    
}
