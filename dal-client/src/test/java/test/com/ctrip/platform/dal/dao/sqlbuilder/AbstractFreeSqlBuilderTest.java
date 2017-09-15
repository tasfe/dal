package test.com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.column;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.columns;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.expression;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.table;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.text;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.toArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Text;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions.Expression;

public class AbstractFreeSqlBuilderTest {
    private static final String template = "template";
    private static final String wrappedTemplate = "[template]";
    private static final String expression = "count()";
    private static final String elseTemplate = "elseTemplate";
    private static final String EMPTY = "";
    private static final String logicDbName = "dao_test_sqlsvr_tableShard";
    private static final String tableName = "dal_client_test";
    
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
        test.append(new Text(template));
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendClauseCondition() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, new Text(template));
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, new Text(template));
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendClauseConditionWithElse() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.append(true, new Text(template), new Text(elseTemplate));
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.append(false, new Text(template), new Text(elseTemplate));
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
        test.appendColumns(template, template);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "], [" + template + "]", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendColumns(text(template), expression(template));
        test.setLogicDbName(logicDbName);
        assertEquals("template, template", test.build());
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
    public void testSelectFrom() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.selectFrom(columns(template, template, template), table(noShardTable));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT [template], [template], [template] FROM [noShard] WITH (NOLOCK)", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.selectFrom(toArray(text(template), expression(template), column(template).as(template)), table(noShardTable));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT template, template, [template] AS template FROM [noShard] WITH (NOLOCK)", test.build());
    }
    
    @Test
    public void testWhere() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.where(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" WHERE template", test.build());
    }
    
    @Test
    public void testWhereClause() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.where(expression("count() "), text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" WHERE count() template", test.build());
    }
    
    @Test
    public void testGroupBy() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.groupBy(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" GROUP BY " + template, test.build());
    }
    
    @Test
    public void testHaving() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.having(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" HAVING " + template, test.build());
    }
    
    @Test
    public void testLeftBracket() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.leftBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(", test.build());
    }
    
    @Test
    public void testRightBracket() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.rightBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(")", test.build());
    }
    
    @Test
    public void testBracket() {
        //Empty
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.bracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("()", test.build());
        
        //One
        test = new AbstractFreeSqlBuilder();
        test.bracket(text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template)", test.build());
        
        //two
        test = new AbstractFreeSqlBuilder();
        test.bracket(text(template), expression(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template" + expression + ")", test.build());
    }
    
    @Test
    public void testAnd() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.and(text(template), text(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(template + " AND " + expression, test.build());
    }
    
    @Test
    public void testAndMultiple() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.and();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" AND ", test.build());
    }
    
    @Test
    public void testOr() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.or();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" OR ", test.build());
    }
    
    @Test
    public void testOrMultiple() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.or(text(template), text(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(template + " OR " + expression, test.build());
    }
    
    @Test
    public void testNot() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.not();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(" NOT ", test.build());
    }
    
    @Test
    public void testNullable() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        try {
            test.nullable(null);
            fail();
        } catch (Exception e) {
        }
        
        test = new AbstractFreeSqlBuilder();
        Expression exp = new Expression(expression);
        test.append(template).append(exp).nullable(null);
        assertTrue(exp.isNull());
        
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        try {
            assertEquals(template + expression, test.build());
            fail();
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testEqual() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.equal(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " = ?", test.build());
    }
    
    @Test
    public void testNotEqual() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.notEqual(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " <> ?", test.build());
    }
    
    @Test
    public void testGreaterThan() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.greaterThan(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " > ?", test.build());
    }
    
    @Test
    public void testGreaterThanEquals() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.greaterThanEquals(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " >= ?", test.build());
    }
    
    @Test
    public void testLessThan() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.lessThan(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " < ?", test.build());
    }
    
    @Test
    public void testLessThanEquals() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.lessThanEquals(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " <= ?", test.build());
    }
    
    @Test
    public void testBetween() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.between(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " BETWEEN ? AND ?", test.build());
    }
    
    @Test
    public void testLike() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.like(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " LIKE ?", test.build());
    }
    
    @Test
    public void testNotLike() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.notLike(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " NOT LIKE ?", test.build());
    }
    
    @Test
    public void testIn() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.in(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IN(?)", test.build());
    }
    
    @Test
    public void testNotIn() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.notIn(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " NOT IN(?)", test.build());
    }
    
    @Test
    public void testIsNull() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.isNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NULL ?", test.build());
    }
    
    @Test
    public void testIsNotNull() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.isNotNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NOT NULL ?", test.build());
    }
    
        
}
