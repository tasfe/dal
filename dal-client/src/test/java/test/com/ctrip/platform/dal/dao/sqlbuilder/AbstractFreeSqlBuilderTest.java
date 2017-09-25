package test.com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.column;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.expression;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.table;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.text;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Clause;
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
        AbstractFreeSqlBuilder test = createDisabled();
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
    
    /**
     * Create test with auto meltdown disabled
     * @return
     */
    private AbstractFreeSqlBuilder createDisabled() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.disableAutoMeltdown();
        return test;
    }
    
    
    @Test
    public void testSetHints() {
        AbstractFreeSqlBuilder test = createDisabled();
        try {
            test.setHints(null);
            fail();
        } catch (Exception e) {
        }
        
        test.setHints(new DalHints());
    }
    
    @Test
    public void testWith() {
        AbstractFreeSqlBuilder test = createDisabled();
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
        AbstractFreeSqlBuilder test = createDisabled();
        test.append(template);
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendCondition() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendWhen(true, template);
        assertEquals(template, test.build());
        
        test = createDisabled();
        test.appendWhen(false, template);
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendConditionWithElse() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendWhen(true, template, elseTemplate);
        assertEquals(template, test.build());
        
        test = createDisabled();
        test.appendWhen(false, template, elseTemplate);
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendClause() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.append(new Text(template));
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendClauseCondition() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendWhen(true, new Text(template));
        assertEquals(template, test.build());
        
        test = createDisabled();
        test.appendWhen(false, new Text(template));
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendClauseConditionWithElse() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendWhen(true, new Text(template), new Text(elseTemplate));
        assertEquals(template, test.build());
        
        test = createDisabled();
        test.appendWhen(false, new Text(template), new Text(elseTemplate));
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendColumn() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendColumn(template);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "]", test.build());
    }
    
    @Test
    public void testAppendTable() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendTable(noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("[" + noShardTable + "]", test.build());
        
        test = createDisabled();
        test.appendTable(tableName);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals("[" + tableName + "_1]", test.build());
    }
    @Test
    public void testSelect() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = createDisabled();
        test.select(template, template, template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT [template], [template], [template]", test.build());
        
        test = createDisabled();
        test.select(template, text(template), expression(template), column(template).as(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT [template], template, template, [template] AS template", test.build());
    }
    
    @Test
    public void testFrom() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = createDisabled();
        test.from(noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("FROM [noShard] WITH (NOLOCK)", test.build());
        
        test = createDisabled();
        test.from(table(noShardTable));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("FROM [noShard] WITH (NOLOCK)", test.build());
    }
    
    @Test
    public void testWhere() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.where(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("WHERE template", test.build());
    }
    
    @Test
    public void testWhereClause() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.where(expression("count() "), text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("WHERE count() template", test.build());
    }
    
    @Test
    public void testOrderBy() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.orderBy(template, true);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("ORDER BY " + wrappedTemplate + " ASC", test.build());
    }
    
    @Test
    public void testGroupBy() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.groupBy(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("GROUP BY " + wrappedTemplate, test.build());
        
        test = createDisabled();
        test.groupBy(expression(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("GROUP BY " + template, test.build());
    }
    
    @Test
    public void testHaving() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.having(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("HAVING " + template, test.build());
    }
    
    @Test
    public void testLeftBracket() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.leftBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(", test.build());
    }
    
    @Test
    public void testRightBracket() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.rightBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(")", test.build());
    }
    
    @Test
    public void testBracket() {
        //Empty
        AbstractFreeSqlBuilder test = createDisabled();
        test.bracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("()", test.build());
        
        //One
        test = createDisabled();
        test.bracket(text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template)", test.build());
        
        //two
        test = createDisabled();
        test.bracket(text(template), expression(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template" + expression + ")", test.build());
    }
    
    @Test
    public void testAnd() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.and(text(template), text(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(template + " AND " + expression, test.build());
    }
    
    @Test
    public void testAndMultiple() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.and(template, template, template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("template AND template AND template", test.build());
    }
    
    @Test
    public void testOr() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.or();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("OR", test.build());
    }
    
    @Test
    public void testOrMultiple() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.or(text(template), text(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(template + " OR " + expression, test.build());
    }
    
    @Test
    public void testNot() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.not();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("NOT", test.build());
    }
    
    @Test
    public void testNullable() {
        AbstractFreeSqlBuilder test = createDisabled();
        try {
            test.nullable(null);
            fail();
        } catch (Exception e) {
        }
        
        test = createDisabled();
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
        AbstractFreeSqlBuilder test = createDisabled();
        test.equal(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " = ?", test.build());
    }
    
    @Test
    public void testNotEqual() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.notEqual(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " <> ?", test.build());
    }
    
    @Test
    public void testGreaterThan() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.greaterThan(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " > ?", test.build());
    }
    
    @Test
    public void testGreaterThanEquals() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.greaterThanEquals(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " >= ?", test.build());
    }
    
    @Test
    public void testLessThan() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.lessThan(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " < ?", test.build());
    }
    
    @Test
    public void testLessThanEquals() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.lessThanEquals(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " <= ?", test.build());
    }
    
    @Test
    public void testBetween() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.between(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " BETWEEN ? AND ?", test.build());
    }
    
    @Test
    public void testLike() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.like(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " LIKE ?", test.build());
    }
    
    @Test
    public void testNotLike() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.notLike(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " NOT LIKE ?", test.build());
    }
    
    @Test
    public void testIn() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.in(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IN(?)", test.build());
    }
    
    @Test
    public void testNotIn() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.notIn(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " NOT IN(?)", test.build());
    }
    
    @Test
    public void testIsNull() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.isNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NULL ?", test.build());
    }
    
    @Test
    public void testIsNotNull() {
        AbstractFreeSqlBuilder test = createDisabled();
        test.isNotNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NOT NULL ?", test.build());
    }
    
    @Test
    public void testExpression() throws SQLException {
        Clause test = expression(template);
        
        AbstractFreeSqlBuilder builder = createDisabled();
        builder.append(test);
        builder.setLogicDbName(logicDbName);

        assertEquals(template, test.build());
    }
    
    @Test
    public void testDisableAutoMeltdown() throws SQLException {
        AbstractFreeSqlBuilder test = createDisabled();
        test.appendExpressions(AND).bracket(AND, OR, AND);
        test.disableAutoMeltdown();
        assertEquals("AND ( AND  OR  AND )", test.build());
    }
    
    @Test
    public void testAutoMeltdown() throws SQLException {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendExpressions(AND).bracket(AND, OR, AND);
        assertEquals("", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendExpressions(template, AND).bracket(AND, OR, AND);
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendColumn(template);
        assertEquals(template + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(template + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template).nullable(null).append(AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template).append(AND).append(expression(template)).nullable(null);
        assertEquals(template+wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND, template).appendTable(template).append(AND).append(expression(template)).nullable(null);
        assertEquals("template AND (template)[template]", test.build());
    }
}
