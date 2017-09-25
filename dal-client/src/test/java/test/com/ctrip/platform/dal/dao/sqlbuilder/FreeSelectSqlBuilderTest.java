package test.com.ctrip.platform.dal.dao.sqlbuilder;

import java.sql.SQLException;

import org.junit.Test;

import static org.junit.Assert.*;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import static com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder.*;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.*;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeUpdateSqlBuilder;

public class FreeSelectSqlBuilderTest {
    private static final String template = "template";
    private static final String wrappedTemplate = "[template]";
    private static final String expression = "count()";
    private static final String elseTemplate = "elseTemplate";
    private static final String EMPTY = "";
    private static final String logicDbName = "dao_test_sqlsvr_tableShard";
    
    private static final String tableName = "dal_client_test";
    private static final String wrappedTableName = "[dal_client_test]";
    
    private static final String noShardTableName = "noShard";
    private static final String wrappedNoShardTableName = "[noShard]";

    private FreeSelectSqlBuilder createTest() {
        return (FreeSelectSqlBuilder)new FreeSelectSqlBuilder(logicDbName).setHints(new DalHints());
    }
    
    @Test
    public void testCreate() throws SQLException {
        try {
            FreeSelectSqlBuilder test = new FreeSelectSqlBuilder(DatabaseCategory.MySql);
            test = new FreeSelectSqlBuilder(DatabaseCategory.SqlServer);
        } catch (Exception e) {
            fail();
        }
        try {
            FreeSelectSqlBuilder test = new FreeSelectSqlBuilder(DatabaseCategory.MySql);
            test.setLogicDbName(logicDbName);
            test.setDbCategory(DatabaseCategory.MySql);
            fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void testSetTemplate() throws SQLException {
        FreeSelectSqlBuilder test = createTest();
        test.setTemplate(template).setTemplate(template);
        assertEquals(template+template, test.build());
    }
    
    @Test
    public void testBuildSqlServerTop() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = createTest();
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.top(10);        
        assertEquals("templatetemplatetemplate OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", test.build());
    }
    
    @Test
    public void testBuildSqlServerAtPage() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = createTest();
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.atPage(10, 10);
        assertEquals("templatetemplatetemplate OFFSET 90 ROWS FETCH NEXT 10 ROWS ONLY", test.build());
    }
    
    @Test
    public void testBuildSqlServerRange() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = createTest();
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.range(10, 10);
        assertEquals("templatetemplatetemplate OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", test.build());

    }

    @Test
    public void testBuildMySqlTop() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = new FreeSelectSqlBuilder(DatabaseCategory.MySql);
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.top(10);        
        assertEquals("templatetemplatetemplate limit 0, 10", test.build());
    }
    
    @Test
    public void testBuildMySqlAtPage() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = new FreeSelectSqlBuilder(DatabaseCategory.MySql);
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.atPage(10, 10);
        assertEquals("templatetemplatetemplate limit 90, 10", test.build());
    }
    
    @Test
    public void testBuildMySqlRange() throws SQLException {
        String builtTpl = template+template+template;
        FreeSelectSqlBuilder test = new FreeSelectSqlBuilder(DatabaseCategory.MySql);
        test.setTemplate(template).setTemplate(template).append(template);
        assertEquals(builtTpl, test.build());

        test.range(10, 10);
        assertEquals("templatetemplatetemplate limit 10, 10", test.build());

    }

    @Test
    public void testBuildSelect() throws SQLException {
        FreeSelectSqlBuilder test = createTest();
        test.select(template, template, template).from(tableName).where(template).groupBy(template);
        test.top(10).setHints(new DalHints().inTableShard(1));
        assertEquals("SELECT [template], [template], [template] FROM [dal_client_test_1] WITH (NOLOCK) WHERE template GROUP BY [template] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", test.build());
    }

    @Test
    public void testBuildMeltdownAtEnd() throws SQLException {
        FreeSelectSqlBuilder test = createTest();
        test.where(template, AND, expression(template).nullable(null)).groupBy(template);
        assertEquals("WHERE template GROUP BY [template]", test.build());
        
        test = createTest();
        test.where(template).and().appendExpression(template).nullable(null).groupBy(template);
        assertEquals("WHERE template GROUP BY [template]", test.build());
    }
    
    @Test
    public void testBuildMeltdownAtBegining() throws SQLException {
        FreeSelectSqlBuilder test = createTest();

        test = createTest();
        test.where(template).nullable(null).and().appendExpression(template).or().appendExpression(template).nullable(null).groupBy(template);
        assertEquals("WHERE template GROUP BY [template]", test.build());

    }
}
