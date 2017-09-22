package test.com.ctrip.platform.dal.dao.sqlbuilder;

import java.sql.SQLException;

import org.junit.Test;

import static org.junit.Assert.*;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
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
}
