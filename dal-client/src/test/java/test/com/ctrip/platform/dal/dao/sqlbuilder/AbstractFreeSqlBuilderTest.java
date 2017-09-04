package test.com.ctrip.platform.dal.dao.sqlbuilder;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.TextClause;

public class AbstractFreeSqlBuilderTest {
    private static final String template = "template";
    private static final String elseTemplate = "elseTemplate";
    private static final String EMPTY = "";
    
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
}
