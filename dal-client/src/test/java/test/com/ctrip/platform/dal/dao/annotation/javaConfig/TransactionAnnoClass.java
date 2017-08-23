package test.com.ctrip.platform.dal.dao.annotation.javaConfig;

import org.springframework.stereotype.Component;

import test.com.ctrip.platform.dal.dao.unitbase.OracleDatabaseInitializer;

import com.ctrip.platform.dal.dao.annotation.DalTransactional;


@Component
public class TransactionAnnoClass {
    public static final String DB_NAME = OracleDatabaseInitializer.DATABASE_NAME;
    
    @DalTransactional(logicDbName = DB_NAME)
    public String perform() {
        return null;
    }
}
