package com.ctrip.platform.dal.dao.client;

/**
 * Internal performance recorder for performance cost in each stage.
 * As each low level DB operation will be logged once at ConnectionAction level, this recorder will 
 * be set into LogEntry which is created as soon as ConnectionAction is created.
 * 
 * @author jhhe
 *
 */
public class CostRecorder {
    private static final String JSON_PATTERN = "{'Decode':'%s','Connect':'%s','Prepare':'%s','Excute':'%s','ClearUp':'%s'}";
    
    private long begin;
    private long beginConnect;
    private long endConnect;
    private long beginExecute;
    private long endExecute;
    private long end;
    
    public void begin(){
        begin = System.currentTimeMillis();
    }
    
    public void beginConnect(){
        beginConnect = System.currentTimeMillis();
    }
    
    public void endConnect(){
        endConnect = System.currentTimeMillis();
    }
    
    public void beginExecute(){
        beginExecute = System.currentTimeMillis();
    }
    
    public void endExectue(){
        endExecute = System.currentTimeMillis();
    }
    
    public String toJson(){
        // Final end
        end = System.currentTimeMillis();
        
        return String.format(JSON_PATTERN, begin == 0 ? 0 : beginConnect - begin,
                endConnect - beginConnect, beginExecute - endConnect,
                endExecute - beginExecute, end - endExecute);
    }
}
