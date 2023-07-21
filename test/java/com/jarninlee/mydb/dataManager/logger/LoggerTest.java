package com.jarninlee.mydb.dataManager.logger;

import org.junit.Test;

import java.io.File;

public class LoggerTest {
    String p = "C:\\Users\\JarNinLee\\Desktop\\Java\\myDB\\src\\main\\resources";
    @Test
    public void testLogger() {
        Logger lg = Logger.create(p +  "\\logger_test.log");
        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("ccc".getBytes());
        lg.log("ddd".getBytes());
        lg.log("eee".getBytes());
        lg.close();

        lg = Logger.open(p +  "\\logger_test.log");
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.next();
        assert log == null;

        lg.close();

        assert new File(p +  "\\logger_test.log").delete();
    }
}