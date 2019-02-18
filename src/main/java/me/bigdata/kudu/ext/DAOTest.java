package me.bigdata.kudu.ext;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import me.bigdata.kudu.ext.dao.ITestDAO;
import me.bigdata.kudu.ext.dao.ITestDemoDAO;
import me.bigdata.kudu.ext.domain.TestDemo;
import me.bigdata.kudu.ext.factory.DAOFactory;
import me.bigdata.kudu.ext.helper.KuduColumn;
import me.bigdata.kudu.ext.helper.KuduMTable;
import org.apache.kudu.Type;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Random;

@Slf4j
public class DAOTest {

    private static ITestDAO testDAO = DAOFactory.getDAO(ITestDAO.class);
    private static ITestDemoDAO testDemoDAO = DAOFactory.getDAO(ITestDemoDAO.class);

    @Test
    public void insert() {
        List<TestDemo> list = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            Random random = new Random();
            TestDemo testDomain = new TestDemo();
            testDomain.setTableName("testhei");
            testDomain.setId(String.valueOf(random.nextInt(1000) + 1));
            testDomain.setAge(random.nextInt(88) + 1);
            testDomain.setUserName("roger.xie" + i);
            testDomain.setSex(random.nextInt(2));
            testDomain.setEmail("email000" + i + "@qq.com" );
            testDomain.setQq("100000" + i);
            //testDemoDAO.insert(testDomain);
            list.add(testDomain);
        }
        testDemoDAO.insert(list);
    }

    @Test
    public void rename() {
        TestDemo testDomain = new TestDemo();
        //testDemoDAO.renameTable(testDomain, "new_tel_test_demo");
        testDemoDAO.renameTable("presto", "a006", "new_tel_test_demo", "tel_test_demo");
    }

    @Test
    public void drop() {
        TestDemo testDomain = new TestDemo();
        testDomain.setTableName("testdemo");
        //testDemoDAO.dropTable(testDomain);
        testDemoDAO.dropTable("presto", "a006", "testxiao");
    }

    @Test
    public void alter() {
        KuduMTable entity01 = new KuduMTable();
        entity01.setTableName("presto::a006.tel_test_demo");
        List<KuduColumn> cl01 = Lists.newArrayList();
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("newsex").setNewColumnName("sex");
        c01.setAlterColumnEnum(KuduColumn.AlterColumnEnum.RENAME_COLUMN);
        KuduColumn c02 = new KuduColumn();
        //c02.setColumnName("myadd01").setAlterColumnEnum(KuduColumn.AlterColumnEnum.ADD_COLUMN).setNullAble(false).setColumnType(Type.STRING).setDefaultValue("haha");
        c02.setColumnName("myadd01").setAlterColumnEnum(KuduColumn.AlterColumnEnum.DROP_COLUMN);
        KuduColumn c03 = new KuduColumn();
        c03.setColumnName("myadd02").setAlterColumnEnum(KuduColumn.AlterColumnEnum.ADD_COLUMN).setColumnType(Type.STRING);
        //c02.setColumnName("myadd02").setAlterColumnEnum(KuduColumn.AlterColumnEnum.DROP_COLUMN);
        cl01.add(c01);
        cl01.add(c02);
        cl01.add(c03);
        entity01.setRows(cl01);
        List<KuduMTable> nn = Lists.newArrayList();
        nn.add(entity01);
        testDemoDAO.alterColumn(nn);
    }

    @Test
    public void update() {
        TestDemo testDomain = new TestDemo();
        testDomain.setTableName("new_tel_test_demo");
        testDomain.setId("100");
        //testDomain.setAge(34);
        testDomain.setUserName("xiexia099");
        //testDomain.setSex(1);
        testDemoDAO.update(testDomain);
    }

    @Test
    public void delete() {
        TestDemo testDomain = new TestDemo();
        testDomain.setId("21");
        testDemoDAO.delete(testDomain);
    }

    @Test
    public void find() {
        TestDemo td = new TestDemo();
        td.setId("100");
        List<TestDemo> demoList2 = testDemoDAO.find(td);
        log.info(JSON.toJSONString(demoList2));
    }

    @Test
    public void findAll() {
        List<TestDemo> demoList2 = testDemoDAO.find();
        log.info(JSON.toJSONString(demoList2));
    }
}
