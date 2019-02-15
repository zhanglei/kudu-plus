package me.bigdata.kudu.ext.factory;

import me.bigdata.kudu.ext.dao.ITestDAO;
import me.bigdata.kudu.ext.dao.ITestDemoDAO;
import me.bigdata.kudu.ext.dao.impl.TestDAOImpl;
import me.bigdata.kudu.ext.dao.impl.TestDemoDAOImpl;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 在这里将所有的查询kudu的方法都在这里创建，供上层调用
 */
public class DAOFactory {

    private static volatile DAOFactory instance = new DAOFactory();

    @SuppressWarnings("rawtypes")
    private static Map<String, Class> classMap = new HashMap<String, Class>();
    private static ConcurrentMap<String, Object> instanceMap = new ConcurrentHashMap<String, Object>();
    private static ConcurrentMap<String, Object> tableTypeInstanceMap = new ConcurrentHashMap<String, Object>();

    private DAOFactory() {

    }

    public static DAOFactory getInstance() {
        return instance;
    }

    static {
        classMap.put(ITestDAO.class.getName(), TestDAOImpl.class);
        classMap.put(ITestDemoDAO.class.getName(), TestDemoDAOImpl.class);
    }

    public static <T> T getDAO(Class<T> clz) {
        return getDAO(clz.getName(), clz);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getDAO(String name, Class<T> clz) {
        Object dao = instanceMap.get(clz);
        // to avoid lock the original name
        String lockName = ("daolock." + name).intern();
        if (dao == null) {
            synchronized (lockName) {
                if (dao == null) {
                    Class<T> daoImplClass = classMap.get(name);
                    try {
                        dao = daoImplClass.newInstance();
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                    instanceMap.putIfAbsent(clz.getName(), dao);
                }
            }
        }
        return (T) dao;
    }

    public static <T> T getDAOByTableType(Class<T> clz, String tableType) {
        return getDAOByTableType(clz.getName(), clz, tableType);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getDAOByTableType(String name, Class<T> clz, String tableType) {
        if (StringUtils.isBlank(tableType)) {
            return getDAO(clz);
        }
        String key = (name + "-" + tableType).intern();
        Object dao = tableTypeInstanceMap.get(key);
        if (dao == null) {
            synchronized (key) {
                if (dao == null) {
                    Class<T> daoImplClass = classMap.get(name);
                    try {
                        dao = daoImplClass.newInstance();
                        Method setTableType = dao.getClass().getMethod("setTableType", String.class);
                        if (setTableType != null) {
                            setTableType.invoke(dao, tableType);
                        }
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } catch (SecurityException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    tableTypeInstanceMap.put(key, dao);
                }
            }
        }
        return (T) dao;
    }
}
