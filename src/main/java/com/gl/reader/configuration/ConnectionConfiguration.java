package com.gl.reader.configuration;

import org.springframework.orm.jpa.EntityManagerFactoryInfo;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Connection;

@Repository
public class ConnectionConfiguration {

    @PersistenceContext
    private EntityManager em;

//    private static ConnectionConfiguration connectionConfiguration;
//    public Connection connection;
//    public ConnectionConfiguration(EntityManager em) {
//        System.out.println("EM " + em);
//        try {
//            EntityManagerFactoryInfo info = (EntityManagerFactoryInfo) em.getEntityManagerFactory();
//            info.getDataSource().getConnection();
//        } catch (Exception e) {
//            System.out.println(e.getMessage(), e);
//        }
//    }
//    public static ConnectionConfiguration getConnection() {
//        if (connectionConfiguration == null)
//            connectionConfiguration = new ConnectionConfiguration(em);
//        return connectionConfiguration;
//    }

    public Connection getConnection() {
        EntityManagerFactoryInfo info = (EntityManagerFactoryInfo) em.getEntityManagerFactory();
        try {
            return info.getDataSource().getConnection();
        } catch (Exception e) {
            return null;
        }
    }

}