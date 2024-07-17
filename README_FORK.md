
## Target
This fork of Apache paimon repo is dedicated to resolve issues of creating and using a paimon metastore of Hive type on Amazon EMR(on EC2 or EMR Serverless) / Glue / Apache Spark on Athena.

## Issue Description
When we create a paimon metastore of Hive type with client class: `com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient`, it will report an error like 👇 :
```
java.lang.NoSuchMethodException: com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient.<init>
    (org.apache.hadoop.hive.conf.HiveConf, org.apache.hadoop.hive.metastore.HiveMetaHookLoader, java.lang.Boolean)
```

The `AWSCatalogMetastoreClient` really doesn't have a constructor of (HiveConf, HiveMetaHookLoader, Boolean) pattern. 

## Solutions

### Solution 1:

Patch the open source repo: [aws-glue-data-catalog-client-for-apache-hive-metastore](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore) by adding a constructor with three
parameters of (HiveConf, HiveMetaHookLoader, Boolean) to meet the requirement of creating a Hive catalog in Apache paimon, and you can refer this code [snippet](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/compare/branch-3.4.0...Moonlight-CL:aws-glue-data-catalog-client-for-apache-hive-metastore:branch-paimon-3.4.0).

This solution has some risks:
1. The repo [aws-glue-data-catalog-client-for-apache-hive-metastore](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore) was not actively updated and maintained.
2. The customized glue data catalog package can not update / sync with the development of Amazon EMR

### Solution 2:

Modify the `org.apache.paimon.hive.RetryingMetaStoreClientFactory` by detecting the metastore client class to qualify what kind of constructor it has. 

Detect what kind of construct(s) the metastore client class have.

```java

    /** Detect the client class whether it has the proper constructor. */
    private static IMetaStoreClient constructorDetectedHiveMetastoreProxySupplier(
            Method getProxyMethod, Configuration hiveConf, String clientClassName)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        try {
            Class<?> baseClass = Class.forName(clientClassName, false, JavaUtils.getClassLoader());

            Class<?>[] fullParams =
                    new Class[] {Configuration.class, HiveMetaHookLoader.class, Boolean.TYPE};
            Object[] fullParamValues =
                    new Object[] {hiveConf, (HiveMetaHookLoader) (tbl -> null), Boolean.TRUE};

            for (int i = fullParams.length; i >= 1; i--) {
                try {
                    baseClass.getConstructor(Arrays.copyOfRange(fullParams, 0, i));
                    return (IMetaStoreClient)
                            getProxyMethod.invoke(
                                    null,
                                    hiveConf,
                                    Arrays.copyOfRange(fullParams, 0, i),
                                    Arrays.copyOfRange(fullParamValues, 0, i),
                                    new ConcurrentHashMap<>(),
                                    clientClassName);
                } catch (NoSuchMethodException e) {
                }
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        throw new IllegalArgumentException(
                "Failed to create the desired metastore client with proper constructors (class name: "
                        + clientClassName
                        + ")");
    }
```

Use this as a `HiveMetastoreProxySupplier` instead in the `PROXY_SUPPLIERS` map.
```java
    // for hive 3.x,
    // and some metastore client classes providing constructors only for 2.x
    .put(
            new Class<?>[] {
                Configuration.class,
                Class[].class,
                Object[].class,
                ConcurrentHashMap.class,
                String.class
            },
            RetryingMetaStoreClientFactory
                    ::constructorDetectedHiveMetastoreProxySupplier)
```

After doing this, you can package and install the Apache paimon jars for you own, and using the customized jars on Amazon EMR to create paimon metastore of Hive type freely.

This is the recommended way!

