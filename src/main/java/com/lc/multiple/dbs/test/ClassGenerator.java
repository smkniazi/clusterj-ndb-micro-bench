package com.lc.multiple.dbs.test;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import java.io.Serializable;

/**
 * Generated classes extending MySQL NDB Dynamic Objects.
 */
public class ClassGenerator implements Serializable {

  private ClassPool pool;

  public ClassGenerator() {
    pool = ClassPool.getDefault();
  }

  public Class<?> generateClass(String tableName) throws Exception {
    CtClass originalClass = pool.get("com.lc.multiple.dbs.test.BaseTable");
    originalClass.defrost();
    originalClass.setName(tableName);

    String methodCode = "public String table() { return \"" + tableName + "\"; }";
    CtMethod tableMethod = CtMethod.make(methodCode, originalClass);
    originalClass.addMethod(tableMethod);

    return originalClass.toClass();
  }
}
