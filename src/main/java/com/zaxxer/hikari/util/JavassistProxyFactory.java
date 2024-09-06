/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.util;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.zaxxer.hikari.pool.*;

import javassist.*;
import javassist.bytecode.ClassFile;

/**
 * This class generates the proxy objects for {@link Connection}, {@link Statement},
 * {@link PreparedStatement}, and {@link CallableStatement}.  Additionally it injects
 * method bodies into the {@link ProxyFactory} class methods that can instantiate
 * instances of the generated proxies.
 *
 * @author Brett Wooldridge
 */
public final class JavassistProxyFactory
{
   private static ClassPool classPool;
   private static String genDirectory = "";

   /**
    *     ClassPoolpool=ClassPool.getDefaultO是Javassist的起手式，代表获得类池，后续会
    * 通过这个类池执行makeClass创建类的操作，这个类可以添加属性、接口、方法、构造器
    * 等，最后通过writeFile的方式写人工作空间。这些工作基本上都是在generateProxyClass的
    * 方法里做的。从上述代码中可以看到，ProxyConnection、ProxyStatement、ProxyPrepared-
    * Statement、ProxyCallableStatement、ProxyResultSet五个代理类都做了处理，但是不同的地
    * 方是，前3个转换为了delegate.method($$)，后两个转换为了((cast)delegate).method($$),
    * 因为ProxyCallableStatement、ProxyResultSet是两个抽象的继承类。
    *
    * HikariCP使用了动态代理字节码技术Javassist进行了5个核心JDBC代理
    * 类的创建及重命名，这些代理类除了原封不动地封装了JDBC原始的特性，HikariCP还增
    * 加了一些与状态管理、连接管理、异常处理、资源清理等相关的定制化内容。
    *
    * 墨既然HikariCP已经编写了ProxyConnection，那么Javaassist再次生成HikariProxy-
    *     考
    *     Connection的目的是什么？
    *     我们可以发现，ProxyConnection已经做了很多的事情，HikariProxy只是为每个方法添
    * 加了一个trycatch块而已，那么再次使用字节码的用意何在呢？
    *     代理委托给真正的驱动程序类。某些代理（比如ResultSet的代理）仅仅拦截一些方法，
    * 但是如果没有代码生成，代理必须实现所有委托给包装实例的50多个方法。基于反射的代
    * 码生成，还意味着当新的JDK版本向现有接口引人新的JDBC方法的时候，我们不需要做
    * 任何额外的工作。从抽象的ProxyConnection类生成具体类，任何未被抽象类覆盖的方法，
    * 以及抛出SQLException的方法都会使用如下代码生成委托，以允许检查异常，并查看是否
    * 断开连接等错误。
    *     try{
    *     return delegate.method($$);
    *     }catch（SQLException e){
    *     throw checkException(e);
    *     其副作用是：如果JDBC的API做了更改，HikariCP就没有那么灵活了，需要跟着修
    * 改。不过至少到目前为止HikariCP还没有遇到过更新时JDBC大幅度向后不兼容的情况。
    */
   public static void main(String... args) throws Exception {
      classPool = new ClassPool();
      classPool.importPackage("java.sql");
      classPool.appendClassPath(new LoaderClassPath(JavassistProxyFactory.class.getClassLoader()));

      if (args.length > 0) {
         genDirectory = args[0];
      }

      // Cast is not needed for these
      String methodBody = "{ try { return delegate.method($$); } catch (SQLException e) { throw checkException(e); } }";
      generateProxyClass(Connection.class, ProxyConnection.class.getName(), methodBody);
      generateProxyClass(Statement.class, ProxyStatement.class.getName(), methodBody);
      generateProxyClass(ResultSet.class, ProxyResultSet.class.getName(), methodBody);
      generateProxyClass(DatabaseMetaData.class, ProxyDatabaseMetaData.class.getName(), methodBody);

      // For these we have to cast the delegate
      methodBody = "{ try { return ((cast) delegate).method($$); } catch (SQLException e) { throw checkException(e); } }";
      generateProxyClass(PreparedStatement.class, ProxyPreparedStatement.class.getName(), methodBody);
      generateProxyClass(CallableStatement.class, ProxyCallableStatement.class.getName(), methodBody);

      modifyProxyFactory();
   }

   /**
    modifyProxyFactory方法则比较简单，它主要是将ProxyConnection、ProxyStatement、
    * ProxyPreparedStatement、ProxyCallableStatement、ProxyResultSet 改名为ProxyConnection、
    * ProxyStatement、ProxyPreparedStatement、ProxyCallableStatement、ProxyResultSet。代码
    *
    * @throws Exception
    */
   private static void modifyProxyFactory() throws NotFoundException, CannotCompileException, IOException {
      System.out.println("Generating method bodies for com.zaxxer.hikari.proxy.ProxyFactory");

      String packageName = ProxyConnection.class.getPackage().getName();
      CtClass proxyCt = classPool.getCtClass("com.zaxxer.hikari.pool.ProxyFactory");
      for (CtMethod method : proxyCt.getMethods()) {
         switch (method.getName()) {
            case "getProxyConnection":
               method.setBody("{return new " + packageName + ".HikariProxyConnection($$);}");
               break;
            case "getProxyStatement":
               method.setBody("{return new " + packageName + ".HikariProxyStatement($$);}");
               break;
            case "getProxyPreparedStatement":
               method.setBody("{return new " + packageName + ".HikariProxyPreparedStatement($$);}");
               break;
            case "getProxyCallableStatement":
               method.setBody("{return new " + packageName + ".HikariProxyCallableStatement($$);}");
               break;
            case "getProxyResultSet":
               method.setBody("{return new " + packageName + ".HikariProxyResultSet($$);}");
               break;
            case "getProxyDatabaseMetaData":
               method.setBody("{return new " + packageName + ".HikariProxyDatabaseMetaData($$);}");
               break;
            default:
               // unhandled method
               break;
         }
      }

      proxyCt.writeFile(genDirectory + "target/classes");
   }

   /**
    *  Generate Javassist Proxy Classes
    */
   private static <T> void generateProxyClass(Class<T> primaryInterface, String superClassName, String methodBody) throws Exception
   {
      String newClassName = superClassName.replaceAll("(.+)\\.(\\w+)", "$1.Hikari$2");

      CtClass superCt = classPool.getCtClass(superClassName);
      CtClass targetCt = classPool.makeClass(newClassName, superCt);
      targetCt.setModifiers(Modifier.setPublic(Modifier.FINAL));

      System.out.println("Generating " + newClassName);

      // Make a set of method signatures we inherit implementation for, so we don't generate delegates for these
      Set<String> superSigs = new HashSet<>();
      for (CtMethod method : superCt.getMethods()) {
         if ((method.getModifiers() & Modifier.FINAL) == Modifier.FINAL) {
            superSigs.add(method.getName() + method.getSignature());
         }
      }

      Set<String> methods = new HashSet<>();
      for (Class<?> intf : getAllInterfaces(primaryInterface)) {
         CtClass intfCt = classPool.getCtClass(intf.getName());
         targetCt.addInterface(intfCt);
         for (CtMethod intfMethod : intfCt.getDeclaredMethods()) {
            final String signature = intfMethod.getName() + intfMethod.getSignature();

            // don't generate delegates for methods we override
            if (superSigs.contains(signature)) {
               continue;
            }

            // Ignore already added methods that come from other interfaces
            if (methods.contains(signature)) {
               continue;
            }

            // Track what methods we've added
            methods.add(signature);

            // Clone the method we want to inject into
            CtMethod method = CtNewMethod.copy(intfMethod, targetCt, null);

            String modifiedBody = methodBody;

            // If the super-Proxy has concrete methods (non-abstract), transform the call into a simple super.method() call
            CtMethod superMethod = superCt.getMethod(intfMethod.getName(), intfMethod.getSignature());
            if ((superMethod.getModifiers() & Modifier.ABSTRACT) != Modifier.ABSTRACT && !isDefaultMethod(intf, intfMethod)) {
               modifiedBody = modifiedBody.replace("((cast) ", "");
               modifiedBody = modifiedBody.replace("delegate", "super");
               modifiedBody = modifiedBody.replace("super)", "super");
            }

            modifiedBody = modifiedBody.replace("cast", primaryInterface.getName());

            // Generate a method that simply invokes the same method on the delegate
            if (isThrowsSqlException(intfMethod)) {
               modifiedBody = modifiedBody.replace("method", method.getName());
            }
            else {
               modifiedBody = "{ return ((cast) delegate).method($$); }".replace("method", method.getName()).replace("cast", primaryInterface.getName());
            }

            if (method.getReturnType() == CtClass.voidType) {
               modifiedBody = modifiedBody.replace("return", "");
            }

            method.setBody(modifiedBody);
            targetCt.addMethod(method);
         }
      }

      targetCt.getClassFile().setMajorVersion(ClassFile.JAVA_8);
      targetCt.writeFile(genDirectory + "target/classes");
   }

   private static boolean isThrowsSqlException(CtMethod method)
   {
      try {
         for (CtClass clazz : method.getExceptionTypes()) {
            if (clazz.getSimpleName().equals("SQLException")) {
               return true;
            }
         }
      }
      catch (NotFoundException e) {
         // fall thru
      }

      return false;
   }

   private static boolean isDefaultMethod(Class<?> intf, CtMethod intfMethod) throws Exception
   {
      List<Class<?>> paramTypes = new ArrayList<>();

      for (CtClass pt : intfMethod.getParameterTypes()) {
         paramTypes.add(toJavaClass(pt));
      }

      return intf.getDeclaredMethod(intfMethod.getName(), paramTypes.toArray(new Class[0])).toString().contains("default ");
   }

   private static Set<Class<?>> getAllInterfaces(Class<?> clazz)
   {
      Set<Class<?>> interfaces = new LinkedHashSet<>();
      for (Class<?> intf : clazz.getInterfaces()) {
         if (intf.getInterfaces().length > 0) {
            interfaces.addAll(getAllInterfaces(intf));
         }
         interfaces.add(intf);
      }
      if (clazz.getSuperclass() != null) {
         interfaces.addAll(getAllInterfaces(clazz.getSuperclass()));
      }

      if (clazz.isInterface()) {
         interfaces.add(clazz);
      }

      return interfaces;
   }

   private static Class<?> toJavaClass(CtClass cls) throws Exception
   {
      if (cls.getName().endsWith("[]")) {
         return Array.newInstance(toJavaClass(cls.getName().replace("[]", "")), 0).getClass();
      }
      else {
         return toJavaClass(cls.getName());
      }
   }

   private static Class<?> toJavaClass(String cn) throws Exception
   {
      switch (cn) {
      case "int":
         return int.class;
      case "long":
         return long.class;
      case "short":
         return short.class;
      case "byte":
         return byte.class;
      case "float":
         return float.class;
      case "double":
         return double.class;
      case "boolean":
         return boolean.class;
      case "char":
         return char.class;
      case "void":
         return void.class;
      default:
         return Class.forName(cn);
      }
   }
}
