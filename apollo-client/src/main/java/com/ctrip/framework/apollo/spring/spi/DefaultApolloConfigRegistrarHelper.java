/*
 * Copyright 2021 Apollo Authors
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
 *
 */
package com.ctrip.framework.apollo.spring.spi;

import com.ctrip.framework.apollo.core.spi.Ordered;
import com.ctrip.framework.apollo.spring.annotation.ApolloAnnotationProcessor;
import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import com.ctrip.framework.apollo.spring.annotation.SpringValueProcessor;
import com.ctrip.framework.apollo.spring.config.PropertySourcesProcessor;
import com.ctrip.framework.apollo.spring.property.SpringValueDefinitionProcessor;
import com.ctrip.framework.apollo.spring.util.BeanRegistrationUtil;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

/**
 * ApolloConfigRegistrar中注册了几个BeanDefinition，具体如下：
 * 这些类要么实现了BeanFactoryPostProcessor接口，要么实现了BeanPostProcessor接口，
 * 前面有提到过BeanFactoryPostProcessor和BeanPostProcessor是Spring提供的扩展机制，
 * BeanFactoryPostProcessor一定是在BeanPostProcessor之前执行。
 *
 */
public class DefaultApolloConfigRegistrarHelper implements ApolloConfigRegistrarHelper {

  private Environment environment;

  /**
   * 注册了
   * @param importingClassMetadata
   * @param registry
   */
  @Override
  public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
    AnnotationAttributes attributes = AnnotationAttributes
        .fromMap(importingClassMetadata.getAnnotationAttributes(EnableApolloConfig.class.getName()));
    final String[] namespaces = attributes.getStringArray("value");
    final int order = attributes.getNumber("order");
    final String[] resolvedNamespaces = this.resolveNamespaces(namespaces);
    PropertySourcesProcessor.addNamespaces(Lists.newArrayList(resolvedNamespaces), order);

    Map<String, Object> propertySourcesPlaceholderPropertyValues = new HashMap<>();
    // to make sure the default PropertySourcesPlaceholderConfigurer's priority is higher than PropertyPlaceholderConfigurer
    propertySourcesPlaceholderPropertyValues.put("order", 0);

    // 1 PropertySourcesPlaceholderConfigurer---- BeanFactoryPostProcessor
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesPlaceholderConfigurer.class.getName(),
        PropertySourcesPlaceholderConfigurer.class, propertySourcesPlaceholderPropertyValues);
    // 2 PropertySourcesProcessor--BeanFactoryPostProcessor
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesProcessor.class.getName(),
        PropertySourcesProcessor.class);
    // 3 ApolloAnnotationProcessor--BeanPostProcessor
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, ApolloAnnotationProcessor.class.getName(),
        ApolloAnnotationProcessor.class);
    // 4 SpringValueProcessor -- BeanFactoryPostProcessor和BeanPostProcessor
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, SpringValueProcessor.class.getName(),
        SpringValueProcessor.class);
    // 5 SpringValueDefinitionProcessor
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, SpringValueDefinitionProcessor.class.getName(),
        SpringValueDefinitionProcessor.class);
  }

  private String[] resolveNamespaces(String[] namespaces) {
    String[] resolvedNamespaces = new String[namespaces.length];
    for (int i = 0; i < namespaces.length; i++) {
      // throw IllegalArgumentException if given text is null or if any placeholders are unresolvable
      resolvedNamespaces[i] = this.environment.resolveRequiredPlaceholders(namespaces[i]);
    }
    return resolvedNamespaces;
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }
}
