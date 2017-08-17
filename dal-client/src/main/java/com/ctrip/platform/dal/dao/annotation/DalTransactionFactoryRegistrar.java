package com.ctrip.platform.dal.dao.annotation;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

public class DalTransactionFactoryRegistrar implements ImportBeanDefinitionRegistrar {
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
//      AnnotationAttributes attributes = AnnotationAttributes.fromMap(importingClassMetadata
//          .getAnnotationAttributes(EnableApolloConfig.class.getName()));
//      String[] namespaces = attributes.getStringArray("value");
//      int order = attributes.getNumber("order");
//      PropertySourcesProcessor.addNamespaces(Lists.newArrayList(namespaces), order);
//
//      BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesPlaceholderConfigurer.class.getName(),
//          PropertySourcesPlaceholderConfigurer.class);
//
//      BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesProcessor.class.getName(),
//          PropertySourcesProcessor.class);
//
//      BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, ApolloAnnotationProcessor.class.getName(),
//          ApolloAnnotationProcessor.class);
//      // Register validator
    }
  }
