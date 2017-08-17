package com.ctrip.platform.dal.dao.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

import com.ctrip.platform.dal.dao.client.DalTransactionFactoryRegistrar;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(DalTransactionFactoryRegistrar.class)
public @interface EnableDalTransaction {}
