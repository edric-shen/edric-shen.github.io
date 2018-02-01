---
layout: post
title:  "Sqoop使用中遇到的问题"
date:   2018-02-01 22:18:42 +0800
categories: Sqoop
tags: Sqoop
---
+ tinyint(1)问题，当数据库字段类型设置为tinyint(1)时，sqoop会把这个当成boolean类型，值只有0和1，解决办法是可以把长度增加或者使用其他类型。
+ sqoop 1.4.6-cdh5.9.0版本使用avro格式时，如果列名为value时，无法推送，需要在抽取和推送的时候把列名使用别名替换掉，在1.4.7版本中没有这个问题，应该是1.4.6版本读取avro文件时候有问题，具体原因有待分析。


