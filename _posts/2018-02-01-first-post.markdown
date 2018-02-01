---
layout: post
title:  "Sqoop使用中遇到的问题"
date:   2018-02-01 22:18:42 +0800
categories: Sqoop
tags: Sqoop
---
+ tinyint(1)问题，当数据库字段类型设置为tinyint(1)时，sqoop会把这个当成boolean类型，值只有0和1，解决办法是可以把长度增加或者使用其他类型。


