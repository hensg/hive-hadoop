FROM centos:7

RUN yum update -y \
  && yum install -y which wget tar java-1.8.0-openjdk-devel

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk

RUN wget -O hadoop.tar.gz https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz \
 && tar -zxvf hadoop.tar.gz -C /opt/ \
 && rm hadoop.tar.gz \
 && ln -s /opt/hadoop-2.6.0 /opt/hadoop

RUN wget -O hive.tar.gz https://archive.apache.org/dist/hive/hive-1.1.0/apache-hive-1.1.0-bin.tar.gz \
 && tar -zxvf hive.tar.gz -C /opt/ \
 && rm hive.tar.gz \
 && ln -s /opt/apache-hive-1.1.0-bin /opt/hive

ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib
ENV HADOOP_OPTS "$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib"
ENV HADOOP_CONF_DIR $HADOOP_HOME/conf

ENV YARN_HOME $HADOOP_HOME

ENV HIVE_HOME /opt/hive
ENV HIVE_CONF_DIR $HIVE_HOME/conf

# FIX java.lang.IncompatibleClassChangeError: Found class jline.Terminal, but interface was expected
ENV HADOOP_USER_CLASSPATH_FIRST true

ENV PATH $PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$HIVE_HOME/bin

ADD hadoop-conf/* $HADOOP_HOME/conf/
ADD hive-conf/* $HIVE_HOME/conf

RUN mkdir -p /mnt/hadoop/namenode
RUN mkdir -p /mnt/hadoop/datanode
RUN mkdir -p /mnt/hive/

RUN hdfs namenode -format

EXPOSE 8020 50010 50070 50075 50090 50105 50030 50060
