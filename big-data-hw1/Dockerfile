FROM ubuntu:18.04

LABEL MAINTAINER="Mahdi Sadeghi"

USER root

# install java and ssh server
RUN apt-get update &&\
    apt-get install -y vim openjdk-8-jre openjdk-8-jdk openssh-server python3-pip

# generate ssh key and add it to authorized key
RUN ssh-keygen -t rsa -P "" -f "/root/.ssh/id_rsa" &&\
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

# hadoop environment variables
ENV HADOOP_HOME="/root/hadoop"
ENV PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV YARN_HOME=${HADOOP_HOME}

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

# exposing ports
EXPOSE 8088 9870 9864 19888 8042 8888

# install hadoop from tar file
COPY ./resources/hadoop-3.2.1.tar.gz /root/ 
RUN ["tar", "-xzf", "/root/hadoop-3.2.1.tar.gz", "--one-top-level=hadoop", "--strip-components", "1", "-C", "/root/"]
RUN ["rm", "/root/hadoop-3.2.1.tar.gz"]

# hadoop configuration files
COPY resources/master/config/* /root/hadoop/etc/hadoop/

# format hdfs
RUN ["/root/hadoop/bin/hdfs", "namenode", "-format"]

# python and packages
RUN ["pip3", "install", "mrjob"]

# input / output and app
COPY solution_runner.sh /root
RUN ["ln" , "-s", "/root/solution_runner.sh", "/bin/solution_runner"]
COPY src /root/src
COPY input /root/input
VOLUME ["/root/output"]

# start
COPY resources/master/bootstrap.sh /root/
ENTRYPOINT [ "/root/bootstrap.sh" ]
