# Data Mining Homework 1 (Hadoop and Mapreduce Introduction)

**CAUTION**: replace "mahdi" with your own home folder name in following instructions

## Hadoop Setup (Ubuntu)

this part contains information on installing and running hadoop on ubuntu from ground up. do consider that
we will be installing hadoop 3.2.x on ubunto 18.04 so some of these instructions may not work on other versions.

### 1. Installing Required System Packages

after setting up your ubuntu system you should install following packages:
- **SSH Server and Client:** hadoop uses this package to access slave / master nodes remotely
- **JRE and JDK:** hadoop is written on java and the main programming language to work with it is java too, so you should install JRE to run hadoop (it is required to run java programs) and install jdk to let hadoop compile and run other programs (including mapreduce functions written in java). it is important to install the 8th version as some classes used in hadoop are depricated in java 9 and removed in java 11.
- **Python and Pip:** python is required for running mapreduce jobs written in python. pip is the main package manager for python, we will use it to install mrjob library. i will be using python3.

you can install these packages with following commands:

```shell
sudo apt update
sudo apt install -y openjdk-8-jre openjdk-8-jdk openssh-server python3-pip
```

python3 package is already installed on ubuntu.

### 2. Downloading and Installing Hadoop

now download hadoop from following [here](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz) and put it in your home directory.

extract it and move it to `/opt` folder with following commands:

```shell
sudo tar -xzf /home/mahdi/hadoop-3.2.1.tar.gz
sudo mv /home/mahdi/hadoop-3.2.1 /opt/hadoop
```

congratulation!!! you have installed hadoop on your sysmtem :) but it won't do anything unless you configure it.

### 3. Configuring Hadoop

go to `/opt/hadoop/etc/hadoop` replace contents of files in this folder like bellow:

**core-site.xml:**
```xml
<configuration>

	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://localhost:9000</value>
	</property>

	<property>
		<name>hadoop.tmp.dir</name>
		<value>/root/hdata</value>
	</property>

</configuration>
```

**hdfs-site.xml:**
```xml
<configuration>

	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>

</configuration>
```

**mapred-site.xml:**
```xml
<configuration>

	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
	
	<property>
		<name>yarn.app.mapreduce.am.env</name>
	       	<value>HADOOP_MAPRED_HOME=/root/hadoop</value>
	</property>
	
	<property>
		 <name>mapreduce.map.env</name>
		 <value>HADOOP_MAPRED_HOME=/root/hadoop</value>
	</property>

	<property>
		<name>mapreduce.reduce.env</name>
		<value>HADOOP_MAPRED_HOME=/root/hadoop</value>
	</property>

</configuration>
```

**yarn-site.xml:**
```xml
<configuration>
	
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>

	<property>
		<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
		<value>org.apache.hadoop.mapred.ShuffleHandler</value>
	</property>

</configuration>
```

you should also add JAVA_HOME to `hadoop-env.sh` file like so (just append this line to the end of it):

```
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
```

alternatively you can just copy contents of `resources/master/config`.

## Hadoop Setup (Docker):

first install docker on your system and change your working directory to this projects home. now you can build project's docker image by following command:

```shell
docker build -t mahs4d/hadoop .
```

you should download and put hadoop-3.2.1.tar.gz into resources directory before running build.

now you can easily start the container with following command (the ports are not exposed because we do not need them):

```shell
docker run -it -v /home/mahdi/hw1-output:/root/output mahs4d/hadoop
```

in the shell you run solutions using:

```shell
cd
./solution_runner 2_a
./solution_runner 2_b
./solution_runner 2_c
./solution_runner 2_d
```

the output will be available in your `/home/mahdi/hw1-output`
