# Update repos
sudo apt update && sudo apt upgrade

# Install JRE
sudo apt install default-jre

# Make python point to python3 & install pip
sudo apt-get install python-is-python3
sudo apt install python3-pip

# Install Spark
wget https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2-scala2.13.tgz
tar -xvzf spark-3.2.0-bin-hadoop3.2-scala2.13.tgz
sudo mv spark-3.2.0-bin-hadoop3.2-scala2.13 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
source ~/.profile

# Install findspark
pip install findspark

# Install anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh
bash Anaconda3-2021.11-Linux-x86_64.sh