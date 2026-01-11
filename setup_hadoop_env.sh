#!/bin/bash
# Script setup biến môi trường Hadoop

echo "Đang cấu hình biến môi trường Hadoop..."

# Set biến môi trường cho session hiện tại
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Kiểm tra JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    # Tìm Java
    JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
    export JAVA_HOME
    echo "Đã set JAVA_HOME: $JAVA_HOME"
fi

# Thêm vào ~/.bashrc nếu chưa có
if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    echo "" >> ~/.bashrc
    echo "# Hadoop Configuration" >> ~/.bashrc
    echo "export HADOOP_HOME=/usr/local/hadoop" >> ~/.bashrc
    echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
    echo "✅ Đã thêm vào ~/.bashrc"
else
    echo "⚠️  HADOOP_HOME đã có trong ~/.bashrc"
fi

echo ""
echo "✅ Cấu hình hoàn tất!"
echo ""
echo "Biến môi trường hiện tại:"
echo "  HADOOP_HOME=$HADOOP_HOME"
echo "  JAVA_HOME=$JAVA_HOME"
echo ""
echo "Để áp dụng cho session hiện tại, chạy:"
echo "  source ~/.bashrc"
echo ""
echo "Hoặc dùng lệnh hdfs với đường dẫn đầy đủ:"
echo "  /usr/local/hadoop/bin/hdfs dfs -ls /"

