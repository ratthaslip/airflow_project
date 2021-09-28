sudo docker run -d \
-e MYSQL_ROOT_PASSWORD=root \
-e TZ='Asia/Bangkok' \
-p 3306:3306 \
-v $(pwd)/data:/var/lib/mysql \
--name airflow-mysql \
--cap-add=sys_nice \
--restart=always \
--default-authentication-plugin=mysql_native_password \
mysql:8.0.20
