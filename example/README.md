
### Deploy depend components  using Docker
MySql 5.7
```bash
sudo docker pull mysql/5.7
sudo docker run --name mysql5.7 -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci

```