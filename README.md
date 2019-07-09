# 计算留存

通过lobby产生的创建用户日志和登录日志，来计算留存。

## 语言

python 3

## 日志格式

json, 必须要有event和player_id，player_id 在对应的event对象中。

### 创建用户日志

```json
{
	"CreatePlayer": {
		"player_id": "10",
	},
	"event": "CreatePlayer",
}

```

### 登录日志

```json
{
	"EnterGame": {
		"player_id": "78"
	},
	"event": "EnterGame",
}

```

## input

s3

### aws credentials 

- 1.Environment variables
- 2.Shared credential file (~/.aws/credentials)
- 3.IAM role.

## output

elasticsearch

### 访问方式

rest api

### index id

索引id是日期加留存天数映射而成，脚本可以多次执行，不会有冗余数据。第一次是创建，第二次以后就是更新

## 参数

参数是通过环境变量来配置的

参数名称                 |是否必须|默认值   | 类型 | 描述
:-:                     | :-:   | :-:     | :-:  | :-:
REGION                  | 是    | 无      |string| aws region| 
BUCKET_NAME             | 是    | 无      |string| s3 bucket name|
CREATTE_FILE_NAME_PREFIX| 是    | 无      |string| create player log path prefix| 
LOGIN_FILE_NAME_PREFIX  | 是    | 无      |string| login log path prefix|
CREATE_PLAYER_EVENT     | 是    | 无      |string| create player event| 
LOGIN_PLAYER_EVENT      | 是    | 无      |string| login event|
RETENTION_DAYS          | 是    | 无      |string| retention days|
ES_URL                  | 是    | 无      |string| elasticsearch url|
ES_USER                 | 是    | 无      |string| elasticsearch user name|
ES_PWD                  | 是    | 无      |string| elasticsearch password|
ES_INDEX_NAME           | 否    |retention|string| elasticsearch index name| 
AWS_ACCESS_KEY_ID       | 否    | 无      |string| aws access key id|
AWS_SECRET_ACCESS_KEY   | 否    | 无      |string| aws secret access key| 

### CREATTE_FILE_NAME_PREFIX and LOGIN_FILE_NAME_PREFIX

```bash
# 如果日志路径是
/test/log/EnterGame/2019/06/16/create16.log
# CREATTE_FILE_NAME_PREFIX是
/test/log/EnterGame/<yyyy>/<MM>/<d>/
```

#### 路径中的日期

- \<yyyy\>代表年
- \<MM\>代表月（总是双数06） 
- \<M\>代表月（真实数字6） 
- \<dd\>代表日（总是双数06） 
- \<d\>代表日（真实数字6） 

### RETENTION_DAYS

天数组成的字符串，用逗号分隔，无序

### example

```bash
REGION=cn-north-1
BUCKET_NAME=log
CREATTE_FILE_NAME_PREFIX=/test/log/<yyyy>/<MM>/<d>/CreatePlayer
LOGIN_FILE_NAME_PREFIX=/test/log/EnterGame/<yyyy>/<MM>/<d>/
CREATE_PLAYER_EVENT=CreatePlayer
LOGIN_PLAYER_EVENT=EnterGame
RETENTION_DAYS=2,3,7,14
ES_USER=xxx
ES_PWD=xxx
ES_URL=https://ip:port
```

## 启动方式

```bash 
python retention.py -h
usage: retention.py [-h] [-d [DAY]]

optional arguments:
  -h, --help            show this help message and exit
  -d [DAY], --day [DAY]
                        Date. The default date is yesterday. The format is
                        YYYY-MM-DD
```



