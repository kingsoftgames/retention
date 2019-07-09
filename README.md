# 计算留存

通过lobby产生的创建用户日志和登录日志，来计算留存。

## 语言

python 3

## 日志格式

空格为分隔符

### 创建用户日志

```bash
1559111639 CreatePlayer {"avatar_id":100000,"player_id":"10","name":"吉娜达勒"}
1559111639 CreatePlayer {"avatar_id":100000,"player_id":"21","name":"吉娜达勒1"}
```

### 登录日志

```bash
1559111639 EnterGame {"player_id":"70"}
1559111639 EnterGame {"player_id":"71"}
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

### Document ID

Document ID是日期加留存天数映射而成，脚本可以多次执行，不会有冗余数据。第一次是创建，第二次以后就是更新

## 参数

参数是通过环境变量来配置的

参数名称                     |是否必须|默认值   | 类型 | 描述
:-:                         | :-:   | :-:     | :-:  | :-:
AWS_REGION                  | 是    | 无      |string| aws region| 
S3_BUCKET                   | 是    | 无      |string| s3 bucket name|
S3_KEY_PREFIX_CREATE_PLAYER | 是    | 无      |string| create player log path prefix| 
S3_KEY_PREFIX_PLAYER_LOGIN  | 是    | 无      |string| login log path prefix|
CREATE_PLAYER_EVENT         | 是    | 无      |string| create player event| 
PLAYER_LOGIN_EVENT          | 是    | 无      |string| login event|
RETENTION_DAYS              | 是    | 无      |string| retention days|
ES_URL                      | 是    | 无      |string| elasticsearch url|
ES_USER                     | 是    | 无      |string| elasticsearch user name|
ES_PWD                      | 是    | 无      |string| elasticsearch password|
ES_INDEX                    | 否    |retention|string| elasticsearch index name| 
AWS_ACCESS_KEY_ID           | 否    | 无      |string| aws access key id|
AWS_SECRET_ACCESS_KEY       | 否    | 无      |string| aws secret access key| 

### CREATE_FILE_NAME_PREFIX and LOGIN_FILE_NAME_PREFIX

```bash
# 如果日志路径是
/test/log/EnterGame/2019/06/16/create16.log
#对应的配置格式是
/test/log/EnterGame/<yyyy>/<MM>/<dd>/
```

#### 路径中的日期

- \<yyyy\>代表年
- \<MM\>代表月（总是双数06） 
- \<M\>代表月（真实数字6） 
- \<dd\>代表日（总是双数06） 
- \<d\>代表日（真实数字6） 

举例
```bash
#<MM>,<M>,<dd>,<d> 只是在月份和天是个位数是有区别
<yyyy>/<MM>/<dd> --> 2019/06/06
<yyyy>/<M>/<d>   --> 2019/6/6
```

### RETENTION_DAYS

天数组成的字符串，用逗号分隔，无序

### example

```bash
AWS_REGION=cn-north-1
S3_BUCKET=log
S3_KEY_PREFIX_CREATE_PLAYER=/test/log/<yyyy>/<MM>/<d>/CreatePlayer
S3_KEY_PREFIX_PLAYER_LOGIN=/test/log/EnterGame/<yyyy>/<MM>/<d>/
CREATE_PLAYER_EVENT=CreatePlayer
PLAYER_LOGIN_EVENT=EnterGame
RETENTION_DAYS=2,3,7,14
ES_USER=xxx
ES_PWD=xxx
ES_URL=https://ip:port
ES_INDEX=retention
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

## 查询分析

elasticsearch中的日志格式

```json
{
  "_index": "retention", // _index就是索引名，就是ES_INDEX定义的值，默认是retention
  "_type": "_doc",
  "_id": "2019-06-29_day3", // Document ID
  "_version": 3,
  "_score": null,
  "_source": {
    "@timestamp": 1561737600000, // 当天的日期
    "type": "day3",    // type就是留存的天数，RETENTION_DAYS设置的值
    "retention": 0.6   // 留存率
  },
  "fields": {
    "@timestamp": [
      "2019-06-28T16:00:00.000Z"
    ]
  },
  "sort": [
    1561737600000
  ]
}
```




