
### 提交说明
优化MultiBulkReply.ToBytes中计算所需内存的算法,提高性能;在utils中新增GetItoaLen方法,以替代原len(strconv.Itoa(i int))计算数字被转换为字符串以后字符串的长度;

### 基准测试 [utils_test.go](lib%2Futils%2Futils_test.go)
#### 测试
OS:Centos 7
VM:VM Workstation 17.5.0
VM配置:2C4G

#### 测试结果
Now:当前版本  
New Version:本次提交的版本

* 非LRANGE指令  
  测试方法:运行以下指令3次,取平均值
```text
redis-benchmark -q -d 4096 --csv -t SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP
```

| Redis 6.2.6 | Request/S   | Now   | Request/S   | New Version | Request/S   | New Vs Now | New Vs Redis6 |
|-------------|-------------|-------|-------------|-------------|-------------|------------|---------------|
| SET         | 160605.7133 | SET   | 68052.81667 | SET         | 66993.39333 | -2%        | -58%          |
| GET         | 152806.5767 | GET   | 62314.54667 | GET         | 62106.69667 | 0%         | -59%          |
| INCR        | 175523.5433 | INCR  | 96774.33667 | INCR        | 103187.75   | 7%         | -41%          |
| LPUSH       | 125236.7433 | LPUSH | 70687.95333 | LPUSH       | 71056.98    | 1%         | -43%          |
| RPUSH       | 138791      | RPUSH | 68633.12667 | RPUSH       | 67574.54    | -2%        | -51%          |
| LPOP        | 153191.02   | LPOP  | 60513.26333 | LPOP        | 61848.86333 | 2%         | -60%          |
| RPOP        | 134505.1    | RPOP  | 68805.09333 | RPOP        | 70723.33333 | 3%         | -47%          |
| SADD        | 170620.0933 | SADD  | 103671.8467 | SADD        | 105098.9467 | 1%         | -38%          |
| HSET        | 157717.3833 | HSET  | 70449.39667 | HSET        | 70000.03333 | -1%        | -56%          |
| SPOP        | 167282.3467 | SPOP  | 104680.9    | SPOP        | 113612.5367 | 9%         | -32%          |

总结:本次提交的性能优化,对非LRANGE的指令吞吐率有部分提升;

* LRANGE指令  
  测试方法:运行以下指令3次,取平均值
```text
redis-benchmark -q -d 4096 --csv -t lrange -n 10000
```

| Redis 6.2.6                        | Request/S   | Now                                | Request/S   | New Version                        | Request/S   | New Vs Now | New Vs Redis6 |
|------------------------------------|-------------|------------------------------------|-------------|------------------------------------|-------------|------------|---------------|
| LPUSH (needed to benchmark LRANGE) | 138415.7267 | LPUSH (needed to benchmark LRANGE) | 31227.84667 | LPUSH (needed to benchmark LRANGE) | 66234.6     | 112%       | -52%          |
| LRANGE_100 (first 100 elements)    | 2394.883333 | LRANGE_100 (first 100 elements)    | 1392.1      | LRANGE_100 (first 100 elements)    | 2100.576667 | 51%        | -12%          |
| LRANGE_300 (first 300 elements)    | 737.4066667 | LRANGE_300 (first 300 elements)    | 562.9166667 | LRANGE_300 (first 300 elements)    | 762.1166667 | 35%        | 3%            |
| LRANGE_500 (first 450 elements)    | 480.99      | LRANGE_500 (first 450 elements)    | 349         | LRANGE_500 (first 450 elements)    | 522.4533333 | 50%        | 9%            |
| LRANGE_600 (first 600 elements)    | 348.7066667 | LRANGE_600 (first 600 elements)    | 281.02      | LRANGE_600 (first 600 elements)    | 389.0833333 | 38%        | 12%           |

总结:本次提交的性能优化,对LRANGE的指令吞吐率提升明显,部分超过Redis6;  