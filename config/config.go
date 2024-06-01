package config

import (
	"log"
	"os"
	"strconv"
)

var (
	USERCENTERPORT string // 用户交互模块服务端口

	K8SNAMSPACE string // K8S命名空间

	MYSQLHOST     string // MYSQL服务地址
	MYSQLPORT     string // MYSQL服务端口
	MYSQLUSER     string // MYSQL服务用户
	MYSQLPASSWORD string // MYSQL服务密码
	MYSQLDATABASE string // MYSQL服务数据库

	ACCELERATIONRATIO int
	MAXHISTORYNUMBER  = 180
)

func init() {
	K8SNAMSPACE = os.Getenv("NAMESPACE")
	if K8SNAMSPACE == "" {
		log.Fatalf("Failed to get namespace from env")
	}

	USERCENTERPORT = os.Getenv("USERCENTER_PORT")
	if USERCENTERPORT == "" {
		log.Fatalf("Failed to get usercenter port from env")
	}

	MYSQLHOST = os.Getenv("MYSQL_HOST")
	if MYSQLHOST == "" {
		log.Fatalf("Failed to get mysql host from env")
	}

	MYSQLPORT = os.Getenv("MYSQL_PORT")
	if MYSQLPORT == "" {
		log.Fatalf("Failed to get mysql port from env")
	}

	MYSQLUSER = os.Getenv("MYSQL_USER")
	if MYSQLUSER == "" {
		log.Fatalf("Failed to get mysql user from env")
	}

	MYSQLPASSWORD = os.Getenv("MYSQL_PASSWORD")
	if MYSQLPASSWORD == "" {
		log.Fatalf("Failed to get mysql password from env")
	}

	MYSQLDATABASE = os.Getenv("MYSQL_DATABASE")
	if MYSQLDATABASE == "" {
		log.Fatalf("Failed to get mysql database from env")
	}

	var err error
	ACCELERATIONRATIO, err = strconv.Atoi(os.Getenv("ACCELERATION_RATIO"))
	if err != nil {
		log.Fatal("Failed to get acceleration ratio from env")
	} else if ACCELERATIONRATIO == 0 {
		log.Fatal("Acceleration ratio cannot be zero")
	}
}
