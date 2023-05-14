package database

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/wildcard"
	"github.com/hdt3213/godis/redis/protocol"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

func init() {

}

type configCmd struct {
	name      string
	operation string
	executor  ExecFunc
}

var configCmdTable = make(map[string]*configCmd)

func ExecConfigCommand(args [][]byte) redis.Reply {
	return execSubCommand(args)
}

func execSubCommand(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return getAllGodisCommandReply()
	}
	subCommand := strings.ToUpper(string(args[1]))
	switch subCommand {
	case "GET":
		return getConfig(args[2:])
	case "SET":
		return setConfig(args[2:])
	case "RESETSTAT":
		// todo add resetstat
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	case "REWRITE":
		// todo add rewrite
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	default:
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	}
}
func getConfig(args [][]byte) redis.Reply {
	result := make([][]byte, 0)
	propertiesMap := getPropertiesMap()
	for _, arg := range args {
		param := string(arg)
		for key, value := range propertiesMap {
			pattern, err := wildcard.CompilePattern(param)
			if err != nil {
				return nil
			}
			isMatch := pattern.IsMatch(key)
			if isMatch {
				result = append(result, []byte(key), []byte(value))
			}
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

func getPropertiesMap() map[string]string {
	PropertiesMap := map[string]string{}
	t := reflect.TypeOf(config.Properties)
	v := reflect.ValueOf(config.Properties)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		var value string
		switch fieldVal.Type().Kind() {
		case reflect.String:
			value = fieldVal.String()
		case reflect.Int:
			value = strconv.Itoa(int(fieldVal.Int()))
		case reflect.Bool:
			if fieldVal.Bool() {
				value = "yes"
			} else {
				value = "no"
			}
		}
		PropertiesMap[key] = value
	}
	return PropertiesMap
}

func setConfig(args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'config|set' command")
	}
	properties := config.CopyProperties()
	updateMap := make(map[string]string)
	mu := sync.Mutex{}
	for i := 0; i < len(args); i += 2 {
		parameter := string(args[i])
		value := string(args[i+1])
		mu.Lock()
		if _, ok := updateMap[parameter]; ok {
			errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - duplicate parameter", parameter)
			return protocol.MakeErrReply(errStr)
		}
		updateMap[parameter] = value
		mu.Unlock()
	}
	for parameter, value := range updateMap {
		err := updateConfig(properties, parameter, value)
		if err != nil {
			return err
		}
	}

	config.Properties = properties
	return &protocol.OkReply{}
}

func updateConfig(properties *config.ServerProperties, parameter string, value string) redis.Reply {
	t := reflect.TypeOf(properties)
	v := reflect.ValueOf(properties)
	n := t.Elem().NumField()
	var isExist bool
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		if key == parameter {
			isExist = true
			isImmutable := config.IsImmutableConfig(parameter)
			if !isImmutable {
				return protocol.MakeErrReply(fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - can't set immutable config", parameter))
			}
			switch fieldVal.Type().Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - argument couldn't be parsed into an integer", parameter)
					return protocol.MakeErrReply(errStr)
				}
				fieldVal.SetInt(intValue)
			case reflect.Bool:
				if "yes" == value {
					fieldVal.SetBool(true)
				} else if "no" == value {
					fieldVal.SetBool(false)
				} else {
					errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - argument couldn't be parsed into a bool", parameter)
					return protocol.MakeErrReply(errStr)
				}
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
			break
		}
	}
	if !isExist {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", parameter))
	}
	return nil
}
